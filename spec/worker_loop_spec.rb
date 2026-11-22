# frozen_string_literal: true

require "async"
require "digest/sha1"
require "socket"
require "tmpdir"

# `count` chunks of 64 KiB, one at a time; 4 MiB by default.
StreamedChunks = Data.define(:count) do
  def self.bytes(count = 64) = count * 65_536

  def each
    chunk = "x" * 65_536
    count.times { yield chunk }
  end
end

# `count` server-sent events of 4 KiB each; with `failing`, the stream
# fails three quarters in.
ServerSentEvents = Data.define(:count, :failing) do
  def each
    event = "data: #{"x" * 4088}\n\n"
    count.times do |index|
      raise "the event stream failed" if failing && index == count * 3 / 4
      yield event
    end
  end
end

# What a worker_loop worker serves: /sleep waits 0.1 s (a fiber wait
# under the worker's scheduler), /nap 0.075 s (halfway between two lane
# ticks); /echo answers with the request body, /peek with its first 6
# bytes; /stream sends 4 MiB in
# 64 KiB chunks from an Enumerable body, or with ?callable from a Rack 3
# streaming one; /events sends ?count= server-sent events (1024, 4 MiB,
# by default), failing three quarters in with &fail; anything else is
# "hello".
LOOP_APP = Ractor.shareable_proc do |env|
  case env["PATH_INFO"]
  when "/sleep"
    sleep 0.1
    [200, {}, ["slept"]]
  when "/nap"
    sleep 0.075
    [200, {}, ["napped"]]
  when "/echo"
    [200, {}, [env["rack.input"].read]]
  when "/peek"
    [200, {}, [env["rack.input"].read(6)]]
  when "/stream"
    chunks = StreamedChunks.new(64)
    body = (env["QUERY_STRING"] == "callable") ? ->(stream) { chunks.each { |chunk| stream.write(chunk) } } : chunks
    [200, {"content-length" => StreamedChunks.bytes.to_s}, body]
  when "/events"
    query = env["QUERY_STRING"].split("&").to_h { |pair| pair.partition("=").values_at(0, 2) }
    [200, {"content-type" => "text/event-stream"}, ServerSentEvents.new(Integer(query.fetch("count", 1024)), query.key?("fail"))]
  else
    [200, {}, ["hello"]]
  end
end

# An after_request_complete hook: /linger is answered at once, then
# keeps its job 0.5 s more.
LINGERING = Ractor.shareable_proc { |env, _status| sleep 0.5 if env["PATH_INFO"] == "/linger" }

# LOOP_APP, except that it accepts a WebSocket handshake with `handler`.
LOOP_WITH_SOCKETS = Ractor.shareable_proc do |env, handler|
  next LOOP_APP.call(env) unless env["rack.upgrade?"] == :websocket

  env["rack.upgrade"] = handler
  [101, {}, []]
end

# Async's reactor, tallying for each fiber on its thread how often the
# fiber waited on an IO and how often it slept (on a timer), in thread
# variables, so each worker Ractor keeps its own.
class TallyingReactor < Async::Reactor
  # [waits, sleeps] of `fiber`, a fiber of this thread.
  def self.of(fiber) = tallies.fetch(fiber, [0, 0]).dup

  def self.tallies
    Thread.current.thread_variable_get(:spec_tallies) ||
      Thread.current.thread_variable_set(:spec_tallies, {}.compare_by_identity)
  end

  def io_wait(...)
    tally(0)
    super
  end

  def kernel_sleep(...)
    tally(1)
    super
  end

  private

  def tally(index) = (TallyingReactor.tallies[Fiber.current] ||= [0, 0])[index] += 1
end

# Callbacks running at once on this worker thread, counted in one of its
# thread variables (its fibers share them): yields the count, this one
# included.
module RunningCallbacks
  def self.count
    thread = Thread.current
    running = thread.thread_variable_get(:spec_running).to_i + 1
    thread.thread_variable_set(:spec_running, running)
    yield running
  ensure
    thread.thread_variable_set(:spec_running, thread.thread_variable_get(:spec_running) - 1)
  end
end

# Reports [socket id, number, callbacks running] to `port` for each
# numbered message, after a pause that suspends its fiber, so a message
# that overtook one of its socket's would be reported first; one that
# starts while another of its socket's runs reports [id, :overlap].
OrderedSocket = Data.define(:port) do
  def on_message(ws, data)
    thread = Thread.current
    busy = thread.thread_variable_get(:spec_busy_sockets) || thread.thread_variable_set(:spec_busy_sockets, {})
    port << [ws.id, :overlap] if busy[ws.id]
    busy[ws.id] = true
    RunningCallbacks.count do |running|
      sleep(rand * 0.002)
      port << [ws.id, Integer(data), running]
    end
  ensure
    busy[ws.id] = false
  end
end

# Reports [:holding, callbacks running] to `port`, holds its fiber until
# `gate` opens, then reports :held.
HoldingSocket = Data.define(:port, :gate) do
  def on_message(_ws, _data)
    RunningCallbacks.count do |running|
      port << [:holding, running]
      sleep 0.005 until File.exist?(gate.path)
    end
    port << :held
  end
end

# Reports :opened to `port` once its on_open waited 0.3 s (a fiber
# wait), and :closed from its on_close.
SlowOpeningSocket = Data.define(:port) do
  def on_open(_ws)
    sleep 0.3
    port << :opened
  end

  def on_close(_ws) = port << :closed
end

# Reports :holding to `port`, then holds its fiber 1.2 s, on "hold";
# any other message is done at once.
StallingSocket = Data.define(:port) do
  def on_message(_ws, data)
    return unless data == "hold"

    port << :holding
    sleep 1.2
  end
end

# Reports :closed to `port` once its on_close has run for `pause` s.
SlowClosingSocket = Data.define(:port, :pause) do
  def on_close(_ws)
    sleep pause
    port << :closed
  end
end

RSpec.describe "worker_loop" do
  include_context "native WebSocket server"

  # Serves `app` in `mode` from one worker thread that runs its loop
  # under Async (or `worker_loop`). Yields the port and the server.
  def start_loop_server(app = LOOP_APP, mode:, concurrency: 8, **opts)
    opts = {worker_loop: async_loop(concurrency), workers: 1, threads: 1, lanes: true}.merge(opts)
    with_server(app, mode: mode, **opts) { |_, port, server| yield port, server }
  end

  # A worker_loop that registers the shareable `hook` as the wake hook,
  # then reports [:wakes, port] (see #wakes): each message sent to that
  # port wakes the worker from a thread of its own, as a receiver thread
  # with news for the worker would.
  def waking_loop(hook, concurrency: 8)
    reports = self.reports.port
    Ractor.shareable_proc do |worker|
      waker = Kino::Worker.on_wake(&hook)
      wakes = Ractor::Port.new
      waking = Thread.new { waker.wake while wakes.receive }
      reports << [:wakes, wakes]
      Sync { |task| worker.serve(concurrency: concurrency) { |job| task.async { job.call } } }
    ensure
      waking&.kill
    end
  end

  # The port a waking_loop reported: each message sent to it wakes the
  # worker.
  def wakes
    kind, port = reports.next(awaiting: "the wake port")
    raise "expected the wake port first, got #{kind.inspect}" unless kind == :wakes

    port
  end

  # What the handlers reported and the spec has not read yet.
  def reports_so_far = [].tap { |unread| unread << reports.next until reports.empty? }

  # A worker_loop whose scheduler stops the second job it starts `after`
  # seconds in, while that job waits: a socket's, as the first is the
  # handshake's.
  def stopping_second_job(after)
    Ractor.shareable_proc do |worker|
      Sync do |task|
        jobs = 0
        worker.serve(concurrency: 4) do |job|
          started = task.async { job.call }
          next unless (jobs += 1) == 2

          task.async do
            sleep after
            started.stop
          end
        end
      end
    end
  end

  # A worker_loop with a background task that runs on `linger` seconds
  # after serve returned, which Sync, and so the loop, waits for.
  def lingering_loop(linger)
    Ractor.shareable_proc do |worker|
      Sync do |task|
        served = false
        task.async do
          sleep 0.02 until served
          sleep linger
        end
        worker.serve(concurrency: 8) { |job| task.async { job.call } }
        served = true
      end
    end
  end

  # What the log says, under the worker's label, about a loop still
  # running a second after its serve returned.
  def lingering_warning = "worker-0: the worker_loop is still running 1.0 s after its serve returned; most likely " \
    "a task that is not transient keeps its scheduler, and so the worker, from finishing"

  # LOOP_APP, except that a WebSocket handshake switches its connection
  # as `kind` says and echoes every frame on it until the client closes.
  # With `ended` (a port), it reports :ended there once the connection
  # closed (a 101's callable 0.05 s later).
  def echoing_switch(kind, ended = nil)
    Ractor.shareable_proc do |env|
      next LOOP_APP.call(env) unless env["rack.hijack?"]
      if kind == :full_hijack
        HijackedEcho.serve(env["rack.hijack"].call, env["HTTP_SEC_WEBSOCKET_KEY"])
        ended << :ended if ended
        next [-1, {}, []]
      end

      accept = [Digest::SHA1.digest(env["HTTP_SEC_WEBSOCKET_KEY"] + WsClient::GUID)].pack("m0")
      headers = {"upgrade" => "websocket", "connection" => "Upgrade", "sec-websocket-accept" => accept}
      echo = lambda do |io|
        while (frame = HijackedEcho.read_frame(io))
          io.write(WsClient.frame(*frame, mask: false))
        end
      ensure
        io.close
        if ended
          sleep 0.05
          ended << :ended
        end
      end
      (kind == :partial_hijack) ? [101, headers.merge("rack.hijack" => echo), []] : [101, headers, echo]
    end
  end

  # LOOP_WITH_SOCKETS for `handler`: shareable, for either mode, when the
  # handler is.
  def loop_app_with_sockets(handler)
    return ->(env) { LOOP_WITH_SOCKETS.call(env, handler) } unless Ractor.shareable?(handler)

    Ractor.shareable_proc { |env| LOOP_WITH_SOCKETS.call(env, handler) }
  end

  # /spin holds its worker's thread on the CPU, never yielding to the
  # scheduler, until the file at ?release= exists (10 s at most); with
  # ?after_wait=1, after a fiber wait first. /doze waits 1.2 s, a fiber
  # wait. Anything else is LOOP_WITH_SOCKETS's, with `handler`.
  def stalling_app(handler = nil)
    Ractor.shareable_proc do |env|
      case env["PATH_INFO"]
      when "/spin"
        query = env["QUERY_STRING"].split("&").to_h { |pair| pair.split("=", 2) }
        sleep 0.01 if query.key?("after_wait")
        deadline = Kino.monotonic + 10
        nil until File.exist?(query["release"]) || Kino.monotonic > deadline
        LOOP_APP.call(env)
      when "/doze"
        sleep 1.2
        [200, {}, ["dozed"]]
      else
        LOOP_WITH_SOCKETS.call(env, handler)
      end
    end
  end

  # busy_ms of the only worker, sampled until `thread` ends.
  def busy_while(server, thread)
    [].tap do |busy|
      while thread.alive?
        busy << server.stats[:worker_status].first[:busy_ms]
        sleep 0.02
      end
    end
  end

  # Serves `app` and its WebSockets from one worker thread in `mode`,
  # running `worker_loop` (Async, 8 jobs at a time, by default).
  def with_loop_sockets(app, mode, worker_loop: async_loop(8), **opts, &block)
    with_ws_server(app, mode: mode, workers: 1, worker_loop: worker_loop, **opts, &block)
  end

  # An on_worker_exit hook that keeps each exit's error for
  # #expect_crash.
  def on_worker_exit
    exits = worker_exits
    ->(_worker_id, error) { exits << error }
  end

  # Expects the next worker exit to be a crash with a Kino::Error saying
  # `message`.
  def expect_crash(message)
    error = worker_exits.pop(timeout: WsClient::DEADLINE)
    expect(error).to be_a(Kino::Error)
    expect(error.message).to eq(message)
  end

  def worker_exits = @worker_exits ||= Thread::Queue.new

  # Half a lane tick (25 ms) past the best of three round trips of a fast
  # request on `port` right now. A request that waits for the next tick
  # waits anywhere from nothing to a whole tick (50 ms), depending on the
  # phase of the ticker, so one sample over this bound shows a wait only
  # about half the time; compare several, and the wait shows in the
  # slowest.
  def within_half_a_tick(port) = Array.new(3) { timed { get(port, "/hello") } }.min + 0.025

  # Eight requests that each wait 0.1 s, at once. One after another they
  # would take eight times as long as one alone; half of that bounds them,
  # so a machine that oversleeps stretches the bound with the requests.
  def expect_eight_sleeps_to_overlap(port)
    alone = Array.new(2) { timed { get(port, "/sleep") } }.min
    took = timed { expect(concurrently(8) { get(port, "/sleep") }).to all(eq("slept")) }
    expect(took).to be < [alone * 4, 0.6].max,
      "eight requests that sleep took #{took.round(2)} s together, one alone #{alone.round(2)} s"
  end

  # Runs the block on `count` threads at once; their results.
  def concurrently(count, &block) = Array.new(count) { Thread.new(&block) }.map(&:value)

  # Requests the workers have taken and not yet answered.
  def open_requests(server) = server.stats[:worker_status].sum { |worker| worker[:in_flight] }

  # Seconds of CPU this process (in these specs, mostly the server's
  # threads) used while the spec slept `seconds`.
  def cpu_during(seconds)
    before = Process.clock_gettime(Process::CLOCK_PROCESS_CPUTIME_ID)
    sleep seconds
    Process.clock_gettime(Process::CLOCK_PROCESS_CPUTIME_ID) - before
  end

  # The body of an HTTP/1.1 response read to the end.
  def response_body_of(response) = response.split("\r\n\r\n", 2).last

  # One request whose fiber has to wait on its client: a slow reader of
  # a 1 MiB event stream (:slow), a reader that goes away mid-stream
  # (:gone; its stream is 4 MiB, more than the server's socket takes
  # before it goes, where Linux starts a loopback send buffer at 2.5 MiB),
  # a reader of a stream that fails (:failing), or an upload that arrives
  # in two halves, read whole (:upload) or answered before its end (:peek).
  def waiting_request(port, kind)
    socket = narrow_connection(port)
    if %i[upload peek].include?(kind)
      socket.write("POST /#{(kind == :peek) ? "peek" : "echo"} HTTP/1.1\r\nHost: x\r\nContent-Length: 8\r\nConnection: close\r\n\r\nhalf")
      sleep 0.02
      socket.write("half")
    else
      count = (kind == :gone) ? 1024 : 256
      socket.write("GET /events?count=#{count}#{"&fail" if kind == :failing} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
      socket.readpartial(4096)
      sleep 0.02
    end
    socket.read unless kind == :gone
  rescue Errno::ECONNRESET
    # A failed stream cuts its connection.
  ensure
    socket&.close
  end

  it "needs a callable, and a shareable one in ractor mode" do
    callable = ->(_worker) {}

    expect { Kino::Server.new(LOOP_APP, mode: :threaded, lanes: true, worker_loop: 42) }
      .to raise_error(ArgumentError, /worker_loop must respond to #call/)
    expect { Kino::Server.new(LOOP_APP, mode: :ractor, lanes: true, worker_loop: callable) }
      .to raise_error(Kino::Error, /shareable worker_loop hook \(build it with Ractor\.shareable_proc/)
    expect(capture_native_stderr { Kino::Server.new(LOOP_APP, mode: :threaded, lanes: true, batch: 4, worker_loop: callable) })
      .to include("batch has no effect with worker_loop")
  end

  each_mode do |mode|
    it "overlaps requests that wait" do
      start_loop_server(mode: mode, concurrency: 8) do |port|
        expect_eight_sleeps_to_overlap(port)
      end
    end

    it "serves with the sample config's worker_loop example" do
      example = Kino::Configuration.sample[/^# require "async"\n# worker_loop .*?^# \}$/m]
      expect(example).not_to be_nil, "the sample config shows no worker_loop example under `# require \"async\"`"

      start_loop_server(mode: mode, worker_loop: worker_loop_from(example.gsub(/^# ?/, ""))) do |port|
        expect_eight_sleeps_to_overlap(port)
      end
    end

    it "serves with the job fibers in doc/worker-loop.md, a socket's events beside requests that hold all of them" do
      FileGate.closed do |gate|
        released = gate.path
        app = Ractor.shareable_proc do |env|
          case env["PATH_INFO"]
          when "/fiber" then [200, {}, [Fiber.current.object_id.to_s]]
          when "/stray"
            # A late wake-up: the scheduler resumes this fiber at its
            # next pass, by when it waits for its next job.
            Fiber.scheduler.push(Fiber.current)
            [200, {}, ["pushed"]]
          when "/gated"
            sleep 0.005 until File.exist?(released)
            [200, {}, ["opened"]]
          else
            LOOP_WITH_SOCKETS.call(env, EchoSocket)
          end
        end

        with_loop_sockets(app, mode, worker_loop: documented_job_fibers) do |port, server|
          expect(get(port, "/stray")).to eq("pushed")
          fibers = []
          took = timed { 33.times { fibers << get(port, "/fiber") } }
          expect(took).to be < 2, "33 quick requests took #{took.round(1)} s: one was lost, handed to a runner that had ended"
          expect(fibers.uniq.size).to be <= 32, "33 requests one after another ran on fibers of their own"
          expect_eight_sleeps_to_overlap(port)

          client = connect(port)
          gated = Array.new(32) { Thread.new { get(port, "/gated") } }
          wait_for("32 requests holding every job fiber") { open_requests(server) == 32 }
          client.send_text("beside them")
          expect(client.read).to eq([WsClient::TEXT, "beside them"])
          gate.open
          expect(gated.map(&:value)).to all(eq("opened"))

          client.close
          expect(timed { server.shutdown }).to be < 1
        end
      end
    end

    # Lane dispatch to worker_loop workers, which still serves TLS and
    # what inline connections hand back: these servers
    # serve nothing inline (see inline_io_spec.rb for placement), but
    # where an example says so.

    # The lane holds four, more than the two slots.
    it "keeps to its concurrency" do
      start_loop_server(mode: mode, concurrency: 2, _inline: false) do |port|
        expect(timed { concurrently(4) { get(port, "/sleep") } }).to be >= 0.2
      end
    end

    it "takes the next request as soon as a job frees its slot" do
      reports = self.reports.port
      app = Ractor.shareable_proc do |env|
        reports << [env["PATH_INFO"], Kino.monotonic]
        LOOP_APP.call(env).tap do
          reports << [:napped, Kino.monotonic] if env["PATH_INFO"] == "/nap"
        end
      end

      start_loop_server(app, mode: mode, concurrency: 1, _inline: false) do |port|
        bound = within_half_a_tick(port)
        3.times { self.reports.next(awaiting: "the fast requests") }
        gaps = Array.new(3) do
          napper = Thread.new { get(port, "/nap") }
          expect(self.reports.next(awaiting: "the nap").first).to eq("/nap")
          expect(get(port, "/hello")).to eq("hello")

          (napped, napped_at), (greeted, greeted_at) = Array.new(2) { self.reports.next(awaiting: "the nap's end and the greeting") }
          expect([napped, greeted]).to eq([:napped, "/hello"]), "the second request ran while the first held the only slot"
          greeted_at - napped_at
        ensure
          napper&.join
        end
        # The middle of three: a tick's wait delays them all, a slow machine one.
        expect(gaps.sort[1]).to be < bound, "after a nap, the next request started #{gaps.map { |t| (t * 1000).round(1) }} ms later"
      end
    end

    it "lets an idle sibling take what waits behind a full worker at its next tick" do
      # Its worker stays full while dispatch counts only open requests.
      start_loop_server(mode: mode, concurrency: 2, threads: 2, after_request_complete: LINGERING, _inline: false) do |port|
        2.times { expect(get(port, "/linger")).to eq("hello") }

        # At the sibling's tick (every 50 ms), long before a job of the
        # full worker ends (0.5 s).
        took = timed { expect(get(port, "/hello")).to eq("hello") }
        expect(took).to be < 0.25, "the request waited #{(took * 1000).round} ms behind the full worker"
      end
    end

    it "keeps a worker whose job still runs after its answer out of dispatch while a sibling idles" do
      reports = self.reports.port
      serving = Ractor.shareable_proc do |worker|
        Sync do |task|
          reports << :serving
          worker.serve(concurrency: 1) { |job| task.async { job.call } }
        end
      end
      start_loop_server(mode: mode, workers: 2, worker_loop: serving, after_request_complete: LINGERING, _inline: false) do |port|
        2.times { expect(self.reports.next(awaiting: "a worker serving")).to eq(:serving) }
        bound = within_half_a_tick(port)
        expect(get(port, "/linger")).to eq("hello")

        # Sent to the full worker, a request would wait there until the
        # sibling's tick steals it. The pause lets the sibling get back to
        # its doorbell first, so most of three in a row would wait about a
        # tick, whenever the loops started, so the middle one would.
        took = Array.new(3) do
          sleep 0.002
          timed { expect(get(port, "/")).to eq("hello") }
        end
        expect(took.sort[1]).to be < bound, "beside a full worker, three requests took #{took.map { |t| (t * 1000).round(1) }} ms"
      end
    end

    # Through hyper, whose writes wait as fibers; inline_io_spec has the
    # inline ones.
    it "streams to a slow reader without blocking the worker (Rack 3 streaming body)" do
      start_loop_server(mode: mode, _inline: false) do |port|
        reader = narrow_connection(port)
        reader.write("GET /stream?callable HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        received = reader.readpartial(65_536)
        sleep 0.05 # the stream fills what the reader leaves unread

        latencies = Array.new(3) { timed { expect(get(port, "/hello")).to eq("hello") }.tap { received << reader.readpartial(65_536) } }
        # A worker blocked on the stalled stream would answer none of them.
        expect(latencies.max).to be < 1, "beside a stalled stream, requests took #{latencies.map { |t| t.round(2) }} s"

        received << reader.read
        expect(received.split("\r\n\r\n", 2).last.bytesize).to eq(StreamedChunks.bytes)
      ensure
        reader&.close
      end
    end

    # Through hyper, where the stream's write waits as a fiber.
    it "ends a stream whose client went away" do
      start_loop_server(mode: mode, concurrency: 1, queue_timeout: 1, _inline: false) do |port|
        reader = narrow_connection(port)
        reader.write("GET /stream HTTP/1.1\r\nHost: x\r\n\r\n")
        reader.readpartial(65_536)
        sleep 0.05 # the stream waits for the reader
        reader.close

        expect(get(port, "/hello")).to eq("hello"), "the stream kept the only slot"
      end
    end

    it "fails a read past max_body_size rather than end the body early" do
      log = capture_native_stderr do
        start_loop_server(mode: mode, max_body_size: 1024) do |port|
          status = post_chunked(port, "/echo", 8)

          expect(status).to satisfy { |line| line.nil? || !line.include?(" 200 ") }
        end
      end

      expect(log).to include("request body exceeded max_body_size")
    end

    it "runs socket events and wakes while every job slot is busy" do
      reports = self.reports.port
      worker_loop = waking_loop(Ractor.shareable_proc { reports << :woke }, concurrency: 1)

      with_loop_sockets(loop_app_with_sockets(EchoSocket), mode, worker_loop: worker_loop,
        after_request_complete: LINGERING) do |port, server|
        wakes = self.wakes
        client = connect(port)
        echo = lambda do |text|
          timed do
            client.send_text(text)
            expect(client.read).to eq([WsClient::TEXT, text])
          end
        end
        # An echo on the idle worker, and half a tick: an event that waits
        # for a tick beside the job takes longer.
        bound = Array.new(3) { echo.call("idle") }.min + 0.025

        held = timed do
          expect(get(port, "/linger")).to eq("hello")
          echoes = Array.new(3) { |index| echo.call(index.to_s) }
          # The middle of three: a tick's wait delays them all, a slow machine one.
          expect(echoes.sort[1]).to be < bound, "beside a held slot, echoes took #{echoes.map { |t| (t * 1000).round(1) }} ms"
          wakes << :wake
          expect(self.reports.next(awaiting: "the wake hook")).to eq(:woke)
        end
        expect(server.stats[:wakes]).to eq(1), "the wake it took counts in the server's wakes"
        # The job holds the slot 0.5 s past its answer: an event or a wake that waited for
        # the slot ends after that.
        expect(held).to be < 0.5, "the events and the wake took #{held.round(2)} s: they waited for the slot, or outlasted it"
      end
    end

    it "frees a job's slot when the job fails", :quiet do
      reports = self.reports.port
      worker_loop = Ractor.shareable_proc do |worker|
        Sync do |task|
          worker.serve(concurrency: 1) do |job|
            task.async do
              job.call
            rescue => e
              reports << e.message
            end
          end
        end
      end
      # Its failure report raises: the error escapes Kino's own rescue.
      app = Ractor.shareable_proc { |env| (env["PATH_INFO"] == "/fail") ? raise(UnreportableError) : LOOP_APP.call(env) }

      start_loop_server(app, mode: mode, worker_loop: worker_loop) do |port|
        expect(get(port, "/fail")).to eq("Internal Server Error\n")
        expect(self.reports.next(awaiting: "the job's failure")).to eq("the error's message raised")

        expect(get(port, "/hello")).to eq("hello")
      end
    end

    # Work after a response, and a socket's on_close: neither is done when
    # the drain is, and either may end last.
    {"a request's job" => [0.3, 0.05], "a socket's job" => [0.05, 0.3]}.each do |last, (after_response, on_close)|
      it "drains suspended requests, then ends the loop once every job is done (#{last} last)" do
        reports = self.reports.port
        after = Ractor.shareable_proc do |env, _status|
          next unless env["PATH_INFO"] == "/sleep"

          sleep after_response
          reports << :completed
        end
        app = loop_app_with_sockets(SlowClosingSocket.new(port: reports, pause: on_close))

        with_loop_sockets(app, mode, worker_loop: async_loop(4, returned: reports), after_request_complete: after,
          shutdown_timeout: 5) do |port, server|
          client = connect(port)
          sleeper = Thread.new { get(port, "/sleep") }
          wait_for("the request in the worker") { server.stats[:served] == 2 }

          shutdown = Thread.new { timed { server.shutdown } }
          client.await_close
          client.disconnect

          expect(shutdown.value).to be < 1
          expect(sleeper.value).to eq("slept")
          ends = Array.new(3) { self.reports.next(awaiting: "the loop's end") }
          expect(ends).to contain_exactly(:completed, :closed, :returned)
          expect(ends.last).to eq(:returned), "serve returned before its jobs were done: #{ends.inspect}"
        end
      end
    end

    # A request the respawned worker took into its lane waits anew, as a
    # new arrival does, once that worker crashes too: the respawns come
    # 0.1, 0.3 and 0.7 s after the first crash, so with queue_timeout at
    # 0.3 s it waits through two of them at most, and is answered by
    # 0.6 s.
    it "crashes a worker whose loop returns before the worker was drained", :quiet do
      start_loop_server(mode: mode, worker_loop: Ractor.shareable_proc { |_worker| }, queue_timeout: 0.3,
        on_worker_exit: on_worker_exit) do |port|
        expect_crash("worker_loop returned before its worker was drained")

        answered = timed { expect(get(port, "/hello")).to eq("Service Unavailable\n").or eq("Internal Server Error\n") }
        expect(answered).to be < 1
      end
    end

    it "warns once about a loop still running a second after its worker drained, and shuts down by the deadline all the same" do
      took = nil
      log = capture_native_stderr do
        start_loop_server(mode: mode, worker_loop: lingering_loop(3)) do |port, server|
          expect(get(port, "/hello")).to eq("hello")
          took = timed { server.shutdown(timeout: 2) }
        end
      end

      expect(log.scan(lingering_warning).size).to eq(1), log
      expect(took).to be_between(2, 2.9), "shutdown took #{took.round(2)} s against its 2 s deadline"
    end

    it "warns once about a retired worker's loop still running a second after its serve returned" do
      log = capture_native_stderr do
        start_loop_server(mode: mode, worker_loop: lingering_loop(2.5), max_workers: 2,
          on_worker_exit: on_worker_exit) do |port, server|
          expect(get(port, "/hello")).to eq("hello")
          retire_slot(server, 0)

          wait_for("the retired worker to leave") { worker_exits.size == 1 }
          expect(worker_exits.pop).to be_nil
        end
      end

      expect(log.scan(lingering_warning).size).to eq(1), log
    end

    it "does not warn about a retired worker's loop that returns within the second" do
      log = capture_native_stderr do
        start_loop_server(mode: mode, worker_loop: lingering_loop(0.3), max_workers: 2,
          on_worker_exit: on_worker_exit) do |port, server|
          expect(get(port, "/hello")).to eq("hello")
          retire_slot(server, 0)
          wait_for("the retired worker to leave") { worker_exits.size == 1 }

          # Past the second, with the pool scaler still looking.
          sleep 1
        end
      end

      expect(log).not_to include("the worker_loop is still running")
    end

    it "lets a retired worker that idles leave at its next tick" do
      start_loop_server(mode: mode, max_workers: 2, on_worker_exit: on_worker_exit) do |port, server|
        expect(get(port, "/hello")).to eq("hello")
        # Back on its doorbell, with nothing to take.
        sleep 0.01

        retire_slot(server, 0)
        left = timed { wait_for("the retired worker to leave", every: 0.005) { worker_exits.size == 1 } }

        expect(worker_exits.pop).to be_nil
        # Its next tick is 50 ms away, or a few on a loaded machine; a
        # worker that missed its retirement waits for a request instead.
        expect(left).to be < 0.5, "the retired worker left #{(left * 1000).round} ms after its retirement"
      end
    end

    it "leaves the GC and allocation figures out of the access log" do
      log = capture_native_stdout(awaiting: /← 200 GET \/hello.*\n/) do
        start_loop_server(mode: mode, log_requests: true) do |port|
          expect(get(port, "/hello")).to eq("hello")
        end
      end

      expect(log).to match(/← 200 GET \/hello  \d+\.\dms \(ruby \d+\.\dms; kino/)
      expect(log).not_to include("[gc")
    end

    it "refuses to serve without a fiber scheduler, and serves once it has one" do
      reports = self.reports.port
      worker_loop = Ractor.shareable_proc do |worker|
        worker.serve(concurrency: 1) { |job| job.call }
      rescue ArgumentError => e
        reports << e.message
        Sync { |task| worker.serve(concurrency: 1) { |job| task.async { job.call } } }
      end

      start_loop_server(mode: mode, worker_loop: worker_loop) do |port|
        expect(self.reports.next(awaiting: "the refusal")).to eq("worker.serve needs a fiber scheduler on this thread")
        expect(get(port, "/hello")).to eq("hello")
      end
    end

    it "keeps each socket's events in order while sockets run concurrently" do
      with_loop_sockets(loop_app_with_sockets(OrderedSocket.new(port: reports.port)), mode) do |port|
        sockets = Array.new(4) { connect(port) }
        (1..100).each { |number| sockets.each { |socket| socket.send_text(number.to_s) } }
        seen = Array.new(400) { reports.next(awaiting: "400 numbered messages") }

        by_socket = seen.group_by(&:first).transform_values { |entries| entries.map { |entry| entry[1] } }
        expect(by_socket.size).to eq(4)
        by_socket.each_value { |numbers| expect(numbers).to eq((1..100).to_a) }
        expect(seen.filter_map { |entry| entry[2] }.max).to be > 1, "the sockets' callbacks never ran at once"
      end
    end

    it "never runs the wake hook twice at once, and folds the wakes made while it runs into one more run" do
      reports = self.reports.port
      hook = Ractor.shareable_proc do
        RunningCallbacks.count do |running|
          reports << :overlap if running > 1
          reports << :started
          # The first run outlasts the wakes made during it on any machine.
          thread = Thread.current
          sleep(thread.thread_variable_get(:spec_ran) ? 0.1 : 1)
          thread.thread_variable_set(:spec_ran, true)
        end
      end

      start_loop_server(mode: mode, worker_loop: waking_loop(hook)) do
        wakes = self.wakes
        wakes << :wake
        expect(self.reports.next(awaiting: "the first run")).to eq(:started)
        10.times { wakes << :wake }
        expect(self.reports.next(awaiting: "the folded run")).to eq(:started)
        sleep 0.3 # past the folded run, which a third would follow at once

        expect(reports_so_far).to eq([])
      end
    end

    it "runs at most a backlog's worth of socket callbacks at once, and serves requests meanwhile" do
      FileGate.closed do |gate|
        with_loop_sockets(loop_app_with_sockets(HoldingSocket.new(port: reports.port, gate: gate)), mode) do |port|
          Array.new(70) { connect(port) }.each { |socket| socket.send_text("hold") }
          holding = Array.new(64) { reports.next(awaiting: "64 callbacks holding") }
          # Answered while all 64 still hold: they let go only at the gate.
          expect(get(port, "/hello")).to eq("hello")
          gate.open

          rest = Array.new(6 + 70) { reports.next(awaiting: "every callback") }
          expect((holding + rest).count(:held)).to eq(70)
          expect((holding + rest).filter_map { |report| report[1] if report.is_a?(Array) }.max).to eq(64)
        end
      end
    end

    it "keeps running events after a callback or the wake hook raised" do
      reports = self.reports.port
      on_error = Ractor.shareable_proc { |_error, env| reports << env["kino.callback"] }
      worker_loop = waking_loop(Ractor.shareable_proc { raise "wake hook failed" })

      log = capture_native_stderr do
        with_loop_sockets(ws_app(FragileSocket), mode, worker_loop: worker_loop, on_error: on_error) do |port, server|
          wakes = self.wakes
          crashes = server.stats[:websockets][:closed][:crash]
          steady = connect(port)
          # Each socket whose callback raised is dropped as a crash, its
          # failure reported or not: one each, as a drop races the
          # messages behind it.
          fragile = %w[boom unreportable deeply\ unreportable].map do |message|
            connect(port).tap { |client| client.send_text(message) }
          end
          expect(self.reports.next(awaiting: "on_error")).to eq("on_message")
          wakes << :wake
          expect(self.reports.next(awaiting: "on_error")).to eq("on_wake")

          steady.send_text("still here")
          expect(steady.read).to eq([WsClient::TEXT, "still here"])
          fragile.each { |client| expect(client.read).to be_nil }
          wait_for("three crashes") { server.stats[:websockets][:closed][:crash] - crashes == 3 }
        end
      end

      expect(log).to include("the error's message raised", "worker_loop event · UnreportableError (its report failed too)",
        "on_message · RuntimeError: boom", "on_wake hook · RuntimeError: wake hook failed")
    end

    [:full_hijack, :partial_hijack, :streaming_body].each do |kind|
      it "frees a job's slot once its connection switched, while the job keeps serving it (#{kind})" do
        with_loop_sockets(echoing_switch(kind, reports.port), mode, worker_loop: async_loop(1), queue_timeout: 1,
          after_request_complete: LINGERING) do |port|
          client = connect(port)
          client.send_text("echoed")
          expect(client.read).to eq([WsClient::TEXT, "echoed"])

          # Behind a slot the switched connection still held, it would wait out the queue timeout.
          expect(timed { expect(get(port, "/hello")).to eq("hello") }).to be < 0.5
          client.send_text("still echoed")
          expect(client.read).to eq([WsClient::TEXT, "still echoed"])

          # Its job's end frees no second slot: a lingering job, answered
          # (so out of the native count), still holds the only one.
          client.close
          expect(reports.next(awaiting: "the switched connection's end")).to eq(:ended)
          # Timed from before /linger, whose job lingers only once it has answered: a
          # client held up between the two requests still sees the wait.
          pair = timed do
            expect(get(port, "/linger")).to eq("hello")
            expect(get(port, "/hello")).to eq("hello")
          end
          expect(pair).to be >= 0.3
        end
      end
    end

    it "lets a request hijack its connection while a fiber of its own waits on the body" do
      reports = self.reports.port
      app = Ractor.shareable_proc do |env|
        input = env["rack.input"]
        reader = Async { input.read }
        sleep 0.05 # the reader's fiber now waits on the body, which the client never sends
        begin
          io = env["rack.hijack"].call
          reports << :hijacked
          io.write("HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok")
          io.close
        rescue IOError => e
          reports << e.message
        end
        reader.stop
        [-1, {}, []]
      end

      start_loop_server(app, mode: mode) do |port|
        socket = client_socket(port)
        socket.write("POST / HTTP/1.1\r\nHost: x\r\nUpgrade: chat\r\nConnection: Upgrade\r\nContent-Length: 5\r\n\r\n")

        expect(self.reports.next(awaiting: "the hijack")).to eq(:hijacked)
        socket.write("hello") # the body, for the connection to end on
        expect(socket.read).to end_with("\r\n\r\nok")
      ensure
        socket&.close
      end
    end

    it "waits for a switched connection's job before the loop ends" do
      reports = self.reports.port

      with_loop_sockets(echoing_switch(:streaming_body, reports), mode, worker_loop: async_loop(1, returned: reports),
        shutdown_timeout: 2) do |port, server|
        client = connect(port)
        client.send_text("echoed")
        expect(client.read).to eq([WsClient::TEXT, "echoed"])

        # Once requests have drained the switched connection closes: its
        # job reads EOF and ends, and only then does the loop return.
        expect(timed { server.shutdown }).to be < 1
        expect(Array.new(2) { self.reports.next(awaiting: "the loop's end") }).to eq([:ended, :returned])
      end
    end

    it "crashes a worker whose socket's job ended before its events ran", :quiet do
      with_loop_sockets(loop_app_with_sockets(StallingSocket.new(port: reports.port)), mode,
        worker_loop: stopping_second_job(0.05), on_worker_exit: on_worker_exit, _inline: false) do |port|
        connect(port).send_text("hold")

        expect_crash("a worker_loop job ended before its socket events or wakes were done")
      end
    end

    it "reports a socket's job that ended before its on_close ran while the worker drained", :quiet do
      app = loop_app_with_sockets(SlowClosingSocket.new(port: reports.port, pause: 0.2))

      with_loop_sockets(app, mode, worker_loop: stopping_second_job(0.1), on_worker_exit: on_worker_exit, _inline: false) do |port, server|
        client = connect(port)
        shutdown = Thread.new { server.shutdown }
        client.await_close
        client.disconnect
        shutdown.join

        expect_crash("a worker_loop job ended before its socket events or wakes were done")
      end
    end

    # The handshake times out while on_open runs, so the socket's close
    # waits behind on_open, for a job of its own.
    it "crashes a worker whose scheduler refuses the job for what waited behind a socket's on_open", :quiet do
      app = loop_app_with_sockets(SlowOpeningSocket.new(port: reports.port))

      # Its scheduler refuses the second job, a socket's, as one that is
      # closing would.
      refusing = Ractor.shareable_proc do |worker|
        Sync do |task|
          jobs = 0
          worker.serve(concurrency: 4) do |job|
            raise "the scheduler is closing" if (jobs += 1) == 2

            task.async { job.call }
          end
        end
      end
      with_loop_sockets(app, mode, worker_loop: refusing, on_worker_exit: on_worker_exit,
        request_timeout: 0.1, shutdown_timeout: 2, _inline: false) do |port, server|
        expect(refusal(port)&.status).to eq(504)

        expect_crash("a worker_loop job ended before its socket events or wakes were done")
        expect(timed { server.shutdown }).to be < 1
      end
    end

    it "frees a job's slot even when the app takes kino.request out of the env" do
      app = Ractor.shareable_proc do |env|
        env.delete("kino.request")
        LOOP_APP.call(env)
      end

      start_loop_server(app, mode: mode, concurrency: 1, queue_timeout: 1, shutdown_timeout: 2) do |port, server|
        2.times { expect(get(port, "/hello")).to eq("hello") }
        expect(timed { server.shutdown }).to be < 1
      end
    end

    it "runs a socket's events after its on_open, even when its handshake timed out meanwhile" do
      app = loop_app_with_sockets(SlowOpeningSocket.new(port: reports.port))

      with_loop_sockets(app, mode, request_timeout: 0.1) do |port|
        expect(refusal(port)&.status).to eq(504)
        expect(Array.new(2) { reports.next(awaiting: "the socket's callbacks") }).to eq([:opened, :closed])
      end
    end

    it "streams to many slow readers without waking while they wait" do
      start_loop_server(mode: mode, concurrency: 256) do |port, server|
        # Each stream far outgrows its socket's buffers. Few enough streams
        # that their full send buffers leave a small macOS machine's network
        # buffers to spare: past them, a write fails with ENOBUFS and ends its
        # stream, as it would through hyper.
        readers = Array.new(64) do
          narrow_connection(port).tap { |reader| reader.write("GET /events?count=1048576 HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n") }
        end
        heads = readers.map do |reader|
          reader.readpartial(4096)[/\A[^\r]*/]
        rescue EOFError
          :eof
        end
        expect(heads.tally).to eq("HTTP/1.1 200 OK" => 64)
        wait_for("every stream in the worker", timeout: 30, detail: -> { server.stats[:worker_status] }) do
          open_requests(server) == 64
        end
        wait_for("a quarter second under 5% of a core", timeout: 30, every: 0) { cpu_during(0.25) < 0.05 * 0.25 }

        busy = cpu_during(2)
        expect(busy).to be < 0.1, "the server used #{(busy * 50).round}% of a core while its streams waited on their readers"
        expect(timed { expect(get(port, "/hello")).to eq("hello") }).to be < 0.5
      ensure
        readers&.each(&:close)
      end
    end

    it "reads a trickled upload without polling" do
      reports = self.reports.port
      app = Ractor.shareable_proc do |env|
        body = env["rack.input"].read
        reports << TallyingReactor.of(Fiber.current)
        [200, {}, [body]]
      end
      pieces = Array.new(32) { |index| format("%04d", index) * 256 }

      # Through hyper, which hands the body over as it comes; inline,
      # it is read whole before the app runs.
      start_loop_server(app, mode: mode, worker_loop: loop_on(TallyingReactor, 8), _inline: false) do |port|
        client = TCPSocket.new("127.0.0.1", port)
        client.timeout = WsClient::DEADLINE
        client.write("POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: #{pieces.sum(&:bytesize)}\r\nConnection: close\r\n\r\n")
        pieces.each do |piece|
          sleep 0.01
          client.write(piece)
        end

        expect(response_body_of(client.read)).to eq(pieces.join)
        waits, sleeps = self.reports.next(awaiting: "the upload's waits")
        expect(sleeps).to eq(0), "the read slept #{sleeps} times while the client trickled its body"
        expect(waits).to be_between(1, pieces.size + 1)
      ensure
        client&.close
      end
    end

    it "closes readiness fds when requests end" do
      reports = self.reports.port
      # Each request's waits, from after_request_complete, or from on_error
      # for a stream that failed.
      waited = Ractor.shareable_proc { |*| reports << TallyingReactor.of(Fiber.current).first }
      kinds = %i[slow gone failing upload peek]

      capture_native_stderr do
        # Served through hyper, where a request's body I/O waits on
        # readiness fds of its own. An inline request waits on its
        # connection's socket instead, if at all: it reads its body whole
        # before the app runs, and a send buffer that takes its whole
        # stream (Linux starts a loopback one at 2.5 MiB) never makes it
        # wait.
        start_loop_server(mode: mode, worker_loop: loop_on(TallyingReactor, 32), after_request_complete: waited,
          on_error: waited, _inline: false) do |port, server|
          batch = -> { Array.new(20) { |index| Thread.new { waiting_request(port, kinds[index % kinds.size]) } }.each(&:join) }
          waits = ->(count) { Array.new(count) { self.reports.next(awaiting: "a request's waits") } }
          batch.call
          wait_for("the first requests done") { open_requests(server).zero? }
          baseline = open_fds
          waits.call(20)

          GC.disable
          begin
            25.times { batch.call }
            wait_for("every request done") { open_requests(server).zero? }
            wait_for("the readiness fds of 500 requests closed (#{open_fds - baseline} open)", timeout: 2) { open_fds <= baseline + 2 }
          ensure
            GC.enable
          end
          waiting = waits.call(500).count(&:positive?)
          expect(waiting).to be > 400, "#{waiting} of the 500 requests waited on a readiness fd"
        end
      end
    end

    # The fiber and the thread share the connection's socket.
    it "lets a fiber write while a thread of the same request is blocked writing, until the client leaves (served inline)" do
      reports = self.reports.port
      app = Ractor.shareable_proc do |env|
        next LOOP_APP.call(env) unless env["PATH_INFO"] == "/both"

        body = lambda do |stream|
          writer = Thread.new do
            200.times { stream.write("x" * 65_536) }
          rescue IOError
            nil # the stream closed under it
          end
          sleep 0.3 # a fiber wait, while the thread blocks on the client
          begin
            stream.write("from the fiber")
          rescue IOError => e
            reports << e.class.name # its write waited for the client, who left
          end
          writer.join
        end
        [200, {}, body]
      end

      start_loop_server(app, mode: mode) do |port, server|
        await_inline(server, port, 1)
        client = narrow_connection(port)
        client.write("GET /both HTTP/1.1\r\nHost: x\r\n\r\n")
        client.readpartial(4096)
        sleep 0.5
        client.close

        expect(self.reports.next(awaiting: "the fiber's write")).to eq("Kino::ClientGone")
        expect(server.stats.dig(:inline, :requests)).to eq(1), "the request went another way: #{server.stats[:inline].inspect}"
        expect(get(port, "/hello")).to eq("hello")
      ensure
        client&.close
      end
    end

    it "closes the readiness fds of a classic worker's requests that waited under the app's own scheduler" do
      app = Ractor.shareable_proc { |env| [200, {}, [Sync { env["rack.input"].read(6) }]] }

      with_server(app, mode: mode, workers: 1, threads: 1) do |_host, port|
        expect(response_body_of(waiting_request(port, :peek))).to eq("halfha")
        baseline = open_fds

        GC.disable
        begin
          40.times { waiting_request(port, :peek) }
          wait_for("the readiness fds of 40 requests closed (#{open_fds - baseline} open)", timeout: 2) do
            open_fds <= baseline + 2
          end
        ensure
          GC.enable
        end
      end
    end

    it "fails a body read still waiting on its client once shutdown passes its deadline", :quiet do
      reports = self.reports.port
      app = Ractor.shareable_proc do |env|
        env["rack.input"].read
        reports << :read
        [200, {}, ["read"]]
      rescue => e
        reports << e.message
        raise
      end

      start_loop_server(app, mode: mode, worker_loop: async_loop(4, returned: reports)) do |port, server|
        client = TCPSocket.new("127.0.0.1", port)
        client.timeout = WsClient::DEADLINE
        # Chunked: a body short enough to read whole before the app runs (inline) never waits.
        client.write("POST /upload HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n4\r\nhalf\r\n")
        wait_for("the upload in the worker") { open_requests(server) == 1 }

        expect(timed { server.shutdown(timeout: 0.2) }).to be < 1
        expect(Array.new(2) { self.reports.next(awaiting: "the read's end") })
          .to eq(["Kino: request interrupted during shutdown", :returned])
      ensure
        client&.close
      end
    end

    it "turns lanes on for a worker_loop" do
      # As the CLI and the Rack handler build a server: every setting,
      # the defaults filled in.
      configured = Kino::Configuration.new.merge!(mode: mode, worker_loop: async_loop(8)).server_options
      expect(Kino::Server.new(LOOP_APP, **configured).stats).to include(lanes: true)

      with_server(LOOP_APP, mode: mode, workers: 1, threads: 1, worker_loop: async_loop(8)) do |_, port, server|
        expect(get(port, "/hello")).to eq("hello")
        expect(server.stats).to include(:lane_depths, lanes: true)
      end
    end

    it "refuses worker_loop with lanes false" do
      expect { Kino::Server.new(LOOP_APP, mode: mode, lanes: false, worker_loop: async_loop(1)) }
        .to raise_error(ArgumentError, /\Aworker_loop needs lanes/)
    end

    # A job that spins at once keeps its loop from its next take; one that
    # spins after a fiber wait, from the end of its wait on the doorbell.
    {"at once" => "", "after a fiber wait" => "after_wait=1&"}.each do |kind, query|
      it "quarantines a worker whose job keeps its thread from the loop (#{kind}), and serves on without it" do
        FileGate.closed do |gate|
          release = gate.path
          start_loop_server(stalling_app, mode: mode, quarantine_timeout: 0.2) do |port, server|
            spinner = Thread.new { get(port, "/spin?#{query}release=#{release}") }
            wait_for("the stuck worker quarantined and replaced") do
              stats = server.stats
              stats[:quarantined] == 1 && stats[:worker_status].length == 2
            end

            expect(get(port, "/hello")).to eq("hello")
            gate.open
            expect(spinner.value).to eq("hello"), "quarantine must leave the stuck request to finish"
          ensure
            gate.open
            spinner&.join
          end
        end
      end
    end

    it "leaves a worker alone whose requests wait past quarantine_timeout while its thread keeps coming back to the loop" do
      start_loop_server(stalling_app, mode: mode, quarantine_timeout: 0.5) do |port, server|
        dozers = Array.new(2) { Thread.new { get(port, "/doze") } }
        wait_for("the requests in the worker") { open_requests(server) == 2 }
        busy = busy_while(server, Thread.new { dozers.each(&:join) })

        expect(dozers.map(&:value)).to all(eq("dozed"))
        # Back to the loop every TICK while they wait: about a TICK at
        # most, never near the timeout.
        expect(busy.max).to be < 250
        expect(server.stats[:quarantined]).to eq(0)
        expect(server.stats[:worker_status].length).to eq(1)
      end
    end

    it "leaves a worker alone whose job slots and events backlog stay full past quarantine_timeout" do
      with_loop_sockets(stalling_app(StallingSocket.new(port: reports.port)), mode, worker_loop: async_loop(1),
        quarantine_timeout: 0.5) do |port, server|
        client = connect(port)
        dozer = Thread.new { get(port, "/doze") }
        wait_for("the request in the only job slot") { open_requests(server) == 1 }
        client.send_text("hold")
        expect(reports.next(awaiting: "the held callback")).to eq(:holding)
        # A backlog's worth behind it, and some for the inbox.
        70.times { client.send_text("x") }
        busy = busy_while(server, dozer)

        expect(dozer.value).to eq("dozed")
        # Back to the loop every TICK while it waits: about a TICK at
        # most, never near the timeout.
        expect(busy.max).to be < 250
        expect(server.stats[:quarantined]).to eq(0)
        expect(server.stats[:worker_status].length).to eq(1)
      end
    end
  end
end
