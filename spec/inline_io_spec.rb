# frozen_string_literal: true

require "socket"
require "stringio"

# The size of INLINE_APP's /big.
INLINE_BIG = 8 << 20

# What an inline server serves: /sleep waits 0.1 s (or ?seconds);
# /echo answers with the request body, its method in x-method; /stream
# streams 10,000 "a"s
# three times from an Enumerable body, /stream3 "x" then "y" from a Rack
# 3 streaming one; /big answers INLINE_BIG bytes at once, or with ?stream
# 64 KiB a write, its length given; anything else is "hello".
INLINE_APP = Ractor.shareable_proc do |env|
  case env["PATH_INFO"]
  when "/sleep"
    sleep(env["QUERY_STRING"].empty? ? 0.1 : Float(env["QUERY_STRING"]))
    [200, {"content-type" => "text/plain"}, ["slept"]]
  when "/echo"
    [200, {"content-type" => "text/plain", "x-method" => env["REQUEST_METHOD"]}, [env["rack.input"].read]]
  when "/stream"
    [200, {"content-type" => "text/plain"}, Array.new(3) { "a" * 10_000 }.each]
  when "/stream3"
    [200, {"content-type" => "text/plain"}, ->(stream) { stream.write("x") && stream.write("y") && stream.close }]
  when "/big"
    if env["QUERY_STRING"] == "stream"
      [200, {"content-length" => INLINE_BIG.to_s}, Array.new(INLINE_BIG / 65_536, "x" * 65_536).each]
    else
      [200, {}, ["x" * INLINE_BIG]]
    end
  else
    [200, {"content-type" => "text/plain"}, ["hello"]]
  end
end

# /fatal raises an exception that is no StandardError, /stop stops the
# request's Async task, /kill kills the thread running it. /stall holds
# its worker's thread 0.5 s, out of the scheduler's reach, then raises an
# exception that is no StandardError, which Async lets take the worker
# down. Anything else is INLINE_APP's.
ENDING_APP = Ractor.shareable_proc do |env|
  case env["PATH_INFO"]
  when "/fatal" then raise Exception, "no StandardError" # rubocop:disable Lint/RaiseException
  when "/stop" then Async::Task.current.stop
  when "/kill" then Thread.current.kill
  when "/stall"
    Kino.sleep(0.5)
    raise Exception, "the worker died" # rubocop:disable Lint/RaiseException
  end
  INLINE_APP.call(env)
end

# A body of "done" whose close, which Kino calls once the answer is out,
# holds its worker's thread at `gate`, out of the scheduler's reach, until
# the spec opens it.
GatedBody = Data.define(:gate) do
  def to_ary = ["done"]

  def close = Fiber.blocking { gate.wait }
end

# Async's reactor, except that a wait for a socket to take more starts
# late: a descriptor handed to the wait may be closed meanwhile.
class SlowToWaitReactor < Async::Reactor
  def io_wait(io, events, timeout = nil)
    Fiber.blocking { sleep 0.3 } if events.anybits?(IO::WRITABLE)
    super
  end
end

# Async's reactor, except that it counts each wait for a socket to take
# more, in a thread variable of its thread (its fibers share it).
class WritableWaitsReactor < Async::Reactor
  def self.count = Thread.current.thread_variable_get(:spec_writable_waits).to_i

  def io_wait(io, events, timeout = nil)
    Thread.current.thread_variable_set(:spec_writable_waits, WritableWaitsReactor.count + 1) if events.anybits?(IO::WRITABLE)
    super
  end
end

# Async's reactor, except that the worker's loop, woken on its doorbell,
# runs only after the other fibers that were ready with it: an order of
# the scheduler's choosing that the loop must not depend on.
class LoopLastReactor < Async::Reactor
  def io_wait(io, ...)
    super.tap do
      worker = Thread.current.thread_variable_get(:kino_worker)
      Fiber.scheduler.yield if worker && io.fileno == worker.ready_fd
    end
  end
end

# Async's reactor, except that it counts each time a fiber steps aside on
# it, by how, in a thread variable of its thread (its fibers share it):
# [its yields, its kernel_sleep(0)s].
class SteppingAsideReactor < Async::Reactor
  def self.steps = Thread.current.thread_variable_get(:spec_steps) || Thread.current.thread_variable_set(:spec_steps, [0, 0])

  def yield
    SteppingAsideReactor.steps[0] += 1
    super
  end

  def kernel_sleep(duration = nil)
    SteppingAsideReactor.steps[1] += 1 if duration == 0
    super
  end
end

# A SteppingAsideReactor without a yield of its own, as a scheduler that
# has none.
class YieldlessReactor < SteppingAsideReactor
  undef_method :yield
end

# /steps answers its worker thread's SteppingAsideReactor.steps so far,
# as "yields,sleeps"; anything else is "hello".
STEPS_APP = Ractor.shareable_proc do |env|
  body = (env["PATH_INFO"] == "/steps") ? SteppingAsideReactor.steps.join(",") : "hello"
  [200, {}, [body]]
end

RSpec.describe "Inline connections" do
  include_context "native WebSocket server"

  def request(path, close: false)
    "GET #{path} HTTP/1.1\r\nhost: x\r\n#{"connection: close\r\n" if close}\r\n"
  end

  # with_inline_server, bound to a unix socket in a directory of its own:
  # yields the socket's path and the server.
  def with_unix_inline_server(app, mode:, **opts)
    Dir.mktmpdir("kino-unix-inline") do |dir|
      path = File.join(dir, "kino.sock")
      with_inline_server(app, mode: mode, bind: "unix://#{path}", **opts) { |*, server| yield path, server }
    end
  end

  # /forever streams 64 KiB a write until one fails, from a Rack 3
  # streaming body, and reports to `port` what failed it; /helper writes
  # from a helper thread instead, while the body waits for it; /duplex
  # too, and the body reports the request body it reads meanwhile;
  # /outlive's helper writes on (under a SlowToWaitReactor) after the
  # body returned, 0.1 s in. Anything else is INLINE_APP's.
  def leaving_app(port)
    Ractor.shareable_proc do |env|
      chunk = "x" * 65_536
      forever = lambda do |stream|
        loop { stream.write(chunk) }
      rescue => e
        port << e.class.name
      end
      case env["PATH_INFO"]
      when "/forever" then [200, {}, forever]
      when "/helper" then [200, {}, ->(stream) { Thread.new { forever.call(stream) }.join }]
      when "/outlive"
        outlive = lambda do |stream|
          Thread.new do
            Fiber.blocking do
              SlowToWaitReactor.new.run(finished: false) { forever.call(stream) }.wait
            ensure
              Fiber.set_scheduler(nil)
            end
          end
          sleep 0.1
        end
        [200, {}, outlive]
      when "/duplex"
        duplex = lambda do |stream|
          writer = Thread.new { forever.call(stream) }
          sleep 0.2 # the helper's writes fill what the client leaves unread
          port << stream.read(5)
          writer.join
        end
        [200, {}, duplex]
      else INLINE_APP.call(env)
      end
    end
  end

  # Whether a GET of /hello gets its answer now, on a new connection.
  def serves_hello?(port)
    get(port, "/hello") == "hello"
  rescue
    false
  end

  # STEPS_APP's count on `socket`'s connection: [yields, sleeps].
  def steps(socket)
    socket.write(request("/steps"))
    read_response(socket).last.split(",").map { Integer(it) }
  end

  # The steps aside between two counts of #steps.
  def stepped(from, to) = to.zip(from).map { |now, before| now - before }

  # A worker_loop on a SteppingAsideReactor whose jobs each run to their
  # end on a blocking fiber, which has no current scheduler: a
  # connection's job holds the worker's thread, its waits on the socket
  # included, until the connection ends.
  def blocking_jobs_loop
    Ractor.shareable_proc do |worker|
      Fiber.blocking do
        reactor = SteppingAsideReactor.new
        reactor.run(finished: false) do
          worker.serve(concurrency: 8) { |job| Fiber.new(blocking: true) { job.call }.resume }
        end.wait
      ensure
        Fiber.set_scheduler(nil)
      end
    end
  end

  # The runner recipe in doc/worker-loop.md (#documented_job_fibers),
  # its Sync running in the task of a `reactor_class` reactor (an
  # Async::Reactor): its runners take their jobs, and step aside, on
  # that scheduler.
  def runners_on(reactor_class)
    recipe = documented_job_fibers
    Ractor.shareable_proc do |worker|
      Fiber.blocking do
        reactor_class.new.run(finished: false) { recipe.call(worker) }.wait
      ensure
        Fiber.set_scheduler(nil)
      end
    end
  end

  %i[threaded ractor].each do |mode|
    context "in #{mode} mode" do
      it "a worker_loop server serves plain HTTP/1.1 inline" do
        with_inline_server(INLINE_APP, mode: mode) do |_, port, server|
          expect(get(port, "/hello")).to eq("hello")
          expect(server.stats[:inline]).to include(requests: 1, handbacks: 0, closed: 0)
          # Served, as through hyper, by the server and by its worker.
          expect(server.stats).to include(served: 1)
          expect(server.stats[:worker_status].sum { it[:served] }).to eq(1)
        end
      end

      # The job of a connection alone on its worker spins for its next
      # request before it waits on the socket: here each comes 5 ms after
      # the last answer, far past the spin, so the spins run out and the
      # connection backs off to spinning now and then. (With one CPU
      # nothing spins: the native spin gate.)
      it "spins for a lone connection's next request, backing off for a client that keeps coming late" do
        allow(Kino).to receive(:available_parallelism).and_return(4)
        with_inline_server(INLINE_APP, mode: mode) do |_, port, server|
          before = server.stats[:inline][:spins]
          socket = TCPSocket.new("127.0.0.1", port).tap { it.timeout = WsClient::DEADLINE }
          40.times do |i|
            sleep 0.005
            socket.write(request("/late/#{i}"))
            expect(read_response(socket).last).to eq("hello"), "request #{i}"
          end
          socket.close
          expect(server.stats[:inline][:spins] - before).to be_between(1, 15)
        end
      end

      # A worker_loop's lines carry no GC or allocation figures, inline or
      # through hyper (an HTTP/1.0 request, handed back).
      it "logs inline requests" do
        log = capture_native_stdout do
          with_inline_server(INLINE_APP, mode: mode, log_requests: true) do |_, port, server|
            expect(get(port, "/hello")).to eq("hello")
            expect(server.stats[:inline][:requests]).to eq(1)
            expect(server.stats[:queue_time]).to include(count: 0)
            # A streamed answer's line follows its head, timed alike.
            expect(get(port, "/stream")).to eq("a" * 30_000)
            expect(server.stats[:inline][:requests]).to eq(2)
            expect(raw_exchange(port, "GET /handed-back HTTP/1.0\r\n\r\n")).to end_with("\r\n\r\nhello")
            expect(server.stats[:inline]).to include(requests: 2, handbacks: 1)
          end
        end
        %w[/hello /stream].each do |path|
          expect(log.scan("→ GET #{path}  from 127.0.0.1").size).to eq(1), log
          completion = %r{← 200 GET #{path}  \d+\.\dms \(ruby \d+\.\dms; kino \d+\.\dms; wait 0\.0ms\)}
          expect(log.scan(completion).size).to eq(1), log
        end
        expect(log).to match(%r{← 200 GET /handed-back  \d+\.\dms \(ruby \d+\.\dms; kino}), log
        expect(log).not_to include("[gc")
      end

      it "serves a request sent as its connection's answer went out after one the scheduler found waiting already, and one sent with the answered one at once" do
        FileGate.closed do |pause|
          FileGate.closed do |first|
            FileGate.closed do |second|
              # Reports each request's path to `events` as it starts. /hold/N answers,
              # then holds its worker's thread at gates[N] once its answer is out (a
              # GatedBody); anything else answers at once.
              events = reports.port
              gates = [pause, first, second].freeze
              app = Ractor.shareable_proc do |env|
                path = env["PATH_INFO"]
                events << path
                [200, {}, path.start_with?("/hold/") ? GatedBody.new(gates[Integer(path.delete_prefix("/hold/"))]) : ["done"]]
              end
              with_inline_server(app, mode: mode, workers: 1) do |_, port|
                # A regression fails on a read that times out rather than hang.
                answered, waiting, pausing = Array.new(3) { TCPSocket.new("127.0.0.1", port).tap { it.timeout = 5 } }
                # Each served once: every job waits on its socket.
                [answered, waiting, pausing].each do |socket|
                  socket.write(request("/"))
                  expect(read_response(socket).last).to eq("done")
                  reports.next(awaiting: "a first request")
                end
                # A third connection holds the worker's thread while one
                # request comes, then another's, so the scheduler finds both
                # at one look, in that order. The first one's answer goes
                # out, then the thread is held again, as a job's work after
                # its answer may hold it, while that connection's client
                # sends its next request.
                pausing.write(request("/hold/0"))
                expect(read_response(pausing).last).to eq("done")
                answered.write(request("/hold/1"))
                sleep 0.02
                waiting.write(request("/waiting"))
                # Over loopback each reaches the server's socket well within this.
                sleep 0.05
                pause.open
                expect(read_response(answered).last).to eq("done")
                answered.write(request("/answered"))
                sleep 0.05
                first.open
                order = Array.new(4) { reports.next(awaiting: "the requests' starts") }
                expect(order).to eq(%w[/hold/0 /hold/1 /waiting /answered]),
                  "the request sent on the answered connection ran ahead of the one waiting already: #{order}"
                expect([answered, waiting].map { read_response(it).last }).to all(eq("done"))

                # One sent with the answered request is read with it, and
                # runs at once.
                answered.write(request("/hold/2") + request("/pipelined"))
                expect(read_response(answered).last).to eq("done")
                waiting.write(request("/waiting"))
                sleep 0.05
                second.open
                order = Array.new(3) { reports.next(awaiting: "the requests' starts") }
                expect(order).to eq(%w[/hold/2 /pipelined /waiting]),
                  "the request read with the answered one waited behind another connection's: #{order}"
                expect([answered, waiting].map { read_response(it).last }).to all(eq("done"))
              ensure
                [answered, waiting, pausing].compact.each(&:close)
              end
            end
          end
        end
      end

      # Each worker_loop, and the step aside it should see: [yields,
      # kernel_sleep(0)s].
      {
        "on a scheduler with a yield of its own" => [-> { loop_on(SteppingAsideReactor, 8) }, [1, 0]],
        "on one without, through kernel_sleep(0)" => [-> { loop_on(YieldlessReactor, 8) }, [0, 1]],
        "with the runner recipe in doc/worker-loop.md" => [-> { runners_on(SteppingAsideReactor) }, [1, 0]]
      }.each do |how, (worker_loop, once)|
        it "steps a connection's job aside once after each answer while another shares its worker, never alone (#{how})" do
          with_inline_server(STEPS_APP, mode: mode, workers: 1, worker_loop: instance_exec(&worker_loop)) do |_, port, server|
            socket = TCPSocket.new("127.0.0.1", port).tap { it.timeout = 5 }
            alone = Array.new(3) { steps(socket) }
            expect(alone.each_cons(2).map { stepped(*it) }).to all(eq([0, 0])),
              "a connection alone on its worker stepped aside: #{alone}"

            other = TCPSocket.new("127.0.0.1", port).tap { it.timeout = WsClient::DEADLINE }
            wait_for("both connections inline") { server.stats[:inline][:connections] == 2 }
            shared = Array.new(3) { steps(socket) }
            expect(shared.each_cons(2).map { stepped(*it) }).to all(eq(once)),
              "a connection sharing its worker did not step aside once after each answer: #{shared}"
          ensure
            [socket, other].compact.each(&:close)
          end
        end
      end

      it "never steps a connection's job aside on a blocking fiber, which has no current scheduler" do
        with_inline_server(STEPS_APP, mode: mode, workers: 1, worker_loop: blocking_jobs_loop) do |_, port, server|
          socket = TCPSocket.new("127.0.0.1", port).tap { it.timeout = 5 }
          # Served once: its job holds the worker's thread from here on.
          first = steps(socket)
          expect(server.stats[:inline][:requests]).to eq(1)
          # Placed on the worker beside it, and taken once its job ends.
          other = TCPSocket.new("127.0.0.1", port).tap { it.timeout = 5 }
          wait_for("both connections inline") { server.stats[:inline][:connections] == 2 }
          counts = [first] + Array.new(3) { steps(socket) }
          expect(counts.each_cons(2).map { stepped(*it) }).to all(eq([0, 0])),
            "a job on a blocking fiber stepped aside: #{counts}"
          socket.close
          other.write(request("/hello"))
          expect(read_response(other).last).to eq("hello")
        ensure
          [socket, other].compact.each(&:close)
        end
      end

      {"buffered" => "/big", "streamed" => "/big?stream"}.each do |kind, path|
        it "a slow reader of a #{kind} answer does not stall the worker's other connections" do
          with_inline_server(INLINE_APP, mode: mode, workers: 1) do |_, port, server|
            reader = narrow_connection(port)
            other = TCPSocket.new("127.0.0.1", port).tap { it.timeout = WsClient::DEADLINE }
            wait_for("both inline connections") { server.stats[:inline][:connections] == 2 }
            reader.write(request(path, close: true))
            expect(reader.wait_readable(5)).to be_truthy, "no answer began"
            received = reader.readpartial(65_536)

            # 64 KiB at most every 50 ms, while the other connection's
            # requests come.
            times = Array.new(10) do
              sleep 0.05
              received << reader.readpartial(65_536)
              started = monotonic
              other.write(request("/hello"))
              expect(read_response(other).last).to eq("hello")
              monotonic - started
            end
            # A worker stalled on the slow reader would hold every round
            # past the reader's next read, so the median tells: one round
            # a busy host holds up does not.
            median = times.sort[times.size / 2]
            expect(median).to be < 0.05, "beside the slow reader, /hello took #{times.map { (it * 1000).round(1) }} ms"

            received << reader.read
            expect(response_body_of(received).bytesize).to eq(INLINE_BIG)
            expect(server.stats[:inline]).to include(requests: 11, handbacks: 0)
          ensure
            [reader, other].compact.each(&:close)
          end
        end
      end

      it "answers the client that left mid-stream with ClientGone in the app" do
        with_inline_server(leaving_app(reports.port), mode: mode, workers: 1) do |_, port, server|
          before = server.stats[:responses_cut]
          socket = TCPSocket.new("127.0.0.1", port).tap { it.timeout = WsClient::DEADLINE }
          socket.write(request("/forever"))
          expect(socket.wait_readable(5)).to be_truthy, "the stream never began"
          socket.readpartial(65_536)
          socket.close

          expect(reports.next(awaiting: "what ended the stream")).to eq("Kino::ClientGone")
          expect(get(port, "/hello")).to eq("hello")
          cut = server.stats[:responses_cut].to_h { |cause, count| [cause, count - before[cause]] }
          expect(cut).to eq(client_gone: 1, shutdown: 0, crash: 0)
          expect(server.stats[:inline]).to include(requests: 2, handbacks: 0)
        ensure
          socket&.close
        end
      end

      it "counts a streamed body hyper would drop as its client gone, as hyper does" do
        paths = %w[/long /refused]
        # /long streams past its content-length of 5, /refused gives one hyper
        # refuses; either way its last write comes 0.2 s on, once hyper would
        # have dropped the body, and it reports to `events` once that write is
        # back.
        events = reports.port
        app = Ractor.shareable_proc do |env|
          length = (env["PATH_INFO"] == "/refused") ? "x" : "5"
          body = Enumerator.new do |out|
            out << "hel" << "lo, world"
            sleep 0.2
            out << "!"
          ensure
            events << :written
          end
          [200, {"content-length" => length}, body]
        end
        seen = {"inline" => {}, "through hyper" => {_inline: false}}.transform_values do |opts|
          with_server(app, mode: mode, workers: 1, threads: 1, worker_loop: async_loop(8), **opts) do |_, port, server|
            await_inline(server, port, 1) unless opts.key?(:_inline)
            paths.map do |path|
              before = server.stats[:responses_cut][:client_gone]
              body = raw_exchange(port, request(path, close: true), wait: 0.3).split("\r\n\r\n", 2)[1]
              expect(reports.next(awaiting: "#{path}'s last write")).to eq(:written)
              [body, server.stats[:responses_cut][:client_gone] - before]
            end
          end
        end
        expect(seen["through hyper"]).to eq([["hello", 1], [nil, 1]])
        expect(seen["inline"]).to eq(seen["through hyper"])
      end

      it "reads the request body while a helper thread is blocked writing the answer" do
        with_inline_server(leaving_app(reports.port), mode: mode, workers: 1) do |_, port, server|
          client = narrow_connection(port)
          client.write("POST /duplex HTTP/1.1\r\nhost: x\r\ncontent-length: 5\r\n\r\nhello")
          expect(reports.next(awaiting: "the body's read")).to eq("hello")
          client.close

          expect(reports.next(awaiting: "what ended the helper's writes")).to eq("Kino::ClientGone")
          expect(get(port, "/hello")).to eq("hello")
          expect(server.stats[:inline]).to include(requests: 2, handbacks: 0)
        ensure
          client&.close
        end
      end

      it "ends a helper thread's write to a client that stopped reading once shutdown passes its deadline" do
        log = capture_native_stderr do
          with_inline_server(leaving_app(reports.port), mode: mode, workers: 1) do |_, port, server|
            client = narrow_connection(port)
            client.write(request("/helper"))
            expect(client.wait_readable(5) && client.gets).to eq("HTTP/1.1 200 OK\r\n")
            # The helper's writes fill what the client leaves unread.
            sleep 0.2
            expect(reports).to be_empty, "the helper's writes ended before the shutdown"

            started = monotonic
            server.shutdown(timeout: 0.2)
            expect(monotonic - started).to be < 1
            expect(reports.next(awaiting: "what ended the helper's writes")).to eq("Kino::ClientGone")
          ensure
            client&.close
          end
        end
        expect(log).not_to include("stuck")
      end

      it "serves other connections while a body is still coming" do
        with_inline_server(INLINE_APP, mode: mode, workers: 1) do |_, port, server|
          uploading, other = Array.new(2) { TCPSocket.new("127.0.0.1", port).tap { it.timeout = WsClient::DEADLINE } }
          wait_for("both inline connections") { server.stats[:inline][:connections] == 2 }
          uploading.write("POST /echo HTTP/1.1\r\nhost: x\r\ncontent-length: 10\r\n\r\nhalf ")
          sleep 0.05
          other.write(request("/hello"))
          expect(other.wait_readable(5)).to be_truthy, "the other connection waited for the upload"
          expect(read_response(other).last).to eq("hello")
          uploading.write("done.")
          expect(read_response(uploading).last).to eq("half done.")
          expect(server.stats[:inline]).to include(requests: 2, handbacks: 0)
        ensure
          [uploading, other].compact.each(&:close)
        end
      end

      it "hands a WebSocket handshake back to the native WebSocket path" do
        with_inline_server(ws_app(EchoSocket), mode: mode) do |_, port, server|
          client = connect(port)
          client.send_text("echo me")
          expect(client.read).to eq([WsClient::TEXT, "echo me"])
          expect(server.stats[:inline][:handbacks]).to eq(1)
        end
      end

      it "never runs more requests at once than its concurrency" do
        # Reports to `events` how many requests run on its worker thread as each
        # starts (a thread variable, which the thread's fibers share). /block
        # holds the thread 50 ms out of the scheduler's reach, with the GVL
        # released; then each request waits 0.2 s on its fiber.
        events = reports.port
        app = Ractor.shareable_proc do |env|
          thread = Thread.current
          thread.thread_variable_set(:spec_running, thread.thread_variable_get(:spec_running).to_i + 1)
          events << thread.thread_variable_get(:spec_running)
          Kino.sleep(0.05) if env["PATH_INFO"] == "/block"
          sleep 0.2
          [200, {}, ["done"]]
        ensure
          thread.thread_variable_set(:spec_running, thread.thread_variable_get(:spec_running) - 1)
        end
        with_inline_server(app, mode: mode, workers: 1, worker_loop: async_loop(2)) do |_, port, server|
          counts = Array.new(3) do
            first, second = Array.new(2) { TCPSocket.new("127.0.0.1", port).tap { it.timeout = WsClient::DEADLINE } }
            wait_for("two inline connections") { server.stats[:inline][:connections] == 2 }
            through_hyper = TCPSocket.new("127.0.0.1", port).tap { it.timeout = WsClient::DEADLINE }

            # While the first holds the thread, the second's request and
            # one dispatched to the worker both become ready.
            first.write(request("/block"))
            started = [reports.next(awaiting: "the blocking request")]
            second.write(request("/"))
            through_hyper.write(request("/"))
            expect([first, second, through_hyper].map { read_response(it).last }).to all(eq("done"))
            started + Array.new(2) { reports.next(awaiting: "the other requests") }
          ensure
            [first, second, through_hyper].compact.each(&:close)
            wait_for("the connections closed") { server.stats[:inline][:connections].zero? }
          end
          expect(counts.flatten.max).to eq(2)
        end
      end

      it "serves a connection's request inline once a request through its worker's lane frees the room" do
        # /block reports to `events`, then holds its worker thread 50 ms, out of
        # the scheduler's reach, with the GVL released; anything else answers
        # at once.
        events = reports.port
        app = Ractor.shareable_proc do |env|
          if env["PATH_INFO"] == "/block"
            events << :blocking
            Kino.sleep(0.05)
          end
          [200, {}, ["done"]]
        end
        with_inline_server(app, mode: mode, workers: 1, worker_loop: loop_on(LoopLastReactor, 1)) do |_, port, server|
          3.times do
            inline = TCPSocket.new("127.0.0.1", port).tap { it.timeout = WsClient::DEADLINE }
            wait_for("the inline connection") { server.stats[:inline][:connections] == 1 }
            through_hyper = TCPSocket.new("127.0.0.1", port).tap { it.timeout = WsClient::DEADLINE }

            # The inline request arrives while the other holds the only
            # slot; once that job ends, the connection's job looks before
            # the worker's loop runs again.
            through_hyper.write(request("/block"))
            reports.next(awaiting: "the request through the lane")
            inline.write(request("/"))
            expect([through_hyper, inline].map { read_response(it).last }).to all(eq("done"))
          ensure
            [inline, through_hyper].compact.each(&:close)
            wait_for("the connections closed") { server.stats[:inline][:connections].zero? }
          end
          expect(server.stats[:inline]).to include(requests: 3, handbacks: 0)
        end
      end

      it "ends a connection once the answer going out as the drain begins is out, a request its client sent meanwhile unread" do
        with_inline_server(INLINE_APP, mode: mode, workers: 1) do |_, port, server|
          client = narrow_connection(port)
          wait_for("the inline connection") { server.stats[:inline][:connections] == 1 }
          client.write(request("/big"))
          expect(client.wait_readable(5)).to be_truthy, "the answer never began"
          # Its job waits for the socket to take more, and reads none of this,
          # which over loopback reaches the server's socket well within the
          # sleep: the close finds it there.
          client.write(request("/behind"))
          sleep 0.05
          Kino::Native.stop_accepting(server_id(server))
          expect(read_response(client).last.bytesize).to eq(INLINE_BIG)
          expect(client.read).to eq(""), "reset, or left open"
        ensure
          client&.close
        end
      end

      it "closes a connection still sending a body once shutdown passes its deadline" do
        log = capture_native_stderr do
          with_inline_server(INLINE_APP, mode: mode, workers: 1) do |_, port, server|
            client = TCPSocket.new("127.0.0.1", port).tap { it.timeout = WsClient::DEADLINE }
            wait_for("the inline connection") { server.stats[:inline][:connections] == 1 }
            client.write("POST /echo HTTP/1.1\r\nhost: x\r\ncontent-length: 8\r\n\r\nhalf")
            # Its job reads the head and half the body: the drain then no
            # longer finds it idle.
            sleep 0.1

            started = monotonic
            server.shutdown(timeout: 0.2)
            expect(monotonic - started).to be < 1
            # Closed, with no answer: the request never ran.
            expect(client.wait_readable(1) && client.read).to eq("")
          ensure
            client&.close
          end
        end
        expect(log).not_to include("stuck")
      end

      it "places no connection on a retired worker and lets it leave once its connections are gone" do
        exits = Thread::Queue.new
        on_exit = ->(_worker_id, error) { exits << error }
        with_inline_server(INLINE_APP, mode: mode, workers: 2, max_workers: 3, on_worker_exit: on_exit) do |_, port, server|
          inline = -> { server.stats[:worker_status].map { it[:inline_connections] } }
          sockets = Array.new(2) { TCPSocket.new("127.0.0.1", port).tap { it.timeout = WsClient::DEADLINE } }
          sockets.each do |socket|
            socket.write(request("/hello"))
            expect(read_response(socket).last).to eq("hello")
          end
          expect(inline.call).to eq([1, 1])

          retire_slot(server, 1)
          wait_for("the retired worker to leave", timeout: 1) { exits.size == 1 }
          expect(exits.pop).to be_nil
          expect(server.stats[:inline][:handbacks]).to eq(1)
          # Its client kept the connection, hyper's from then on.
          sockets.each do |socket|
            socket.write(request("/hello"))
            expect(read_response(socket).last).to eq("hello")
          end
          more = Array.new(3) { TCPSocket.new("127.0.0.1", port).tap { it.timeout = WsClient::DEADLINE } }
          wait_for("the new connections placed") { inline.call.sum == 4 }
          expect(inline.call).to eq([4, 0])
          more.each do |socket|
            socket.write(request("/hello"))
            expect(read_response(socket).last).to eq("hello")
          end
        ensure
          [*sockets, *more].compact.each(&:close)
        end
      end

      # A worker killed: worker_crash_spec.
      it "answers the request already sent on an idle inline connection of a worker an exception crashed, through hyper" do
        capture_native_stderr do
          with_inline_server(ENDING_APP, mode: mode, workers: 1, worker_loop: loop_on(Async::Reactor, 8)) do |_, port, server|
            kept = TCPSocket.new("127.0.0.1", port).tap { it.timeout = WsClient::DEADLINE }
            kept.write(request("/hello"))
            expect(read_response(kept).last).to eq("hello")
            dying = TCPSocket.new("127.0.0.1", port).tap { it.timeout = WsClient::DEADLINE }
            wait_for("both inline connections") { server.stats[:inline][:connections] == 2 }
            dying.write(request("/stall"))
            # The worker's thread is held now: the next request on the
            # idle connection waits unread as the worker goes down.
            wait_for("the stalling request running") { server.stats[:in_flight] == 1 }
            kept.write(request("/hello"))

            expect(dying.wait_readable(5) && read_response(dying).first).to eq("HTTP/1.1 500 Internal Server Error")
            expect(kept.wait_readable(5) && read_response(kept).last).to eq("hello"), "the request sent was lost"
            kept.write(request("/hello"))
            expect(kept.wait_readable(2) && read_response(kept).last).to eq("hello"), "its client lost the connection"
            expect(server.stats[:inline]).to include(closed: 1, handbacks: 1)
          ensure
            [kept, dying].compact.each(&:close)
          end
        end
      end

      it "answers 500 when an exception that is no StandardError ends an inline request" do
        capture_native_stderr do
          with_inline_server(ENDING_APP, mode: mode) do |_, port, server|
            %w[/fatal /stop /kill].each do |path|
              expect(timed { expect(raw_exchange(port, request(path))).to start_with("HTTP/1.1 500 ") }).to be < 1
              wait_for("serving after #{path}") { serves_hello?(port) }
              # No new connection waits on a worker that is gone, however
              # many open at once.
              clients = Array.new(4) { TCPSocket.new("127.0.0.1", port).tap { it.timeout = WsClient::DEADLINE } }
              clients.each { it.write(request("/hello")) }
              expect(clients.map { it.wait_readable(2) && read_response(it).last }).to all(eq("hello"))
            ensure
              clients&.each(&:close)
            end
          end
        end
      end

      it "closes the socket an inline request's writes waited on once its connection is over" do
        # /waits streams 16 MiB from a Rack 3 streaming body, 64 KiB a write,
        # and reports to `events` how often those writes waited for the socket to
        # take more (under a WritableWaitsReactor). Anything else is
        # INLINE_APP's.
        events = reports.port
        app = Ractor.shareable_proc do |env|
          next INLINE_APP.call(env) unless env["PATH_INFO"] == "/waits"

          chunk = "x" * 65_536
          body = lambda do |stream|
            before = WritableWaitsReactor.count
            256.times { stream.write(chunk) }
            events << WritableWaitsReactor.count - before
          ensure
            stream.close
          end
          [200, {}, body]
        end
        with_inline_server(app, mode: mode, workers: 1,
          worker_loop: loop_on(WritableWaitsReactor, 8)) do |_, port|
          # Each client reads only 0.2 s on, once the writes have filled
          # what both sockets take, more than Linux's loopback buffers hold
          # too, so they wait.
          exchange = lambda do
            client = narrow_connection(port)
            client.write(request("/waits", close: true))
            sleep 0.2
            expect(read_response(client).last.bytesize).to eq(16 << 20)
            expect(client.read).to eq("")
            reports.next(awaiting: "the stream's waits")
          ensure
            client&.close
          end
          # The first opens whatever the server opens once.
          expect(exchange.call).to be_positive, "no write waited on its socket"
          baseline = open_fds

          GC.disable
          begin
            waits = Array.new(3) { exchange.call }
            expect(waits).to all(be_positive), "an exchange whose writes never waited, of #{waits}"
            wait_for("the sockets the writes waited on closed (#{open_fds - baseline} open)", timeout: 2) do
              open_fds <= baseline
            end
          ensure
            GC.enable
          end
        end
      end

      it "keeps the descriptor a helper's write waits on until that wait is back, its request over" do
        with_inline_server(leaving_app(reports.port), mode: mode, workers: 1) do |_, port, server|
          client = narrow_connection(port)
          client.write(request("/outlive", close: true))
          expect(client.wait_readable(5) && client.gets).to eq("HTTP/1.1 200 OK\r\n")
          # The body returned; the helper's write is about to wait.
          sleep 0.15
          client.setsockopt(Socket::SOL_SOCKET, Socket::SO_LINGER, [1, 0].pack("ii"))
          client.close

          # Its connection ended under it; the stream it wrote to ended
          # first.
          expect(reports.next(awaiting: "what ended the helper's writes")).to eq("RuntimeError")
          expect(get(port, "/hello")).to eq("hello")
          expect(server.stats[:inline]).to include(requests: 2, handbacks: 0)
        end
      end

      # A unix socket's writes, which take less at once than TCP's.
      context "over a unix socket" do
        it "writes an answer larger than the socket takes at once, buffered or streamed" do
          with_unix_inline_server(INLINE_APP, mode: mode) do |path, server|
            UNIXSocket.open(path) do |socket|
              socket.write(request("/big"))
              expect(read_response(socket).last.bytesize).to eq(INLINE_BIG)
              socket.write(request("/big?stream"))
              expect(read_response(socket).last.bytesize).to eq(INLINE_BIG)
            end
            expect(server.stats[:inline]).to include(requests: 2, handbacks: 0, closed: 0)
          end
        end
      end

      it "a TLS worker_loop server serves through hyper" do
        tls = {cert: TlsFixture.cert_pem, key: TlsFixture.key_pem}
        with_server(INLINE_APP, mode: mode, worker_loop: async_loop(8), tls: tls) do |host, port, server|
          http = Net::HTTP.new(host, port)
          http.use_ssl = true
          http.verify_mode = OpenSSL::SSL::VERIFY_NONE
          expect(http.start { |session| session.get("/hello").body }).to eq("hello")
          expect(server.stats[:inline]).to be_nil
        end
      end

      it "serves nothing inline without a worker_loop" do
        with_server(INLINE_APP, mode: mode) do |_, port, server|
          expect(get(port, "/hello")).to eq("hello")
          expect(server.stats[:inline]).to be_nil
        end
      end
    end
  end
end
