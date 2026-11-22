# frozen_string_literal: true

require "async"
require "socket"

# An endless event stream: an event every 10 ms until a write fails.
# Reports what ended it, as an app rescuing IOError sees it, and its
# close.
EndlessEvents = Data.define(:port) do
  def each
    events = 0
    loop do
      yield "data: #{events += 1}\n\n"
      sleep 0.01
    end
  rescue IOError => e
    port << [:ended, e.class]
    raise
  end

  def close = port << :closed
end

# Writes 16 MiB chunks, far more than socket buffers hold, and reports
# each, endlessly.
Flood = Data.define(:port) do
  def each
    chunk = ("x" * (16 << 20)).freeze
    written = 0
    loop do
      yield chunk
      port << [:written, written += 1]
    end
  end
end

# Writes 16 MiB chunks through the block from a helper thread until a
# write fails, and reports how it failed; once the helper's writes
# stall, the body itself raises.
HelperFlood = Data.define(:port) do
  def each(&write)
    chunk = ("x" * (16 << 20)).freeze
    progress = Thread::Queue.new
    Thread.new do
      loop do
        write.call(chunk)
        progress << :written
      end
    rescue => e
      port << [:helper, e.class, e.is_a?(IOError)]
    end
    nil while progress.pop(timeout: 0.3)
    raise "gave up on the stalled client"
  end
end

# `body`'s chunks as a Rack 3 streaming body writes them, to the stream.
StreamOf = Data.define(:body) do
  def call(stream) = body.each { |chunk| stream << chunk }
end

# What a proxy streams back while a helper thread still waits for the
# request body: its first chunk at once, then what `reader` read.
RelayedBody = Data.define(:reader) do
  def each
    yield "streaming; "
    yield "read: #{reader.value}"
  end
end

# Full duplex: reads the request body from the stream while a helper
# thread is blocked writing to a client that reads nothing; reports what
# it read, then what ended the helper's writes.
DuplexStream = Data.define(:port) do
  def call(stream)
    chunk = ("x" * 65_536).freeze
    writer = Thread.new do
      loop { stream.write(chunk) }
    rescue IOError => e
      port << [:writer, e.class]
    end
    port << [:read, stream.read(5)]
    writer.join
  end
end

# A streaming body's client can leave, or stop reading, at any time:
# the body's next write raises, so it ends and frees its worker; and
# cutting a stream short never waits for its client to read. The same
# under a worker_loop, whose writes wait as fibers instead of blocking.
RSpec.describe "streaming bodies" do
  include_context "native WebSocket server"

  # Each body at its path; the same body as a Rack 3 streaming body at
  # its path with "3" after it.
  def streaming_app(port)
    Ractor.shareable_proc do |env|
      path = env["PATH_INFO"]
      body = case path.delete_suffix("3")
      when "/events" then EndlessEvents.new(port)
      when "/flood" then Flood.new(port)
      when "/helper" then HelperFlood.new(port)
      when "/relay"
        input = env["rack.input"]
        reader = Thread.new { input.read }
        Thread.pass until reader.stop? # blocked reading, as the client sends nothing yet
        RelayedBody.new(reader)
      when "/duplex" then DuplexStream.new(port)
      when "/late"
        Kino.sleep(0.5) # past the request_timeout a spec sets, so a 504 went out
        EndlessEvents.new(port)
      else ["ok"]
      end
      [200, {"content-type" => "text/event-stream"}, path.end_with?("3") ? StreamOf.new(body) : body]
    end
  end

  # Every completed request and every on_error, reported.
  def with_streaming_server(mode, **options, &block)
    port = reports.port
    hooks = {
      after_request_complete: Ractor.shareable_proc { |env, status| port << [:complete, env["PATH_INFO"], status] },
      on_error: Ractor.shareable_proc { |error, _env| port << [:error, error.class] }
    }
    with_server(streaming_app(port), mode: mode, workers: 1, threads: 1, **hooks, **server_defaults, **options,
      &block)
  end

  def request(path, method = "GET", headers = "") = "#{method} #{path} HTTP/1.1\r\nHost: kino.test\r\n#{headers}\r\n"

  # Asks for `path`, then never reads; a small receive buffer fills fast.
  def stalled_client(port, path, method = "GET", headers = "")
    narrow_connection(port, receive_buffer: 4096).tap { |socket| socket.write(request(path, method, headers)) }
  end

  # Reports up to and including the request's completion, or its error.
  def reports_until_done
    collected = [reports.next(awaiting: "the body's end")]
    collected << reports.next(awaiting: "after_request_complete") until collected.last in [:complete | :error, *]
    collected
  end

  def serves_next_request(port)
    Net::HTTP.start("127.0.0.1", port, read_timeout: 3) { |http| http.get("/").body }
  end

  # Reads from `socket` into `received` until it includes `text`; fails
  # the spec if that takes past 5 s or the connection ends first.
  def read_until(socket, received, text)
    until received.include?(text)
      raise "no #{text.inspect} within 5s; got #{received.inspect}" unless socket.wait_readable(5)

      received << socket.readpartial(4096)
    end
  rescue EOFError
    raise "the connection ended before #{text.inspect}; got #{received.inspect}"
  end

  each_mode do |mode|
    # A Rack 3 streaming body has no close of its own.
    {"/events" => [:closed], "/events3" => []}.each do |path, closed|
      it_in_both_loops "ends an endless #{path} body once its client hangs up, quietly, counts it cut, and frees its worker" do
        log = capture_native_stderr do
          with_streaming_server(mode) do |_host, port, server|
            cut = server.stats[:responses_cut]
            TCPSocket.open("127.0.0.1", port) do |socket|
              socket.write(request(path))
              read_until(socket, +"", "data: 1\n")
            end
            left = monotonic

            expect(reports_until_done).to eq([[:ended, Kino::ClientGone], *closed, [:complete, path, 200]])
            expect(monotonic - left).to be < 1, "the body outlived its client by #{(monotonic - left).round(2)}s"
            expect(server.stats[:responses_cut].to_h { |cause, count| [cause, count - cut[cause]] })
              .to eq(client_gone: 1, shutdown: 0, crash: 0)
            expect(serves_next_request(port)).to eq("ok")
          end
        end
        expect(log).not_to include("ClientGone"), "a client hanging up is no error: #{log}"
      end
    end

    # Kino answered 504 before the /late app started its stream. Either
    # way the body's first write ends it, quietly, and no stream was cut.
    it_in_both_loops "ends an endless body at once when its response carries none, a HEAD's or one a 504 answered" do
      with_streaming_server(mode, request_timeout: 0.2) do |host, port, server|
        cut = server.stats[:responses_cut]
        Net::HTTP.start(host, port, read_timeout: 3) do |http|
          {head: "/events", get: "/late"}.each do |method, path|
            expect(http.public_send(method, path).code).to eq((path == "/late") ? "504" : "200")
            expect(reports_until_done).to eq([[:ended, Kino::ClientGone], :closed, [:complete, path, 200]])
          end
        end
        expect(server.stats[:responses_cut]).to eq(cut)
      end
    end

    # The helper is blocked in a write when Kino cuts the stream for the
    # body's error: its write fails rather than wait on the client for
    # good.
    %w[/helper /helper3].each do |path|
      it_in_both_loops "keeps serving after a #{path} body raises while its helper thread is blocked writing" do
        client = nil
        log = capture_native_stderr do
          with_streaming_server(mode) do |_host, port, server|
            client = stalled_client(port, path)
            failures = Array.new(2) { reports.next(awaiting: "on_error and the helper's failed write") }

            expect(failures).to contain_exactly([:error, RuntimeError], [:helper, Kino::ClientGone, true])
            expect(serves_next_request(port)).to eq("ok")
            expect(server.stats[:respawns]).to eq(0)
          ensure
            client&.close
          end
        end
        expect(log).to include("RuntimeError: gave up on the stalled client")
      end
    end

    it_in_both_loops "shuts down by its deadline past a streaming client that stopped reading" do
      server = Kino::Server.new(streaming_app(reports.port), mode: mode, workers: 1, threads: 1, **server_defaults).start
      client = stalled_client(server.port, "/flood")
      # The flood's writes stopped going out: none reported for 0.3 s.
      reports.next(awaiting: "the flood's first write")
      quiet_since = monotonic
      while monotonic - quiet_since < 0.3
        next sleep(0.02) if reports.empty?

        reports.next
        quiet_since = monotonic
      end

      elapsed = timed { capture_native_stderr { server.shutdown(timeout: 0.5) } }

      expect(elapsed).to be < 2.5, "shutdown must return by its deadline, took #{elapsed.round(2)}s"
    ensure
      client&.close
      server&.shutdown
    end

    # The response head and first chunk go out (with the access log
    # timing the app call) while the helper waits for the body; the client
    # sends the body only once it has them, and the helper's read ends the
    # stream. Chunked, as a body short enough to read whole before the app
    # runs (inline) never waits.
    it_in_both_loops "streams a response while a helper thread is blocked reading the request body" do
      received = +""
      log = capture_native_stderr do
        capture_native_stdout do
          with_streaming_server(mode, log_requests: true) do |_host, port, server|
            TCPSocket.open("127.0.0.1", port) do |client|
              client.write("POST /relay HTTP/1.1\r\nHost: kino.test\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n")
              read_until(client, received, "streaming; ")
              client.write("5\r\nhello\r\n0\r\n\r\n")
              read_until(client, received, "read: hello")
            end

            expect(received).to start_with("HTTP/1.1 200 OK\r\n")
            expect(reports_until_done).to eq([[:complete, "/relay", 200]])
            expect(server.stats[:respawns]).to eq(0)
          end
        end
      end
      expect(log).to be_empty
    end

    # The body's read waits for the client, then gets what it sent, while
    # the helper stays blocked on the client that reads nothing; the
    # helper's write fails once the client leaves.
    it_in_both_loops "reads the request body while a helper thread is blocked writing the response" do
      client = nil
      with_streaming_server(mode) do |_host, port, server|
        client = stalled_client(port, "/duplex", "POST", "transfer-encoding: chunked\r\n")
        sleep 0.3 # the helper's writes fill the client's buffers
        client.write("5\r\nhello\r\n0\r\n\r\n")

        expect(reports.next(awaiting: "the body's read")).to eq([:read, "hello"])
        client.close
        expect(reports_until_done).to eq([[:writer, Kino::ClientGone], [:complete, "/duplex", 200]])
        expect(serves_next_request(port)).to eq("ok")
        expect(server.stats[:respawns]).to eq(0)
      ensure
        client&.close
      end
    end
  end
end
