# frozen_string_literal: true

require "digest/sha2"
require "httpx"
require "net/http"
require "rack/lint"

RSpec.describe "rack.hijack" do
  include_context "native WebSocket server"

  # Hijack needs no lanes: two single-thread ractors without them, unless
  # a spec says otherwise or the example runs under a worker_loop, which
  # needs them.
  def with_hijack_server(app = HijackedEcho, **opts, &block)
    with_ws_server(app, lanes: server_defaults.key?(:worker_loop), **opts, &block)
  end

  # Both worker modes, for the examples that run in each.
  def worker_modes = [{mode: :threaded}, {mode: :ractor}]

  # A GET through Net::HTTP, over TLS when `tls`: the whole response.
  def http_get(port, path, tls: false)
    tls ? https_get("127.0.0.1", port, path) : Net::HTTP.get_response("127.0.0.1", path, port)
  end

  # Everything the server sends until it closes the connection; fails the
  # spec if it is still open after `timeout` seconds.
  def read_until_closed(socket, timeout: WsClient::DEADLINE)
    deadline = monotonic + timeout
    received = +"".b
    loop do
      case (chunk = socket.read_nonblock(65_536, exception: false))
      when nil then return received
      when :wait_readable
        remaining = deadline - monotonic
        unless remaining.positive? && IO.select([socket], nil, nil, remaining)
          raise "the connection was still open after #{timeout}s; got #{received.inspect}"
        end
      else received << chunk
      end
    end
  end

  # Everything the server answers `request` with, sent on a connection of
  # its own, until it closes the connection.
  def exchange(port, request)
    socket = TCPSocket.new("127.0.0.1", port)
    socket.write(request)
    read_until_closed(socket)
  ensure
    socket&.close
  end

  # The headers that make a raw request one that asks to upgrade, the only
  # kind that is offered rack.hijack.
  def upgrade_headers = "Upgrade: chat\r\nConnection: Upgrade\r\n"

  # An Upgrade request through Net::HTTP, over TLS when `tls`; hyper hands
  # its connection over only if the app answers 101.
  def get_upgrade(port, path = "/", tls: false)
    http = Net::HTTP.new("127.0.0.1", port)
    http.use_ssl = tls
    http.verify_mode = OpenSSL::SSL::VERIFY_NONE
    http.start { |session| session.get(path, "Upgrade" => "websocket", "Connection" => "Upgrade") }
  end

  # The app's triple is ignored once it hijacked (Rack 3), but its body
  # still closes.
  %i[threaded ractor].product([false, true]).each do |mode, lanes|
    it "lets an app that writes its own 101 talk WebSocket through the hijacked IO, ignoring its triple " \
      "(#{mode}, lanes #{lanes})" do
      reported = reports.port
      app = Ractor.shareable_proc do |env|
        HijackedEcho.call(env)
        [200, {"x-ignored" => "yes"}, ReportedBody.new(reported, false)]
      end
      log = capture_native_stderr do
        with_hijack_server(app, mode:, lanes:) do |port|
          client = connect(port)
          # The last one crosses the 64 KiB length boundary and any one read.
          sent = [[WsClient::TEXT, "héllo"], [WsClient::BINARY, "\x00\xff".b], [WsClient::TEXT, "x" * 70_000]]

          sent.each { |opcode, payload| client.send_frame(opcode, payload) }

          expect(Array.new(3) { client.read }).to eq(sent)
          expect(client.close).to eq(1000)
          expect(client.headers).not_to include("x-ignored")
          expect(reports.next(awaiting: "the body's close")).to eq(:body_closed)
        end
      end

      expect(log).to be_empty
    end
  end

  [:partial_hijack, :streaming_body].each do |kind|
    it "a 101 with a #{kind} hands the callable the switched connection" do
      # Answers a WebSocket handshake with a 101 and, as `kind`, a callable
      # that echoes one frame and closes the connection.
      app = Ractor.shareable_proc do |env|
        accept = [Digest::SHA1.digest(env["HTTP_SEC_WEBSOCKET_KEY"] + WsClient::GUID)].pack("m0")
        headers = {"upgrade" => "websocket", "connection" => "Upgrade", "sec-websocket-accept" => accept}
        echo = lambda do |io|
          opcode, payload = HijackedEcho.read_frame(io)
          io.write(WsClient.frame(opcode, payload, mask: false))
          io.close
        end
        (kind == :partial_hijack) ? [101, headers.merge("rack.hijack" => echo), []] : [101, headers, echo]
      end
      log = capture_native_stderr do
        [false, true].each do |lanes|
          with_hijack_server(app, lanes: lanes) do |port|
            client = connect(port)

            client.send_text("echoed by the #{kind}")

            echoed = client.read
            expect(echoed).to eq([WsClient::TEXT, "echoed by the #{kind}"]), "lanes #{lanes}: got #{echoed.inspect}"
            expect(client.read).to be_nil, "lanes #{lanes}: the callable's close must end the connection"
          end
        end
      end

      expect(log).to be_empty
    end
  end

  it "fires after_request_complete once a handshake's 101 is out and after a partial hijack, not after a full hijack" do
    completed = reports.port
    app = Ractor.shareable_proc do |env|
      case env["PATH_INFO"]
      when "/native"
        env["rack.upgrade"] = EchoSocket
        [101, {}, []]
      when "/partial" then [200, {"rack.hijack" => ->(io) { io.close }}, []]
      when "/full" then HijackedEcho.call(env)
      else [200, {}, ["plain"]]
      end
    end
    hook = Ractor.shareable_proc { |env, status| completed << [env["PATH_INFO"], status] }

    worker_modes.each do |mode|
      with_hijack_server(app, lanes: true, workers: 1, threads: 1, after_request_complete: hook, **mode) do |port|
        connect(port, path: "/native")
        get_upgrade(port, "/partial")
        connect(port, path: "/full")
        Net::HTTP.get("127.0.0.1", "/plain", port)

        fired = Array.new(3) { reports.next(awaiting: "after_request_complete") }
        expect(fired).to eq([["/native", 101], ["/partial", 200], ["/plain", 200]]), mode.to_s
      end
    end
  end

  it "full and partial hijack pass Rack::Lint, and a partial hijack's head goes out without its rack.hijack header" do
    app = Rack::Lint.new(lambda do |env|
      if env["PATH_INFO"] == "/partial"
        [200, {"content-type" => "text/plain", "rack.hijack" => ->(io) {
          io.write("partial")
          io.close
        }}, []]
      else
        HijackedEcho.call(env)
        [200, {}, []]
      end
    end)

    log = capture_native_stderr do
      with_hijack_server(app, mode: :threaded) do |port|
        client = connect(port)
        client.send_text("linted")
        expect(client.read).to eq([WsClient::TEXT, "linted"])
        response = get_upgrade(port, "/partial")
        expect(response.body).to eq("partial")
        expect(response["content-type"]).to eq("text/plain")
        expect(response.to_hash).not_to include("rack.hijack")
      end
    end

    expect(log).to be_empty
  end

  it "only HTTP/1.1 requests that ask to upgrade offer rack.hijack" do
    app = Ractor.shareable_proc do |env|
      [200, {"content-type" => "text/plain"}, [env.slice("rack.hijack?", "rack.hijack").keys.join(",")]]
    end
    worker_modes.each do |mode|
      with_hijack_server(app, **mode) do |port|
        upgrade = get_upgrade(port).body
        http11 = http_get(port, "/").body
        http10 = exchange(port, "GET / HTTP/1.0\r\n#{upgrade_headers}\r\n").split("\r\n\r\n", 2).last
        h2 = HTTPX.with(fallback_protocol: "h2").get("http://127.0.0.1:#{port}/")

        expect(upgrade).to eq("rack.hijack?,rack.hijack"), "#{mode}: an Upgrade request"
        expect(http11).to eq(""), "#{mode}: HTTP/1.1 without an Upgrade header"
        expect(http10).to eq(""), "#{mode}: HTTP/1.0"
        expect(h2.version).to eq("2.0")
        expect(h2.body.to_s).to eq(""), "#{mode}: HTTP/2 has no connection to hand over"
      end
    end
  end

  it "a hijack that answers without switching frees its worker, and is answered later from another thread" do
    # A request at /poll takes the connection over and returns at once,
    # which frees its worker, and a thread of the app's answers it once
    # /publish?message brings news. The news queue is the worker thread's,
    # so this works in a worker ractor; `reported` hears :hijacked once a
    # poll is held.
    reported = reports.port
    app = Ractor.shareable_proc do |env|
      worker = Thread.current
      news = worker.thread_variable_get(:news) || worker.thread_variable_set(:news, Thread::Queue.new)
      if env["PATH_INFO"] == "/poll"
        io = env["rack.hijack"].call
        Thread.new do
          body = %([{"data":"#{news.pop}"}])
          io.write("HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: #{body.bytesize}\r\n\r\n#{body}")
          io.close
        end
        reported << :hijacked
        [418, {}, ["I'm a teapot, undefined in spec"]]
      else
        news << env["QUERY_STRING"]
        [200, {"content-type" => "text/plain"}, ["published"]]
      end
    end
    worker_modes.product([false, true]).each do |mode, tls|
      context = "#{mode}, tls #{tls}"
      with_hijack_server(app, workers: 1, threads: 1, **mode, **(tls ? TlsFixture.options : {})) do |port|
        poll = Thread.new { get_upgrade(port, "/poll", tls:) }
        expect(reports.next(awaiting: "the poll's hijack")).to eq(:hijacked)

        expect(http_get(port, "/publish?hello", tls:).body).to eq("published"), "#{context}: the only worker is free"
        response = poll.value

        expect(response.code).to eq("200"), context
        expect(response.to_hash).to include("content-type" => ["application/json"], "connection" => ["close"])
        expect(response.body).to eq(%([{"data":"hello"}])), context
      end
    end
  end

  it "passes on an answer with a Content-Length, as a rejected handshake's 400, and answers 502 to any other framing" do
    app = Ractor.shareable_proc do |env|
      io = env["rack.hijack"].call
      io.write({
        "/rejected" => "HTTP/1.1 400 Bad Request\r\ncontent-type: text/plain\r\ncontent-length: 11\r\n\r\nBad Request",
        "/chunked" => "HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n",
        "/until-close" => "HTTP/1.1 200 OK\r\n\r\nhello"
      }.fetch(env["PATH_INFO"]))
      io.close
      [-1, {}, []]
    end
    worker_modes.each do |opts|
      with_hijack_server(app, **opts) do |port|
        rejected = get_upgrade(port, "/rejected")
        expect([rejected.code, rejected.body, rejected["content-type"], rejected["connection"]])
          .to eq(["400", "Bad Request", "text/plain", "close"]), "with #{opts}"

        %w[/chunked /until-close].each do |path|
          refused = get_upgrade(port, path)
          expect([refused.code, refused["connection"]]).to eq(%w[502 close]), "#{path} with #{opts}"
        end
      end
    end
  end

  it "a hijacked connection answered without switching is the app's: a request pipelined behind it goes unanswered" do
    app = Ractor.shareable_proc do |env|
      next [200, {"content-type" => "text/plain"}, ["plain"]] unless env["PATH_INFO"] == "/hijacked"

      io = env["rack.hijack"].call
      io.write("HTTP/1.1 200 OK\r\ncontent-length: 8\r\n\r\nhijacked")
      io.close
      [-1, {}, []]
    end
    worker_modes.each do |opts|
      with_hijack_server(app, **opts) do |port|
        response = exchange(port, "GET /hijacked HTTP/1.1\r\nHost: kino.test\r\n#{upgrade_headers}\r\n" \
          "GET /plain HTTP/1.1\r\nHost: kino.test\r\n\r\n")

        expect(response.scan("HTTP/1.1 ").size).to eq(1), "with #{opts}: got #{response.inspect}"
        expect(response).to end_with("\r\n\r\nhijacked"), "with #{opts}"
      end
    end
  end

  it "a hijacked POST still reads its body from rack.input, bytes sent after the hijack included" do
    reported = reports.port
    app = Ractor.shareable_proc do |env|
      io = env["rack.hijack"].call
      reported << :hijacked
      received = env["rack.input"].read
      answer = "#{received.bytesize} #{Digest::SHA256.hexdigest(received)}"
      io.write("HTTP/1.1 200 OK\r\ncontent-length: #{answer.bytesize}\r\n\r\n#{answer}")
      io.close
      [-1, {}, []]
    end
    # More than the request body channel holds.
    body = (1..40_000).to_a.join(",")
    worker_modes.each do |mode|
      with_hijack_server(app, **mode) do |port|
        socket = TCPSocket.new("127.0.0.1", port)
        socket.write("POST /upload HTTP/1.1\r\nHost: kino.test\r\n#{upgrade_headers}" \
          "content-length: #{body.bytesize}\r\n\r\n#{body[0, 100]}")
        expect(reports.next(awaiting: "the POST's hijack")).to eq(:hijacked)
        socket.write(body[100..])

        head, echoed = read_until_closed(socket).split("\r\n\r\n", 2)

        expect(head).to start_with("HTTP/1.1 200 OK\r\n"), mode.to_s
        expect(echoed).to eq("#{body.bytesize} #{Digest::SHA256.hexdigest(body)}"), mode.to_s
      ensure
        socket&.close
      end
    end
  end

  # A /poll is a long poll with no news: its worker thread keeps the
  # socket. Any other request is answered half a second later, while its
  # app is still on its worker.
  it_in_both_loops "a drain lets an app that hijacked and is still on its worker answer, and answers 503 to the rest " \
    "without waiting for them" do
    reported = reports.port
    app = Ractor.shareable_proc do |env|
      io = env["rack.hijack"].call
      reported << :hijacked
      if env["PATH_INFO"] == "/poll"
        worker = Thread.current
        (worker.thread_variable_get(:held) || worker.thread_variable_set(:held, [])) << io
      else
        sleep 0.5
        io.write("HTTP/1.1 200 OK\r\ncontent-length: 8\r\n\r\nanswered")
        io.close
      end
      [-1, {}, []]
    end
    requests = ["GET /poll HTTP/1.1\r\nHost: kino.test\r\n#{upgrade_headers}\r\n"] * 2 +
      ["GET / HTTP/1.1\r\nHost: kino.test\r\n#{upgrade_headers}\r\n"] * 2
    # The last variant runs a GC as each worker ends. That collects what
    # a worker Ractor's app held, the polls' IOs among them, as Ruby 4.1
    # does as the Ractor ends, so their ends close before the drain
    # closes the bridges. (The threaded pool keeps its threads, and what
    # they held, until the shutdown is over.)
    variants = {
      "alone" => {},
      "with io_shards" => {io_shards: true},
      "with a GC as each worker ends" => {on_worker_exit: ->(*) { GC.start }}
    }
    worker_modes.product(variants.to_a).each do |mode, (variant, settings)|
      run = "#{mode} #{variant}"
      with_hijack_server(app, **mode, **settings) do |port, server|
        sockets = requests.map { |request| TCPSocket.new("127.0.0.1", port).tap { |socket| socket.write(request) } }
        requests.size.times { expect(reports.next(awaiting: "the hijacks")).to eq(:hijacked) }

        elapsed = timed { server.shutdown(timeout: 10) }

        expect(elapsed).to be < 3, "#{run}: shutdown waited toward its 10s deadline"
        polls, answered = sockets.map { |socket| read_until_closed(socket, timeout: 1) }.each_slice(2).to_a
        expect(polls).to all(start_with("HTTP/1.1 503 ")), "#{run}: #{polls}"
        expect(answered).to all(start_with("HTTP/1.1 200 OK\r\n").and(end_with("\r\n\r\nanswered"))), "#{run}: #{answered}"
      ensure
        sockets&.each(&:close)
      end
    end
  end

  it_in_both_loops "a drain closes a connection that switched protocols once requests drain, ending an app serving it on its worker" do
    reported = reports.port
    worker_modes.product(%i[full_hijack partial_hijack streaming_body]).each do |mode, kind|
      # Switches a WebSocket handshake to the app's own protocol, as `kind`,
      # and serves the connection on its worker: reads until the connection
      # ends, reporting :switched before and :ended after.
      app = Ractor.shareable_proc do |env|
        accept = [Digest::SHA1.digest(env["HTTP_SEC_WEBSOCKET_KEY"] + WsClient::GUID)].pack("m0")
        headers = {"upgrade" => "websocket", "connection" => "Upgrade", "sec-websocket-accept" => accept}
        serve = lambda do |io|
          reported << :switched
          io.read
        rescue IOError, SystemCallError
          nil
        ensure
          reported << :ended
          io.close
        end
        case kind
        when :full_hijack
          io = env["rack.hijack"].call
          io.write("HTTP/1.1 101 Switching Protocols\r\n#{headers.map { |name, value| "#{name}: #{value}\r\n" }.join}\r\n")
          serve.call(io)
          [-1, {}, []]
        when :partial_hijack then [101, headers.merge("rack.hijack" => serve), []]
        else [101, headers, serve]
        end
      end
      with_hijack_server(app, **mode) do |port, server|
        connect(port)
        expect(reports.next(awaiting: "the switch")).to eq(:switched)

        started = monotonic
        server.shutdown(timeout: 10)

        expect(monotonic - started).to be < 3, "#{mode}, #{kind}: shutdown waited toward its 10s deadline"
        expect(reports.next(awaiting: "the app's read to end")).to eq(:ended)
      end
    end
  end

  it_in_both_loops "refuses rack.hijack, crashing nothing, while another of the app's threads waits on the request body" do
    reported = reports.port
    app = Ractor.shareable_proc do |env|
      input = env["rack.input"]
      reader = Thread.new { input.read }
      Thread.pass until reader.stop? # blocked reading, as the client sends nothing yet
      begin
        env["rack.hijack"].call
      rescue IOError => e
        reported << e.message
      end
      [409, {"content-type" => "text/plain"}, ["not hijacked"]]
    end
    worker_modes.each do |mode|
      with_hijack_server(app, **mode) do |port, server|
        socket = TCPSocket.new("127.0.0.1", port)
        socket.write("POST / HTTP/1.1\r\nHost: kino.test\r\n#{upgrade_headers}content-length: 5\r\n\r\n")

        expect(reports.next(awaiting: "the refused hijack")).to include("another of the app's threads is waiting"), mode.to_s
        socket.write("hello")
        # An upgrade request answered plainly keeps its connection open.
        answer = +""
        deadline = monotonic + WsClient::DEADLINE
        until answer.end_with?("not hijacked")
          unless IO.select([socket], nil, nil, [deadline - monotonic, 0].max)
            raise "#{mode}: no whole answer within #{WsClient::DEADLINE}s; got #{answer.inspect}"
          end
          answer << socket.readpartial(1024)
        end
        expect(answer).to start_with("HTTP/1.1 409 "), mode.to_s
        expect(server.stats[:respawns]).to eq(0), mode.to_s
      ensure
        socket&.close
      end
    end
  end

  each_mode do |mode|
    # A hijack that opened after the scaler's last look meets the retirement.
    it "has a retired worker that holds a hijacked connection take nothing new, and leave once it closes" do
      exits = Thread::Queue.new
      with_hijack_server(HijackedEcho, mode: mode, workers: 1, threads: 1,
        on_worker_exit: ->(_worker, error) { exits << [:exit, error] }) do |port, server|
        client = connect(port)
        served = server.stats[:served]

        retire_slot(server, 0)
        # A worker waiting on the shared queue looks again within a 50 ms tick,
        # or a few on a loaded machine; until it has, it still takes a request.
        sleep 0.5
        waiting = Thread.new { Net::HTTP.start("127.0.0.1", port, max_retries: 0) { |http| http.get("/") } }
        wait_for("the request to queue") { server.stats[:queued] == 1 }
        sleep 0.2

        expect(server.stats).to include(queued: 1, served: served)
        client.send_text("still bridged")
        expect(client.read).to eq([WsClient::TEXT, "still bridged"])
        expect(exits).to be_empty
        expect(client.close).to eq(1000)
        expect(exits.pop(timeout: WsClient::DEADLINE)).to eq([:exit, nil])
      ensure
        server.shutdown(timeout: 0.5)
        waiting&.kill
      end
    end
  end
end
