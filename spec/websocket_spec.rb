# frozen_string_literal: true

# Has on_close alone, reporting [:close, socket id].
CloseOnlySocket = Data.define(:port) do
  def on_close(ws) = port << [:close, ws.id]
end

# Writes frames of `size` bytes in on_open, before its client has the
# socket and so before any goes out, until one is refused (at most
# 1,024), and reports [writes accepted, ws.pending].
OpenFillingSocket = Data.define(:port, :size) do
  def on_open(ws)
    frame = "x" * size
    port << [(1..1024).take_while { ws.write(frame) }.size, ws.pending]
  end
end

# Reports each socket's handle as it opens, and [:close, socket id] as
# it closes.
HandleSocket = Data.define(:port) do
  def on_open(ws) = port << ws

  def on_close(ws) = port << [:close, ws.id]
end

# One per socket, with mutable state: answers each message with how
# many its socket has sent.
class CountingSocket
  def initialize = @count = 0

  def on_message(ws, _data) = ws.write((@count += 1).to_s)
end

# Flow control: "fill" writes 1 MiB frames until one is refused (at most
# 64) and reports [:filled, writes accepted, ws.pending]; "echo" echoes;
# "pending" reports [:pending, ws.pending]. on_drained reports
# [:drained, ws.pending].
DrainingSocket = Data.define(:port) do
  def on_message(ws, data)
    case data
    when "fill"
      frame = "x" * 1_048_576
      accepted = (1..64).take_while { ws.write(frame) }.size
      port << [:filled, accepted, ws.pending]
    when "pending" then port << [:pending, ws.pending]
    else ws.write(data)
    end
  end

  def on_drained(ws) = port << [:drained, ws.pending]
end

# Closes with the code each message names, or answers with the refusal.
module ClosingSocket
  def self.on_message(ws, data)
    ws.close(Integer(data))
  rescue ArgumentError => e
    ws.write(e.message)
  end
end

RSpec.describe "native WebSockets" do
  include_context "native WebSocket server"

  [{mode: :ractor}, {mode: :threaded}, {mode: :ractor, batch: 4}, {mode: :threaded, tls: true}].each do |opts|
    it "echoes text and binary messages in order (#{opts})" do
      tls = opts.fetch(:tls, false)
      with_ws_server(ws_app(EchoSocket), **opts.except(:tls), **(tls ? TlsFixture.options : {})) do |port|
        client = connect(port, tls:)
        # Sizes cross both extended-length boundaries (126 and 65_536 bytes).
        sent = Array.new(100) do |index|
          text = "#{index}é" + "x" * ((index % 25 == 24) ? 70_000 : index * 3)
          index.even? ? [WsClient::TEXT, text] : [WsClient::BINARY, text.b]
        end

        sent.each { |opcode, payload| client.send_frame(opcode, payload) }

        expect(Array.new(100) { client.read }).to eq(sent)
      end
    end
  end

  it "passes the app's headers into the 101, and sends the app's own response when it declines a handshake" do
    accepting = ws_app(EchoSocket, {"sec-websocket-protocol" => "chat", "set-cookie" => "seen=1"})
    app = Ractor.shareable_proc do |env|
      next accepting.call(env) unless env["PATH_INFO"] == "/declined"

      [403, {"content-type" => "text/plain", "x-declined" => "yes"}, ["declined, offered #{env["rack.upgrade?"].inspect}"]]
    end
    with_ws_server(app) do |port|
      client = connect(port, headers: {"Sec-WebSocket-Protocol" => "chat, superchat"})
      expect(client.headers).to include("sec-websocket-protocol" => "chat", "set-cookie" => "seen=1")
      client.send_text("still a socket")
      expect(client.read).to eq([WsClient::TEXT, "still a socket"])

      expect { connect(port, path: "/declined") }.to raise_error(WsClient::Refused) { |refused|
        expect([refused.status, refused.headers["x-declined"], refused.body]).to eq([403, "yes", "declined, offered :websocket"])
      }
    end
  end

  it "closes with 1009 past websocket_max_message" do
    with_ws_server(ws_app(EchoSocket), websocket_max_message: 64) do |port|
      client = connect(port)

      client.send_text("a" * 64)
      expect(client.read).to eq([WsClient::TEXT, "a" * 64])

      client.send_text("a" * 65)
      expect(client.await_close).to eq(1009)
    end
  end

  it "closes with any code an endpoint may send, and refuses the rest" do
    with_ws_server(ws_app(ClosingSocket)) do |port|
      client = connect(port)

      [0, 999, 1004, 1005, 1006, 1012, 1015, 2999, 5000, 70_000].each do |code|
        client.send_text(code.to_s)
        expect(client.read).to eq([WsClient::TEXT, "invalid close code #{code}: send 1000-1003, 1007-1011 or 3000-4999"])
      end
      client.send_text("4000")

      expect(client.await_close).to eq(4000)
    end
  end

  it "does not offer rack.upgrade? without lanes, and warns once per server" do
    app = Ractor.shareable_proc do |env|
      env["rack.upgrade"] = EchoSocket # set regardless, as a careless app would
      Kino.sleep(0.2) # two handshakes at once then reach both workers
      [426, {"content-type" => "text/plain"}, ["rack.upgrade? #{env["rack.upgrade?"].inspect} on #{Ractor.current.name}"]]
    end

    log = capture_native_stderr do
      with_ws_server(app, lanes: false) do |port|
        refusals = Array.new(2) { Thread.new { refusal(port) } }.map(&:value)

        expect(refusals.map { |refused| refused&.status }).to eq([426, 426])
        bodies = refusals.map(&:body)
        expect(bodies).to all(start_with("rack.upgrade? nil on worker-"))
        expect(bodies.uniq.size).to eq(2), "both workers must meet the warning: #{bodies}"
      end
    end

    expect(log.scan("rack.upgrade").size).to eq(1), log
    expect(log).to include("lanes true")
  end

  each_mode do |mode|
    it "holds 256 frames for a client however small they are, and 4 MiB however few" do
      reported = reports.port
      app = Ractor.shareable_proc do |env|
        if env["rack.upgrade?"] == :websocket
          env["rack.upgrade"] = OpenFillingSocket.new(port: reported, size: Integer(env["PATH_INFO"][1..]))
        end
        [101, {}, []]
      end
      with_ws_server(app, mode:) do |port|
        # Frames of 1 KiB fill the queue at 256, far below 4 MiB; four of
        # 1,000,000 bytes fit, and with its head a fifth would pass 4 MiB;
        # a frame of any size fits an empty queue.
        {1_024 => 256, 1_000_000 => 4, 5_000_000 => 1}.each do |size, fit|
          connect(port, path: "/#{size}")
          accepted, pending = reports.next(awaiting: "the writes on_open got through")

          expect([accepted, pending]).to eq([fit, fit]),
            "#{accepted} writes of #{size} bytes accepted and #{pending} pending, not #{fit}"
        end
      end
    end

    it "lets a handler leave out callbacks, and gives back the credit of messages it has no on_message for" do
      log = capture_native_stderr do
        with_ws_server(ws_app(CloseOnlySocket.new(port: reports.port)), mode:) do |port|
          client = connect(port)

          # Were their credit not given back, the reader would stop at 16
          # and never read the close.
          32.times { |index| client.send_text("unheard #{index}") }

          expect(client.close).to eq(1000), "the close behind the messages went unread"
          expect(reports.next(awaiting: "on_close").first).to eq(:close)
        end
      end

      expect(log).to be_empty
    end

    it "counts frames not yet written in ws.pending, and runs on_drained once a refused write's queue empties, " \
      "and only then" do
      with_ws_server(ws_app(DrainingSocket.new(port: reports.port)), mode:) do |port|
        client = connect(port)
        client.send_text("echo")
        expect(client.read).to eq([WsClient::TEXT, "echo"])
        client.send_text("pending")
        expect(reports.next(awaiting: "ws.pending")).to eq([:pending, 0]), "on_drained ran with no write refused"

        client.send_text("fill") # and read nothing for now
        filled, accepted, pending = reports.next(awaiting: "the count of writes accepted")
        expect(filled).to eq(:filled)
        expect(accepted).to be_between(2, 63), "#{accepted} of 64 writes of 1 MiB went through"
        expect(pending).to be_between(1, accepted), "#{pending} of #{accepted} frames reported unwritten"
        expect { reports.next(timeout: 0.3, awaiting: "on_drained") }.to raise_error(/sent no on_drained/)

        expect(Array.new(accepted) { client.read.first }).to all(eq(WsClient::TEXT))
        expect(reports.next(awaiting: "on_drained")).to eq([:drained, 0])
      end
    end

    it "gives each socket its own handler object, with mutable state" do
      app = Ractor.shareable_proc do |env|
        env["rack.upgrade"] = CountingSocket.new if env["rack.upgrade?"] == :websocket
        [101, {}, []]
      end
      with_ws_server(app, mode:, workers: 1) do |port|
        sockets = Array.new(2) { connect(port) }

        replies = sockets.map do |client|
          Array.new(3) do
            client.send_text("count")
            client.read.last
          end
        end

        expect(replies).to eq([%w[1 2 3], %w[1 2 3]])
      end
    end

    it "hands out socket handles, frozen, shareable and equal by id, that work from another Ractor and a plain " \
      "thread until their socket is gone" do
      with_ws_server(ws_app(HandleSocket.new(port: reports.port)), mode:) do |port|
        client = connect(port)
        ws = reports.next(awaiting: "the socket's handle")

        expect(ws).to be_frozen
        expect(Ractor.shareable?(ws)).to be(true)
        expect(Kino::WebSocket[ws.id]).to eq(ws).and(eql(ws))
        expect(Kino::WebSocket[ws.id].hash).to eq(ws.hash)
        expect(Ractor.new(ws) { |socket| socket.write("from a ractor") }.value).to be(true)
        expect(client.read).to eq([WsClient::TEXT, "from a ractor"])
        expect(Ractor.new(ws.id) { |id| Kino::WebSocket[id].subscribe("handles") }.value).to be(true)
        Kino::WebSocket.publish("handles", "published")
        expect(client.read).to eq([WsClient::TEXT, "published"])
        expect(Thread.new { ws.close(4000) }.value).to be(true)

        expect(client.close).to eq(4000)
        expect(reports.next(awaiting: "on_close")).to eq([:close, ws.id])
        expect(Kino::WebSocket[ws.id]).to be_nil
        expect(ws.write("too late")).to be(false)
      end
    end
  end
end
