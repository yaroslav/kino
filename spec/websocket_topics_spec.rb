# frozen_string_literal: true

require "net/http"

# Subscribes each socket to "room" and greets its client with "<worker
# ractor's name, or main> <socket id>". A message from a client is
# published to "room" from that worker.
module RoomSocket
  def self.on_open(ws)
    ws.subscribe("room")
    ws.write("#{Ractor.current.name || "main"} #{ws.id}")
  end

  def self.on_message(_ws, data) = Kino::WebSocket.publish("room", data)
end

# Subscribes each socket to "news" when it opens, after a few
# milliseconds of setup, as an app that first loads its user would.
module NewsSocket
  def self.on_open(ws)
    Kino.sleep(0.005)
    ws.subscribe("news")
  end
end

# Subscribes each socket to "bytes" in an envelope that is not UTF-8,
# to "chat" in two envelopes (one of them twice), and to "marker" bare;
# then reports the socket's handle.
EnvelopedSocket = Data.define(:port) do
  def on_open(ws)
    ws.subscribe("bytes", prefix: "\x00\xff".b, suffix: "\xfe".b)
    2.times { ws.subscribe("chat", prefix: "a:") }
    ws.subscribe("chat", prefix: "b:", suffix: ";")
    ws.subscribe("marker")
    port << ws
  end
end

# "subscribe <n>" subscribes the socket to "wide" through n envelopes
# that skip when full, and answers "ok". "race" subscribes to "narrow"
# as another thread of the worker keeps running; it answers how long
# the subscribe took and the longest that thread stood still, both in
# ms.
module FanOutSocket
  def self.now = Kino.monotonic

  def self.on_message(ws, data)
    command, count = data.split
    if command == "subscribe"
      Integer(count).times { |index| ws.subscribe("wide", prefix: index.to_s, when_full: :skip) }
      return ws.write("ok")
    end

    started = now
    stop = false
    other = Thread.new do
      last, longest = started, 0.0
      loop do
        tick = now
        longest = [longest, tick - last].max
        last = tick
        break if stop
      end
      longest
    end
    ws.subscribe("narrow")
    subscribed = now
    stop = true
    ws.write(format("%.1f %.1f", (subscribed - started) * 1000, other.value * 1000))
  end
end

RSpec.describe "WebSocket topics" do
  include_context "native WebSocket server"

  def text(payload) = [WsClient::TEXT, payload]

  it "delivers a publish from any worker, a plain HTTP request or another Ractor to subscribers on every worker" do
    # Accepts handshakes with RoomSocket, each taking 10 ms so that many
    # made at once spread over the workers. Any other request publishes
    # its query string to "room" and answers with publish's result.
    app = Ractor.shareable_proc do |env|
      if env["rack.upgrade?"] == :websocket
        sleep 0.01
        env["rack.upgrade"] = RoomSocket
        [101, {}, []]
      else
        [200, {"content-type" => "text/plain"}, [Kino::WebSocket.publish("room", env["QUERY_STRING"]).to_s]]
      end
    end
    with_ws_server(app, workers: 4) do |port|
      # Clients subscribed to "room": [client, worker name, socket id].
      members = Array.new(40) do
        Thread.new do
          client = connect(port)
          worker, id = client.read.last.split
          [client, worker, Integer(id)]
        end
      end.map(&:value)
      by_worker = members.group_by { |_client, worker, _id| worker }
      expect(by_worker.size).to eq(4), "40 sockets opened on #{by_worker.keys.sort} only"

      # One member on each worker publishes.
      sent = by_worker.map do |worker, on_worker|
        publisher, = on_worker.first
        publisher.send_text("from #{worker}")
        "from #{worker}"
      end
      expect(Net::HTTP.get("127.0.0.1", "/?from-a-request", port)).to eq("true")
      expect(Ractor.new { Kino::WebSocket.publish("room", "from-a-ractor") }.value).to be(true)
      sent += %w[from-a-request from-a-ractor]

      members.each do |client, worker, id|
        received = Array.new(6) { client.read }
        expect(received).to match_array(sent.map { |body| text(body) }), "socket #{id} on #{worker}"
      end
    end
  end

  it "delivers a publish made as soon as the client sees the socket open to a subscription from on_open" do
    with_ws_server(ws_app(NewsSocket)) do |port|
      10.times do |index|
        client = connect(port)
        Kino::WebSocket.publish("news", "item #{index}")

        received = begin
          client.read(timeout: 1)
        rescue WsClient::TimedOut
          nil
        end
        expect(received).to eq(text("item #{index}")), "handshake #{index}: the publish missed on_open's subscription"
        client.disconnect
      end
    end
  end

  it "wraps each publish in every envelope of the topic's subscriptions, text and binary, until unsubscribed" do
    with_ws_server(ws_app(EnvelopedSocket.new(port: reports.port))) do |port|
      client = connect(port)
      ws = reports.next(awaiting: "the socket's handle")

      expect(Kino::WebSocket.publish("bytes", "\x01\x02".b, binary: true)).to be(true)
      expect(client.read).to eq([WsClient::BINARY, "\x00\xff\x01\x02\xfe".b])
      expect(Kino::WebSocket.publish("chat", "\xff".b, binary: true)).to be(true)
      expect(Array.new(2) { client.read }).to contain_exactly([WsClient::BINARY, "a:\xff".b], [WsClient::BINARY, "b:\xff;".b])

      # A text publish must be UTF-8, whatever the String's encoding says,
      # and leaves out the envelopes that are not.
      expect { Kino::WebSocket.publish("chat", "\xff") }.to raise_error(ArgumentError, /UTF-8.*binary: true/)
      expect { Kino::WebSocket.publish("chat", "\xff".b) }.to raise_error(ArgumentError)
      expect(Kino::WebSocket.publish("chat", "café".b)).to be(true)
      expect(Kino::WebSocket.publish("bytes", "text")).to be(true)
      Kino::WebSocket.publish("marker", "end")
      expect(Array.new(3) { client.read }).to eq([text("a:café"), text("b:café;"), text("end")])

      expect(ws.unsubscribe("chat", prefix: "b:")).to be(true)
      Kino::WebSocket.publish("chat", "1")
      Kino::WebSocket.publish("marker", "next")
      expect(Array.new(2) { client.read }).to eq([text("a:1"), text("next")])
      expect(ws.unsubscribe("chat")).to be(true)
      expect(ws.unsubscribe("chat")).to be(false)
      Kino::WebSocket.publish("chat", "2")
      Kino::WebSocket.publish("marker", "end")
      expect(client.read).to eq(text("end"))
      expect { ws.subscribe("chat", when_full: :drop) }
        .to raise_error(ArgumentError, "when_full must be :close or :skip, not :drop")
    end
  end

  # A subscribe that waits lets the worker's other threads run, in
  # either mode.
  each_mode do |mode|
    it "keeps a worker's other threads running while a subscribe waits for a long fan-out" do
      with_ws_server(ws_app(FanOutSocket), mode:, workers: 1) do |port, server|
        wide = connect(port)
        wide.send_text("subscribe 4000")
        expect(wide.read).to eq(text("ok"))
        racer = connect(port)

        # 2 MiB through 4000 envelopes: the socket's queue fills within the
        # first few, and each one after that is skipped, so a skip tells
        # that the fan-out is under way, with nearly all of it to go.
        skipped = -> { server.stats[:websockets][:messages_skipped] }
        before = skipped.call
        Kino::WebSocket.publish("wide", "x" * 2_097_152)
        wait_for("the fan-out under way", every: 0.001) { skipped.call > before }
        racer.send_text("race")

        subscribe_ms, still_ms = racer.read(timeout: 30).last.split.map { |ms| Float(ms) }
        expect(subscribe_ms).to be > 30, "the subscribe took #{subscribe_ms} ms: it never waited for the fan-out"
        # Held up, the other thread stands still for the whole wait.
        expect(still_ms).to be < subscribe_ms / 3,
          "another thread of the worker stood still for #{still_ms} ms while the subscribe waited #{subscribe_ms} ms"
      end
    end
  end
end
