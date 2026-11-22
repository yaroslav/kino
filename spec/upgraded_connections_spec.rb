# frozen_string_literal: true

require "net/http"

# Upgraded connections, native WebSockets and hijacked ones alike, still
# hold the TCP connection they came in on: they keep its max_connections
# slot.
RSpec.describe "upgraded connections" do
  include_context "native WebSocket server"

  # Runs `attempt` on a thread of its own: it must get nowhere while the
  # server's connection slots are all taken, and complete once the block
  # has freed one. Returns what the attempt returned.
  def expect_held_back(attempt)
    pending = Thread.new(&attempt)
    expect(pending.join(0.5)).to be_nil, "a connection past max_connections was served"
    yield
    expect(pending.join(WsClient::DEADLINE)).to be_truthy,
      "the waiting connection was not served within #{WsClient::DEADLINE}s of a slot freeing"
    pending.value
  end

  it "open native WebSockets count toward max_connections" do
    with_ws_server(ws_app(EchoSocket), max_connections: 2) do |port|
      first, _second = Array.new(2) { connect(port) }

      third = expect_held_back(-> { connect(port) }) { first.close }

      third.send_text("served once a socket closed")
      expect(third.read).to eq([WsClient::TEXT, "served once a socket closed"])
    end
  end

  it "a hijacked connection holds its max_connections slot until the app closes it" do
    with_ws_server(HijackedEcho, lanes: false, max_connections: 1) do |port|
      client = connect(port)

      plain = expect_held_back(-> { Net::HTTP.get("127.0.0.1", "/", port) }) do
        # The app echoes the close frame and closes its socket; the client
        # never closes its own side.
        client.send_frame(WsClient::CLOSE, [1000].pack("n"))
        expect(client.read).to eq([WsClient::CLOSE, [1000].pack("n")])
        expect(client.read).to be_nil, "the app's close did not reach the client"
      end

      expect(plain).to eq("plain")
    end
  end
end
