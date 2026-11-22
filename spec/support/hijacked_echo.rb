# frozen_string_literal: true

# What Action Cable does on a server with full rack.hijack: take the
# connection, answer the WebSocket handshake on it, and serve the socket
# from a thread of its own (here echoing every message) while the worker
# moves on. Its triple is Action Cable's, which no server may send.
# Requests without an Upgrade header get a plain 200.
module HijackedEcho
  def self.call(env)
    return [200, {"content-type" => "text/plain"}, ["plain"]] unless env["rack.hijack?"] && env["HTTP_UPGRADE"]

    io = env["rack.hijack"].call
    key = env["HTTP_SEC_WEBSOCKET_KEY"]
    Thread.new { serve(io, key) }
    [-1, {}, []]
  end

  # The headers of a 101 that accepts the WebSocket handshake in `env`, for apps that switch the
  # connection themselves.
  def self.accept_headers(env)
    accept = WsClient.accept(env["HTTP_SEC_WEBSOCKET_KEY"])
    {"upgrade" => "websocket", "connection" => "Upgrade", "sec-websocket-accept" => accept}
  end

  def self.serve(io, key)
    io.write("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n" \
      "Connection: Upgrade\r\nSec-WebSocket-Accept: #{WsClient.accept(key)}\r\n\r\n")
    while (frame = read_frame(io))
      opcode, payload = frame
      io.write(WsClient.frame(opcode, payload, mask: false))
      break if opcode == WsClient::CLOSE
    end
  rescue IOError, SystemCallError
    # The client or the server went away; nothing to answer.
  ensure
    io.close
  end

  # The next (masked) client frame as [opcode, payload]; nil once the
  # connection ends, mid-frame included.
  def self.read_frame(io)
    head = take(io, 2) or return
    length = head.getbyte(1) & 0x7f
    length = take(io, 2)&.unpack1("n") if length == 126
    length = take(io, 8)&.unpack1("Q>") if length == 127
    key = length && take(io, 4)
    payload = key && take(io, length)
    [head.getbyte(0) & 0x0f, WsClient.apply_mask(payload, key)] if payload
  end

  def self.take(io, count)
    data = io.read(count)
    data if data&.bytesize == count
  end
end
