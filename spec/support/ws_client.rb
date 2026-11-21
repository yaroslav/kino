# frozen_string_literal: true

require "digest/sha1"
require "openssl"
require "securerandom"
require "socket"
require_relative "waiting"

# A small RFC 6455 client for specs: the opening handshake, masked
# frames, and reads that fail the spec instead of hanging. Plain TCP or
# TLS (Kino's test certificate is self-signed, so it is not verified).
class WsClient
  include Waiting

  TEXT = 0x1
  BINARY = 0x2
  CLOSE = 0x8
  PING = 0x9
  PONG = 0xA

  # RFC 6455 1.3: hashed with the client's key into Sec-WebSocket-Accept.
  GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

  # Seconds a read waits before the spec fails.
  DEADLINE = 5

  # The server answered the handshake with something other than a 101.
  class Refused < StandardError
    attr_reader :status, :headers, :body

    def initialize(status, headers, body)
      @status = status
      @headers = headers
      @body = body
      super("handshake answered with #{status} instead of 101: #{body.inspect}")
    end
  end

  # Nothing arrived before the deadline.
  class TimedOut < StandardError; end

  # The 101's headers, names downcased.
  attr_reader :headers

  # The code of the server's close frame; nil until one arrives (and
  # for ever, when the connection is dropped without one).
  attr_reader :close_code

  def self.open(host, port, path: "/", headers: {}, tls: false)
    socket = TCPSocket.new(host, port)
    if tls
      context = OpenSSL::SSL::SSLContext.new
      context.verify_mode = OpenSSL::SSL::VERIFY_NONE
      socket = OpenSSL::SSL::SSLSocket.new(socket, context)
      socket.sync_close = true
      socket.hostname = host
      socket.connect
    end
    new(socket, "#{host}:#{port}", path, headers)
  end

  # One frame on the wire. Clients mask (RFC 6455 5.3); a server's
  # frames go `mask: false`.
  def self.frame(opcode, payload, mask: true)
    payload = payload.b
    length = payload.bytesize
    mask_bit = mask ? 0x80 : 0
    head = [0x80 | opcode].pack("C")
    head << if length < 126
      [mask_bit | length].pack("C")
    elsif length < 65_536
      [mask_bit | 126, length].pack("Cn")
    else
      [mask_bit | 127, length].pack("CQ>")
    end
    return head + payload unless mask

    key = SecureRandom.random_bytes(4)
    head + key + apply_mask(payload, key)
  end

  def self.apply_mask(payload, key)
    padded = payload + ("\0".b * (-payload.bytesize % 4))
    word = key.unpack1("N")
    padded.unpack("N*").map { |chunk| chunk ^ word }.pack("N*").byteslice(0, payload.bytesize)
  end

  def initialize(io, authority, path, headers)
    @io = io
    @buffer = +"".b
    @closed = false
    handshake(authority, path, headers)
  rescue
    io.close
    raise
  end

  def send_text(text) = send_frame(TEXT, text)

  def send_frame(opcode, payload, mask: true)
    @io.write(self.class.frame(opcode, payload, mask: mask))
  end

  # The next frame as [opcode, payload] (text payloads are UTF-8), or nil
  # once the server dropped the connection.
  def read(timeout: DEADLINE)
    deadline = monotonic + timeout
    first, second = take(2, deadline).bytes
    length = case second & 0x7f
    when 126 then take(2, deadline).unpack1("n")
    when 127 then take(8, deadline).unpack1("Q>")
    else second & 0x7f
    end
    raise "the server masked a frame (RFC 6455 5.1 forbids it)" if second & 0x80 != 0

    opcode = first & 0x0f
    payload = take(length, deadline)
    if opcode == CLOSE
      @closed = true
      @close_code = (payload.bytesize >= 2) ? payload.unpack1("n") : 1005
    end
    payload.force_encoding(Encoding::UTF_8) if opcode == TEXT
    [opcode, payload]
  rescue EOFError, Errno::ECONNRESET
    @closed = true
    nil
  rescue TimedOut
    raise TimedOut, "no frame from the server within #{timeout}s"
  end

  # #read for a client that answers each ping with a pong, as browsers
  # do: the next frame that is not a ping, or nil once the server
  # dropped the connection.
  def read_answering_pings(timeout: DEADLINE)
    deadline = monotonic + timeout
    loop do
      frame = read(timeout: [deadline - monotonic, 0].max)
      return frame unless frame&.first == PING

      send_frame(PONG, frame.last)
    end
  end

  # The code of the server's close frame, read past any frames still on
  # their way; nil when it dropped the connection without one.
  def await_close
    read until @closed
    @close_code
  end

  # The closing handshake: send a close frame, skip what was still on
  # its way, return the code the server answered with.
  def close(code = 1000)
    send_frame(CLOSE, [code].pack("n")) unless @closed
    await_close.tap { disconnect }
  end

  # Drop the connection without a closing handshake.
  def disconnect
    @io.close unless @io.closed?
  end

  private

  def handshake(authority, path, headers)
    key = [SecureRandom.random_bytes(16)].pack("m0")
    lines = ["GET #{path} HTTP/1.1", "Host: #{authority}", "Upgrade: websocket",
      "Connection: Upgrade", "Sec-WebSocket-Key: #{key}", "Sec-WebSocket-Version: 13"]
    headers.each { |name, value| lines << "#{name}: #{value}" }
    @io.write("#{lines.join("\r\n")}\r\n\r\n")

    deadline = monotonic + DEADLINE
    status, @headers = read_head(deadline)
    raise Refused.new(status, @headers, read_body(@headers, deadline)) unless status == 101

    expected = [Digest::SHA1.digest(key + GUID)].pack("m0")
    return if @headers["sec-websocket-accept"] == expected

    raise "wrong Sec-WebSocket-Accept: #{@headers["sec-websocket-accept"].inspect}, expected #{expected}"
  rescue TimedOut
    raise TimedOut, "no complete handshake response within #{DEADLINE}s"
  end

  def read_head(deadline)
    fill(deadline) until (stop = @buffer.index("\r\n\r\n"))
    status_line, *fields = @buffer.byteslice(0, stop).split("\r\n")
    @buffer = @buffer.byteslice(stop + 4..)
    headers = fields.to_h do |field|
      name, value = field.split(":", 2)
      [name.downcase, value.strip]
    end
    [Integer(status_line[/\AHTTP\/1\.1 (\d{3})/, 1]), headers]
  end

  def read_body(headers, deadline) = take(Integer(headers.fetch("content-length", "0")), deadline)

  def take(count, deadline)
    fill(deadline) while @buffer.bytesize < count
    taken = @buffer.byteslice(0, count)
    @buffer = @buffer.byteslice(count..)
    taken
  end

  def fill(deadline)
    loop do
      case (chunk = @io.read_nonblock(65_536, exception: false))
      when :wait_readable, :wait_writable
        remaining = deadline - monotonic
        ready = (remaining > 0) && IO.select([@io.to_io], nil, nil, remaining)
        raise TimedOut unless ready
      when nil
        raise EOFError
      else
        return @buffer << chunk
      end
    end
  end
end
