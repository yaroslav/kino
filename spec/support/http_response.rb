# frozen_string_literal: true

# Reads an HTTP/1.1 response off an IO: a connection, or a StringIO of
# what one read.
module HttpResponse
  module_function

  # The status line and the headers (names downcased).
  def head(io)
    status = io.gets("\r\n")&.chomp("\r\n") or raise "the connection closed before a response"
    headers = {}
    until (field = line(io)).empty?
      name, value = field.split(":", 2)
      headers[name.downcase] = value.strip
    end
    [status, headers]
  end

  # The body after `headers`, framed by its chunks (joined) or its
  # content-length.
  def body(io, headers)
    return exactly(io, Integer(headers.fetch("content-length", 0))) unless headers["transfer-encoding"]&.include?("chunked")

    body = +"".b
    until (size = Integer(line(io), 16)).zero?
      body << exactly(io, size)
      raise "a chunk ran past its size" unless exactly(io, 2) == "\r\n"
    end
    raise "the body did not end with its last chunk" unless exactly(io, 2) == "\r\n"
    body
  end

  def line(io) = io.gets("\r\n")&.chomp("\r\n") || raise("the connection closed mid-response")

  def exactly(io, count)
    read = io.read(count) || +""
    raise "the connection closed mid-response, #{count - read.bytesize} bytes short" if read.bytesize < count

    read
  end
end
