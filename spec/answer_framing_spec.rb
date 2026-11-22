# frozen_string_literal: true

require "socket"

# How a buffered answer is framed on a connection that stays open: by its
# body, never by the app's content-length or transfer-encoding, so the
# answer behind it on the connection is read whole. A 204 names no length,
# and a HEAD gets its body's length and no body.
FRAMING_APP = Ractor.shareable_proc do |env|
  case env["PATH_INFO"]
  when "/long" then [200, {"content-length" => "5"}, ["hello world"]]
  when "/short" then [200, {"content-length" => "20"}, ["hello"]]
  when "/bare" then [200, {}, ["hello"]]
  when "/encoded" then [200, {"transfer-encoding" => "chunked"}, ["hello"]]
  when "/both" then [200, {"content-length" => "5", "transfer-encoding" => "chunked"}, ["hello"]]
  when "/empty" then [204, {"content-length" => "5"}, []]
  when "/streamed-empty" then [204, {"content-length" => "5"}, Enumerator.new { |out| out << "" }]
  else [200, {"content-length" => "2"}, ["ok"]]
  end
end

# The request, then the content-length (nil: none) and the body it reads.
FRAMING_ROWS = [
  ["GET", "/long", "11", "hello world"],
  ["GET", "/short", "5", "hello"],
  ["GET", "/encoded", "5", "hello"],
  ["GET", "/both", "5", "hello"],
  ["HEAD", "/bare", "5", ""],
  ["GET", "/empty", nil, ""],
  ["HEAD", "/empty", nil, ""],
  ["GET", "/streamed-empty", nil, ""],
  ["HEAD", "/streamed-empty", nil, ""]
].freeze

# What a CONNECT, answered 200 with no body, reads back: no length, as
# RFC 9110 forbids one on a 2xx to a CONNECT.
CONNECT_APP = Ractor.shareable_proc { |_env| [200, {}, []] }

RSpec.describe "Answer framing" do
  each_mode do |mode|
    it "frames a buffered answer by its body, and the next answer on the connection is read whole" do
      with_server(FRAMING_APP, mode: mode, workers: 1, threads: 1) do |_host, port|
        FRAMING_ROWS.each do |method, path, length, body|
          socket = client_socket(port)
          socket.write("#{method} #{path} HTTP/1.1\r\nhost: x\r\n\r\nGET /next HTTP/1.1\r\nhost: x\r\n\r\n")
          _status, headers = HttpResponse.head(socket)
          read = (method == "HEAD") ? "" : HttpResponse.body(socket, headers)

          expect(headers.values_at("content-length", "transfer-encoding")).to eq([length, nil]), "#{method} #{path}"
          expect(read).to eq(body), "#{method} #{path}"
          expect(read_response(socket).values_at(0, 2)).to eq(["HTTP/1.1 200 OK", "ok"]), "#{method} #{path}"
        ensure
          socket&.close
        end
      end
    end

    it "answers a CONNECT 200 with no length" do
      with_server(CONNECT_APP, mode: mode, workers: 1, threads: 1) do |_host, port|
        socket = client_socket(port)
        socket.write("CONNECT example.test:443 HTTP/1.1\r\nhost: example.test:443\r\n\r\n")

        expect(socket.gets("\r\n\r\n")).not_to match(/content-length/i)
      ensure
        socket&.close
      end
    end
  end
end
