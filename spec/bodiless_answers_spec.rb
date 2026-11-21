# frozen_string_literal: true

require "httpx"
require "socket"

# What the /stream paths write in two chunks, and /raise raises before an
# answer.
BODILESS_APP = Ractor.shareable_proc do |env|
  case env["PATH_INFO"]
  when "/stream" then [200, {}, Enumerator.new { |out| out << "hel" << "lo" }]
  when "/stream-204" then [204, {}, Enumerator.new { |out| out << "hel" << "lo" }]
  when "/stream-304" then [304, {}, Enumerator.new { |out| out << "hel" << "lo" }]
  when "/raise" then raise "the app failed"
  else [200, {"content-length" => "2"}, ["ok"]]
  end
end

BODILESS_SHAPES = [
  ["a buffered answer to a HEAD", "HEAD", "/buffered", "200"],
  ["a streamed answer to a HEAD", "HEAD", "/stream", "200"],
  ["a streamed 204 that writes", "GET", "/stream-204", "204"],
  ["a streamed 304 that writes", "GET", "/stream-304", "304"],
  ["a canned 500 to a HEAD", "HEAD", "/raise", "500"]
].freeze

# No body bytes where HTTP allows no body, on any answer: a buffered one, a
# streamed one whose app writes anyway, and Kino's own 500, over HTTP/1.1
# (the next answer on the connection is read whole) and over HTTP/2 (no
# DATA).
RSpec.describe "Answers that carry no body" do
  each_mode do |mode|
    it "sends no body bytes over HTTP/1.1 and keeps the next answer whole" do
      with_server(BODILESS_APP, mode: mode, workers: 1, threads: 1) do |_host, port|
        BODILESS_SHAPES.each do |label, method, path, code|
          socket = client_socket(port)
          socket.write("#{method} #{path} HTTP/1.1\r\nhost: x\r\n\r\nGET /next HTTP/1.1\r\nhost: x\r\n\r\n")
          head, = HttpResponse.head(socket)
          # Read as the next answer, a byte of body behind the first head would break its status line.
          status, _headers, body = read_response(socket)

          expect(head).to start_with("HTTP/1.1 #{code} "), label
          expect(status).to eq("HTTP/1.1 200 OK"), label
          expect(body).to eq("ok"), label
        ensure
          socket&.close
        end
      end
    end

    it "sends no DATA over HTTP/2" do
      with_server(BODILESS_APP, mode: mode, workers: 1, threads: 1) do |_host, port|
        client = HTTPX.with(fallback_protocol: "h2")
        BODILESS_SHAPES.each do |label, method, path, code|
          response = client.request(method, "http://127.0.0.1:#{port}#{path}")

          expect(response.status).to eq(Integer(code)), label
          expect(response.body.to_s).to eq(""), label
        end
      end
    end
  end
end
