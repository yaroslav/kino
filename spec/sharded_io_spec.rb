# frozen_string_literal: true

require "socket"
require "tmpdir"

RSpec.describe "sharded native I/O" do
  def https_get(host, port, path)
    http = Net::HTTP.new(host, port)
    http.use_ssl = true
    http.verify_mode = OpenSSL::SSL::VERIFY_NONE
    http.start { |session| session.get(path) }
  end

  it "serves requests in ractor mode" do
    app = Ractor.shareable_proc { |_env| [200, {"content-type" => "text/plain"}, ["ok"]] }

    with_server(app, mode: :ractor, workers: 2, threads: 1, io_shards: true, io_threads: 2) do |host, port|
      responses = Array.new(8) do
        Thread.new { Net::HTTP.get_response(host, "/", port).body }
      end.map(&:value)

      expect(responses).to all(eq("ok"))
    end
  end

  it "serves TLS requests on shards" do
    app = ->(env) { [200, {"content-type" => "text/plain"}, [env["rack.url_scheme"]]] }
    tls = {cert: TlsFixture.cert_pem, key: TlsFixture.key_pem}

    with_server(app, mode: :threaded, workers: 1, threads: 1,
      io_shards: true, io_threads: 2, tls: tls) do |host, port|
      expect(https_get(host, port, "/").body).to eq("https")
    end
  end

  it "serves unix-socket requests on shards" do
    Dir.mktmpdir("kino-shards-unix") do |dir|
      path = File.join(dir, "kino.sock")
      app = ->(env) { [200, {"content-type" => "text/plain"}, [env["PATH_INFO"]]] }

      with_server(app, bind: "unix://#{path}", io_shards: true, io_threads: 2) do
        response = UNIXSocket.open(path) do |sock|
          sock.write("GET /unix HTTP/1.1\r\nHost: kino\r\nConnection: close\r\n\r\n")
          sock.read
        end

        expect(response).to start_with("HTTP/1.1 200")
        expect(response).to end_with("/unix")
      end
    end
  end

  it "keeps in-flight requests alive while shutdown drains" do
    entered = Queue.new
    release = Queue.new
    app = lambda do |_env|
      entered << true
      release.pop
      [200, {"content-type" => "text/plain"}, ["done"]]
    end
    server = Kino::Server.new(app, mode: :threaded, workers: 1, threads: 1,
      io_shards: true, io_threads: 2).start

    request = Thread.new { Net::HTTP.get_response("127.0.0.1", "/", server.port) }
    expect(entered.pop(timeout: 2)).to be(true)
    shutdown = Thread.new { server.shutdown(timeout: 5) }
    sleep 0.05
    release << true

    expect(request.value.code).to eq("200")
    shutdown.join
  ensure
    release << true if release
    shutdown&.join
    server&.shutdown
  end
end
