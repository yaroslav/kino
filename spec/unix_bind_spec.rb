# frozen_string_literal: true

require "socket"
require "tmpdir"

RSpec.describe "unix socket bind" do
  let(:app) do
    ->(env) { [200, {"content-type" => "text/plain"}, ["hi #{env["REMOTE_ADDR"]} #{env["PATH_INFO"]}"]] }
  end

  around do |example|
    Dir.mktmpdir("kino-unix") do |dir|
      @path = File.join(dir, "kino.sock")
      example.run
    end
  end

  def get(path, target)
    UNIXSocket.open(path) do |sock|
      sock.write("GET #{target} HTTP/1.1\r\nHost: kino\r\nConnection: close\r\n\r\n")
      sock.read
    end
  end

  it "serves HTTP over the socket, with a local peer address and no port" do
    with_server(app, bind: "unix://#{@path}") do |_host, port, _server|
      expect(port).to eq(0)
      expect(File.socket?(@path)).to be(true)

      response = get(@path, "/hello")

      expect(response).to start_with("HTTP/1.1 200")
      expect(response).to end_with("hi 127.0.0.1 /hello")
    end
  end

  it "removes the socket file on shutdown" do
    server = Kino::Server.new(app, bind: "unix://#{@path}", workers: 1, threads: 1, mode: :threaded).start
    server.shutdown

    expect(File.exist?(@path)).to be(false)
  end

  it "reclaims a stale socket file nobody is listening on" do
    UNIXServer.new(@path).close
    expect(File.exist?(@path)).to be(true)

    with_server(app, bind: "unix://#{@path}") do
      expect(get(@path, "/")).to start_with("HTTP/1.1 200")
    end
  end

  it "refuses a socket another process is listening on" do
    listener = UNIXServer.new(@path)

    expect {
      Kino::Server.new(app, bind: "unix://#{@path}", workers: 1, threads: 1, mode: :threaded).start
    }.to raise_error(RuntimeError, /in use/)
  ensure
    listener&.close
  end

  it "rejects TLS on a unix socket" do
    expect {
      Kino::Server.new(app, bind: "unix://#{@path}", tls: {cert: TlsFixture.cert_pem, key: TlsFixture.key_pem})
    }.to raise_error(ArgumentError, /unix/)
  end

  it "prints the socket as the listening address" do
    with_server(app, bind: "unix://#{@path}") do |_host, _port, server|
      expect { Kino::CLI.action!(server) }.to output(%r{listening: unix://#{Regexp.escape(@path)}$}).to_stdout
    end
  end
end
