# frozen_string_literal: true

# Phase 1: the wire works. Real sockets, real HTTP: no rack-test fakery.
RSpec.describe "HTTP basics" do
  let(:hello_app) do
    lambda do |env|
      [200, {"content-type" => "text/plain", "x-kino" => "yes"},
        ["Hello from #{env["REQUEST_METHOD"]} #{env["PATH_INFO"]}"]]
    end
  end

  it "serves a GET request" do
    with_server(hello_app) do |host, port|
      response = Net::HTTP.get_response(host, "/greetings", port)

      expect(response.code).to eq("200")
      expect(response.body).to eq("Hello from GET /greetings")
      expect(response["x-kino"]).to eq("yes")
    end
  end

  it "exposes request headers and query string in the env" do
    echo_env = lambda do |env|
      body = "#{env["QUERY_STRING"]}|#{env["HTTP_X_REQUEST_ID"]}|#{env["SERVER_NAME"]}"
      [200, {"content-type" => "text/plain"}, [body]]
    end

    with_server(echo_env) do |host, port|
      http = Net::HTTP.new(host, port)
      response = http.get("/echo?a=1&b=2", {"x-request-id" => "abc-123"})

      expect(response.body).to eq("a=1&b=2|abc-123|127.0.0.1")
    end
  end

  it "resolves SERVER_NAME and SERVER_PORT per Host header (cached values stay distinct)" do
    echo_host = lambda do |env|
      [200, {"content-type" => "text/plain"}, ["#{env["SERVER_NAME"]}:#{env["SERVER_PORT"]}"]]
    end

    with_server(echo_host) do |host, port|
      http = Net::HTTP.new(host, port)
      first = http.get("/", {"Host" => "alpha.example:8080"}).body
      second = http.get("/", {"Host" => "beta.example:9090"}).body
      third = http.get("/", {"Host" => "alpha.example:8080"}).body

      expect(first).to eq("alpha.example:8080")
      expect(second).to eq("beta.example:9090")
      expect(third).to eq("alpha.example:8080")
    end
  end

  it "round-trips a POST body" do
    echo_body = lambda do |env|
      [200, {"content-type" => "application/octet-stream"}, [env["rack.input"].read]]
    end

    with_server(echo_body) do |host, port|
      http = Net::HTTP.new(host, port)
      response = http.post("/upload", "some posted bytes", {"content-type" => "text/plain"})

      expect(response.code).to eq("200")
      expect(response.body).to eq("some posted bytes")
    end
  end

  it "returns 500 when the app raises" do
    boom = ->(_env) { raise "kaput" }

    with_server(boom) do |host, port|
      response = Net::HTTP.get_response(host, "/", port)

      expect(response.code).to eq("500")
      expect(response.body).to include("Internal Server Error")
    end
  end

  it "keeps serving after an app error" do
    flaky = lambda do |env|
      raise "kaput" if env["PATH_INFO"] == "/boom"

      [200, {"content-type" => "text/plain"}, ["still alive"]]
    end

    with_server(flaky) do |host, port|
      expect(Net::HTTP.get_response(host, "/boom", port).code).to eq("500")
      expect(Net::HTTP.get_response(host, "/ok", port).body).to eq("still alive")
    end
  end

  it "stops cleanly: port released, worker threads gone" do
    server = Kino::Server.new(hello_app).start
    port = server.port
    expect(Net::HTTP.get_response("127.0.0.1", "/", port).code).to eq("200")

    threads_before = Thread.list.size
    server.shutdown

    expect(Thread.list.size).to be < threads_before
    expect do
      Net::HTTP.start("127.0.0.1", port, open_timeout: 0.5) { |http| http.get("/") }
    end.to raise_error(SystemCallError)
  end
end
