# frozen_string_literal: true

require "httpx"
require "rack"

# HTTP/2 serving: ALPN-negotiated h2 over TLS, prior-knowledge h2c on
# plaintext, and the h2-specific Rack env contract (SERVER_PROTOCOL,
# HTTP_HOST/SERVER_NAME/SERVER_PORT from the :authority pseudo-header).
RSpec.describe "HTTP/2" do
  def tls_opts
    {tls: {cert: TlsFixture.cert_pem, key: TlsFixture.key_pem}}
  end

  # An h2-over-TLS client; kino's test cert is self-signed.
  def h2_tls_client
    HTTPX.with(ssl: {verify_mode: OpenSSL::SSL::VERIFY_NONE})
  end

  # A prior-knowledge h2c client: speaks the h2 preface on plaintext.
  def h2c_client
    HTTPX.with(fallback_protocol: "h2")
  end

  def echo_env
    lambda do |env|
      body = [
        env["SERVER_PROTOCOL"], env["HTTP_HOST"], env["SERVER_NAME"],
        env["SERVER_PORT"], env["rack.url_scheme"]
      ].join("|")
      [200, {"content-type" => "text/plain"}, [body]]
    end
  end

  it "negotiates h2 over TLS and fills the env from :authority" do
    with_server(echo_env, **tls_opts) do |host, port|
      response = h2_tls_client.get("https://#{host}:#{port}/")

      expect(response.version).to eq("2.0")
      expect(response.status).to eq(200)
      expect(response.body.to_s)
        .to eq("HTTP/2|#{host}:#{port}|#{host}|#{port}|https")
    end
  end

  it "serves prior-knowledge h2c on plaintext" do
    with_server(echo_env) do |host, port|
      response = h2c_client.get("http://#{host}:#{port}/")

      expect(response.version).to eq("2.0")
      expect(response.body.to_s)
        .to eq("HTTP/2|#{host}:#{port}|#{host}|#{port}|http")
    end
  end

  it "still serves HTTP/1.1 on the same plaintext port" do
    with_server(echo_env) do |host, port|
      response = Net::HTTP.start(host, port) { |http| http.get("/") }

      expect(response.body).to start_with("HTTP/1.1|")
    end
  end

  it "streams a chunked response over h2" do
    chunks = Enumerator.new do |yielder|
      yielder << "alpha "
      yielder << "beta "
      yielder << "gamma"
    end
    app = ->(_env) { [200, {"content-type" => "text/plain"}, chunks] }

    with_server(app) do |host, port|
      response = h2c_client.get("http://#{host}:#{port}/")

      expect(response.version).to eq("2.0")
      expect(response.body.to_s).to eq("alpha beta gamma")
    end
  end

  it "reads small and multi-frame uploads over h2" do
    app = ->(env) { [200, {"content-type" => "text/plain"}, [env["rack.input"].read.bytesize.to_s]] }

    with_server(app) do |host, port|
      url = "http://#{host}:#{port}/upload"

      small = h2c_client.post(url, body: "hello")
      expect(small.version).to eq("2.0")
      expect(small.body.to_s).to eq("5")

      large = "x" * (256 * 1024)
      big = h2c_client.post(url, body: large)
      expect(big.body.to_s).to eq(large.bytesize.to_s)
    end
  end

  it "serves multiplexed requests on one connection" do
    app = ->(env) { [200, {"content-type" => "text/plain"}, [env["PATH_INFO"]]] }

    with_server(app, workers: 1, threads: 2) do |host, port|
      responses = h2c_client.get(
        "http://#{host}:#{port}/one", "http://#{host}:#{port}/two"
      )

      expect(responses.map(&:status)).to eq([200, 200])
      expect(responses.map { |r| r.body.to_s }).to eq(["/one", "/two"])
      expect(responses.map(&:version).uniq).to eq(["2.0"])
    end
  end

  it "serves h2c through sharded I/O" do
    with_server(echo_env, io_shards: true) do |host, port|
      response = h2c_client.get("http://#{host}:#{port}/")

      expect(response.version).to eq("2.0")
      expect(response.body.to_s).to start_with("HTTP/2|")
    end
  end

  it "passes Rack::Lint over h2" do
    app = Rack::Lint.new(echo_env)

    with_server(app) do |host, port|
      response = h2c_client.get("http://#{host}:#{port}/")

      expect(response.status).to eq(200)
      expect(response.body.to_s).to start_with("HTTP/2|")
    end
  end

  it "keeps distinct authorities distinct in the host cache" do
    with_server(echo_env) do |_host, port|
      by_ip = h2c_client.get("http://127.0.0.1:#{port}/")
      by_name = h2c_client.get("http://localhost:#{port}/")

      expect(by_ip.body.to_s)
        .to eq("HTTP/2|127.0.0.1:#{port}|127.0.0.1|#{port}|http")
      expect(by_name.body.to_s)
        .to eq("HTTP/2|localhost:#{port}|localhost|#{port}|http")
    end
  end

  it "upgrades a Host-header cache entry in place for h2" do
    with_server(echo_env) do |host, port|
      # The same authority first arrives as an h1 Host header (caching
      # name+port without the full host string), then as :authority.
      h1 = Net::HTTP.start(host, port) { |http| http.get("/") }
      expect(h1.body).to eq("HTTP/1.1|#{host}:#{port}|#{host}|#{port}|http")

      h2 = h2c_client.get("http://#{host}:#{port}/")
      expect(h2.body.to_s).to eq("HTTP/2|#{host}:#{port}|#{host}|#{port}|http")
    end
  end

  describe "http2 false" do
    it "negotiates down to HTTP/1.1 over TLS" do
      with_server(echo_env, http2: false, **tls_opts) do |host, port|
        response = h2_tls_client.get("https://#{host}:#{port}/")

        expect(response.version).to eq("1.1")
        expect(response.body.to_s).to start_with("HTTP/1.1|")
      end
    end

    it "refuses the h2 preface on plaintext" do
      with_server(echo_env, http2: false) do |host, port|
        response = h2c_client.get("http://#{host}:#{port}/")

        expect(response).to be_a(HTTPX::ErrorResponse)
      end
    end
  end
end
