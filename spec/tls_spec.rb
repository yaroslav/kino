# frozen_string_literal: true

# Phase 5: TLS termination in Rust via rustls.
RSpec.describe "TLS" do
  def tls_opts
    {tls: {cert: TlsFixture.cert_pem, key: TlsFixture.key_pem}}
  end

  def https_get(host, port, path)
    http = Net::HTTP.new(host, port)
    http.use_ssl = true
    http.verify_mode = OpenSSL::SSL::VERIFY_NONE
    http.start { |session| session.get(path) }
  end

  it "serves HTTPS" do
    app = ->(_env) { [200, {"content-type" => "text/plain"}, ["secure"]] }

    with_server(app, **tls_opts) do |host, port|
      response = https_get(host, port, "/")

      expect(response.code).to eq("200")
      expect(response.body).to eq("secure")
    end
  end

  it "reports rack.url_scheme as https" do
    app = ->(env) { [200, {"content-type" => "text/plain"}, [env["rack.url_scheme"]]] }

    with_server(app, **tls_opts) do |host, port|
      expect(https_get(host, port, "/").body).to eq("https")
    end
  end

  it "rejects plain HTTP on a TLS port" do
    app = ->(_env) { [200, {}, []] }

    with_server(app, **tls_opts) do |host, port|
      expect do
        Net::HTTP.start(host, port, open_timeout: 1, read_timeout: 1) { |http| http.get("/") }
      end.to raise_error(StandardError) # dropped connection surfaces as EOF/reset/timeout
    end
  end

  it "raises at start for an invalid key" do
    app = ->(_env) { [200, {}, []] }
    server = Kino::Server.new(app, mode: :threaded, tls: {cert: TlsFixture.cert_pem, key: "not a key"})

    expect { server.start }.to raise_error(RuntimeError, /TLS/)
  end

  it "raises when only one of cert/key is given" do
    app = ->(_env) { [200, {}, []] }

    expect { Kino::Server.new(app, mode: :threaded, tls: {cert: TlsFixture.cert_pem}) }
      .to raise_error(ArgumentError, /cert.*key/)
  end
end
