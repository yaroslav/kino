# frozen_string_literal: true

require "openssl"

# Self-signed localhost cert, generated once per suite, passed inline as PEM.
module TlsFixture
  module_function

  def cert_pem
    generate unless defined?(@cert_pem)
    @cert_pem
  end

  def key_pem
    generate unless defined?(@key_pem)
    @key_pem
  end

  def generate
    key = OpenSSL::PKey::RSA.new(2048)
    cert = OpenSSL::X509::Certificate.new
    cert.version = 2
    cert.serial = 1
    name = OpenSSL::X509::Name.parse("/CN=localhost")
    cert.subject = name
    cert.issuer = name
    cert.public_key = key.public_key
    cert.not_before = Time.now - 3600
    cert.not_after = Time.now + 24 * 3600

    extensions = OpenSSL::X509::ExtensionFactory.new
    extensions.subject_certificate = cert
    extensions.issuer_certificate = cert
    cert.add_extension(extensions.create_extension("subjectAltName", "DNS:localhost,IP:127.0.0.1"))
    cert.add_extension(extensions.create_extension("basicConstraints", "CA:TRUE", true))

    cert.sign(key, OpenSSL::Digest.new("SHA256"))

    @cert_pem = cert.to_pem
    @key_pem = key.to_pem
  end
end
