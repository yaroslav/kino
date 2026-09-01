//! rustls acceptor construction. Cert/key inputs are either file paths or
//! inline PEM (detected by the `-----BEGIN` marker), so test fixtures never
//! need temp files.

use std::io::BufReader;
use std::sync::Arc;

use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::TlsAcceptor;

fn pem_bytes(input: &str) -> Result<Vec<u8>, String> {
    if input.contains("-----BEGIN") {
        Ok(input.as_bytes().to_vec())
    } else {
        std::fs::read(input).map_err(|e| format!("cannot read {input}: {e}"))
    }
}

pub fn build_acceptor(cert: &str, key: &str, http2: bool) -> Result<TlsAcceptor, String> {
    Ok(TlsAcceptor::from(Arc::new(server_config(
        cert, key, http2,
    )?)))
}

fn server_config(cert: &str, key: &str, http2: bool) -> Result<ServerConfig, String> {
    let cert_pem = pem_bytes(cert)?;
    let key_pem = pem_bytes(key)?;

    let certs: Vec<CertificateDer> = rustls_pemfile::certs(&mut BufReader::new(&cert_pem[..]))
        .collect::<Result<_, _>>()
        .map_err(|e| format!("invalid certificate PEM: {e}"))?;
    if certs.is_empty() {
        return Err("no certificates found in PEM".to_string());
    }

    let key: PrivateKeyDer = rustls_pemfile::private_key(&mut BufReader::new(&key_pem[..]))
        .map_err(|e| format!("invalid private key PEM: {e}"))?
        .ok_or_else(|| "no private key found in PEM".to_string())?;

    let mut config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| format!("TLS config rejected: {e}"))?;
    // Preference order matters: h2 first, so an HTTP/2-capable client
    // negotiates it; a client offering only http/1.1 still matches.
    config.alpn_protocols = if http2 {
        vec![b"h2".to_vec(), b"http/1.1".to_vec()]
    } else {
        vec![b"http/1.1".to_vec()]
    };
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::{build_acceptor, server_config};

    const CERT: &str = "-----BEGIN CERTIFICATE-----
MIIBfjCCASWgAwIBAgIUTs9+cVJSzJjy4TSi9YLEd+i4KiIwCgYIKoZIzj0EAwIw
FDESMBAGA1UEAwwJbG9jYWxob3N0MCAXDTI2MDYxMDE3MjUzNVoYDzIxMjYwNTE3
MTcyNTM1WjAUMRIwEAYDVQQDDAlsb2NhbGhvc3QwWTATBgcqhkjOPQIBBggqhkjO
PQMBBwNCAAQCgKc2l0PwJW5CAWzp8uW8iIIaGaPkWJ2lRijROyX9v7f8aSQlb6kE
wKhI8kG8SbeUc+zbKkzGgRXNaZHY/mAao1MwUTAdBgNVHQ4EFgQUsHZa6iIl6Xho
+EWK6t2Fy9sWAnYwHwYDVR0jBBgwFoAUsHZa6iIl6Xho+EWK6t2Fy9sWAnYwDwYD
VR0TAQH/BAUwAwEB/zAKBggqhkjOPQQDAgNHADBEAiBZM27gueoioJ9YPb+310NI
vdrY8C5A0QufP/Y1Bm0OrgIgQz+pJX47iTdoINM49gX/6ekLZgmfjwzilJK37E4z
gw8=
-----END CERTIFICATE-----";

    const KEY: &str = "-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgSlcfB39H6lv5IqYe
Q+daLGVt9Bf/i5bf+OnLUKcaZYahRANCAAQCgKc2l0PwJW5CAWzp8uW8iIIaGaPk
WJ2lRijROyX9v7f8aSQlb6kEwKhI8kG8SbeUc+zbKkzGgRXNaZHY/mAa
-----END PRIVATE KEY-----";

    #[test]
    fn inline_pem_builds_an_acceptor() {
        assert!(build_acceptor(CERT, KEY, true).is_ok());
    }

    #[test]
    fn missing_file_paths_error_without_panicking() {
        let err = build_acceptor("/nonexistent/cert.pem", "/nonexistent/key.pem", true)
            .err()
            .expect("missing files");
        assert!(err.contains("cannot read"));
    }

    #[test]
    fn pem_without_certificates_is_rejected() {
        let err = build_acceptor(KEY, KEY, true)
            .err()
            .expect("a key is not a cert");
        assert!(err.contains("no certificates found"));
    }

    #[test]
    fn pem_without_a_key_is_rejected() {
        let err = build_acceptor(CERT, CERT, true)
            .err()
            .expect("a cert is not a key");
        assert!(err.contains("no private key found"));
    }

    #[test]
    fn mismatched_cert_and_garbage_key_are_rejected() {
        let err = build_acceptor(
            CERT,
            "-----BEGIN PRIVATE KEY-----\ngarbage\n-----END PRIVATE KEY-----",
            true,
        )
        .err()
        .expect("garbage key");
        assert!(!err.is_empty());
    }

    #[test]
    fn alpn_advertises_h2_first_when_http2_is_on() {
        let config = server_config(CERT, KEY, true).expect("valid PEM");
        assert_eq!(
            config.alpn_protocols,
            vec![b"h2".to_vec(), b"http/1.1".to_vec()]
        );
    }

    #[test]
    fn alpn_advertises_only_http11_when_http2_is_off() {
        let config = server_config(CERT, KEY, false).expect("valid PEM");
        assert_eq!(config.alpn_protocols, vec![b"http/1.1".to_vec()]);
    }

    /// A real rustls handshake over an in-memory pipe: an h2-capable
    /// client must land on "h2", proving the advertisement is not just a
    /// config field but what the wire negotiates. The client skips
    /// certificate verification (the fixture cert is CA-flagged, which
    /// webpki refuses as an end-entity cert): ALPN is under test here,
    /// not the trust chain.
    #[tokio::test]
    async fn handshake_negotiates_h2_with_a_capable_client() {
        use std::sync::Arc;

        use tokio_rustls::rustls::client::danger::{
            HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
        };
        use tokio_rustls::rustls::crypto::WebPkiSupportedAlgorithms;
        use tokio_rustls::rustls::pki_types::ServerName;
        use tokio_rustls::rustls::ClientConfig;
        use tokio_rustls::{TlsAcceptor, TlsConnector};

        #[derive(Debug)]
        struct AcceptAny(WebPkiSupportedAlgorithms);

        impl ServerCertVerifier for AcceptAny {
            fn verify_server_cert(
                &self,
                _end_entity: &tokio_rustls::rustls::pki_types::CertificateDer<'_>,
                _intermediates: &[tokio_rustls::rustls::pki_types::CertificateDer<'_>],
                _server_name: &ServerName<'_>,
                _ocsp_response: &[u8],
                _now: tokio_rustls::rustls::pki_types::UnixTime,
            ) -> Result<ServerCertVerified, tokio_rustls::rustls::Error> {
                Ok(ServerCertVerified::assertion())
            }

            fn verify_tls12_signature(
                &self,
                _message: &[u8],
                _cert: &tokio_rustls::rustls::pki_types::CertificateDer<'_>,
                _dss: &tokio_rustls::rustls::DigitallySignedStruct,
            ) -> Result<HandshakeSignatureValid, tokio_rustls::rustls::Error> {
                Ok(HandshakeSignatureValid::assertion())
            }

            fn verify_tls13_signature(
                &self,
                _message: &[u8],
                _cert: &tokio_rustls::rustls::pki_types::CertificateDer<'_>,
                _dss: &tokio_rustls::rustls::DigitallySignedStruct,
            ) -> Result<HandshakeSignatureValid, tokio_rustls::rustls::Error> {
                Ok(HandshakeSignatureValid::assertion())
            }

            fn supported_verify_schemes(&self) -> Vec<tokio_rustls::rustls::SignatureScheme> {
                self.0.supported_schemes()
            }
        }

        let acceptor =
            TlsAcceptor::from(Arc::new(server_config(CERT, KEY, true).expect("valid PEM")));

        let provider = tokio_rustls::rustls::crypto::ring::default_provider();
        let verifier = AcceptAny(provider.signature_verification_algorithms);
        let mut client = ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(verifier))
            .with_no_client_auth();
        client.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        let connector = TlsConnector::from(Arc::new(client));

        let (client_io, server_io) = tokio::io::duplex(16 * 1024);
        let server = tokio::spawn(async move { acceptor.accept(server_io).await });
        let domain = ServerName::try_from("localhost").expect("server name");
        let client_stream = connector
            .connect(domain, client_io)
            .await
            .expect("client handshake");
        let server_stream = server.await.expect("join").expect("server handshake");

        assert_eq!(client_stream.get_ref().1.alpn_protocol(), Some(&b"h2"[..]));
        assert_eq!(server_stream.get_ref().1.alpn_protocol(), Some(&b"h2"[..]));
    }
}
