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
    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// A TLS server and client for tests: an acceptor for a fixture cert,
/// and a connector that takes any certificate (the fixture is CA-flagged,
/// which webpki refuses as an end-entity cert).
#[cfg(test)]
pub(crate) mod testing {
    use std::sync::Arc;

    use tokio_rustls::rustls::client::danger::{
        HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
    };
    use tokio_rustls::rustls::crypto::WebPkiSupportedAlgorithms;
    use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName, UnixTime};
    use tokio_rustls::rustls::{ClientConfig, DigitallySignedStruct, Error, SignatureScheme};
    use tokio_rustls::{TlsAcceptor, TlsConnector};

    pub const CERT: &str = "-----BEGIN CERTIFICATE-----
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

    pub const KEY: &str = "-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgSlcfB39H6lv5IqYe
Q+daLGVt9Bf/i5bf+OnLUKcaZYahRANCAAQCgKc2l0PwJW5CAWzp8uW8iIIaGaPk
WJ2lRijROyX9v7f8aSQlb6kEwKhI8kG8SbeUc+zbKkzGgRXNaZHY/mAa
-----END PRIVATE KEY-----";

    /// An acceptor for the fixture cert, offering h2 when `http2` is.
    pub fn acceptor(http2: bool) -> TlsAcceptor {
        super::build_acceptor(CERT, KEY, http2).expect("valid PEM")
    }

    /// A connector that offers `alpn` and trusts any server.
    pub fn connector(alpn: &[&[u8]]) -> TlsConnector {
        let provider = tokio_rustls::rustls::crypto::ring::default_provider();
        let verifier = AcceptAny(provider.signature_verification_algorithms);
        let mut client = ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(verifier))
            .with_no_client_auth();
        client.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
        TlsConnector::from(Arc::new(client))
    }

    /// The fixture's name.
    pub fn localhost() -> ServerName<'static> {
        ServerName::try_from("localhost").expect("server name")
    }

    #[derive(Debug)]
    struct AcceptAny(WebPkiSupportedAlgorithms);

    impl ServerCertVerifier for AcceptAny {
        fn verify_server_cert(
            &self,
            _end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &ServerName<'_>,
            _ocsp_response: &[u8],
            _now: UnixTime,
        ) -> Result<ServerCertVerified, Error> {
            Ok(ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, Error> {
            Ok(HandshakeSignatureValid::assertion())
        }

        fn verify_tls13_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, Error> {
            Ok(HandshakeSignatureValid::assertion())
        }

        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            self.0.supported_schemes()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::build_acceptor;
    use super::testing::{acceptor, connector, localhost, CERT, KEY};

    #[test]
    fn pem_that_cannot_make_an_acceptor_is_refused_saying_why() {
        let garbage = "-----BEGIN PRIVATE KEY-----\ngarbage\n-----END PRIVATE KEY-----";
        #[rustfmt::skip] // One row per refused cert and key.
        let refused = [
            ("/nonexistent/cert.pem", "/nonexistent/key.pem", "cannot read"),
            (KEY, KEY, "no certificates found"),
            (CERT, CERT, "no private key found"),
            (CERT, garbage, "TLS config rejected"),
        ];
        for (cert, key, says) in refused {
            let error = build_acceptor(cert, key, true).err();
            let error = error.unwrap_or_else(|| panic!("{says}: built"));
            assert!(error.contains(says), "{says}: {error}");
        }
    }

    /// A real rustls handshake over an in-memory pipe, its client offering HTTP/1.1 first: the
    /// server's order chooses, so h2 while http2 is on. The client skips certificate
    /// verification: ALPN is under test here, not the trust chain.
    #[tokio::test]
    async fn the_handshake_chooses_h2_over_http11_only_while_http2_is_on() {
        for (http2, chosen) in [(true, &b"h2"[..]), (false, &b"http/1.1"[..])] {
            let acceptor = acceptor(http2);
            let (client_io, server_io) = tokio::io::duplex(16 * 1024);
            let server = tokio::spawn(async move { acceptor.accept(server_io).await });
            let client = connector(&[b"http/1.1", b"h2"])
                .connect(localhost(), client_io)
                .await
                .expect("client handshake");
            let server = server.await.expect("join").expect("server handshake");
            assert_eq!(client.get_ref().1.alpn_protocol(), Some(chosen), "{http2}");
            assert_eq!(server.get_ref().1.alpn_protocol(), Some(chosen), "{http2}");
        }
    }
}
