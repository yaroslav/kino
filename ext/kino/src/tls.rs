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

pub fn build_acceptor(cert: &str, key: &str) -> Result<TlsAcceptor, String> {
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
    config.alpn_protocols = vec![b"http/1.1".to_vec()];

    Ok(TlsAcceptor::from(Arc::new(config)))
}

#[cfg(test)]
mod tests {
    use super::build_acceptor;

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
        assert!(build_acceptor(CERT, KEY).is_ok());
    }

    #[test]
    fn missing_file_paths_error_without_panicking() {
        let err = build_acceptor("/nonexistent/cert.pem", "/nonexistent/key.pem")
            .err().expect("missing files");
        assert!(err.contains("cannot read"));
    }

    #[test]
    fn pem_without_certificates_is_rejected() {
        let err = build_acceptor(KEY, KEY).err().expect("a key is not a cert");
        assert!(err.contains("no certificates found"));
    }

    #[test]
    fn pem_without_a_key_is_rejected() {
        let err = build_acceptor(CERT, CERT).err().expect("a cert is not a key");
        assert!(err.contains("no private key found"));
    }

    #[test]
    fn mismatched_cert_and_garbage_key_are_rejected() {
        let err = build_acceptor(CERT, "-----BEGIN PRIVATE KEY-----\ngarbage\n-----END PRIVATE KEY-----")
            .err().expect("garbage key");
        assert!(!err.is_empty());
    }
}
