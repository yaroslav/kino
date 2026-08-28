//! Listening sockets for the main server and the control plane: a TCP
//! `host:port`, or a `unix://path` domain socket (the usual shape behind
//! nginx). Binding is synchronous so an address conflict surfaces at boot.

use std::io;
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};

/// The bind scheme that selects a unix domain socket; anything else is a
/// TCP host.
pub const UNIX_SCHEME: &str = "unix://";

/// The socket path of a `unix://` bind, or None for a TCP host.
pub fn unix_path(bind: &str) -> Option<&Path> {
    bind.strip_prefix(UNIX_SCHEME).map(Path::new)
}

/// A bound, non-blocking listener of either kind.
pub enum Listener {
    Tcp(std::net::TcpListener),
    Unix(UnixListener, PathBuf),
}

impl Listener {
    /// Bind `bind:port` (TCP; a hostname resolves to its addresses and the
    /// first that binds wins) or `unix://path`.
    pub fn bind(bind: &str, port: u16) -> io::Result<Listener> {
        match unix_path(bind) {
            Some(path) => Ok(Listener::Unix(bind_unix(path)?, path.to_path_buf())),
            None => {
                let listener = std::net::TcpListener::bind((bind, port))?;
                listener.set_nonblocking(true)?;
                Ok(Listener::Tcp(listener))
            }
        }
    }

    /// The bound TCP port; 0 for a unix socket, which has none.
    pub fn port(&self) -> io::Result<u16> {
        match self {
            Listener::Tcp(listener) => Ok(listener.local_addr()?.port()),
            Listener::Unix(..) => Ok(0),
        }
    }
}

/// Bind a unix domain socket at `path`. A path that already exists is
/// either a live listener (refuse: never steal it) or a stale file left
/// behind by a crashed process (unlink and reclaim). A connect probe tells
/// them apart: a successful connect means someone is accepting right now.
pub fn bind_unix(path: &Path) -> io::Result<UnixListener> {
    match std::os::unix::net::UnixStream::connect(path) {
        Ok(_) => return Err(io::Error::new(io::ErrorKind::AddrInUse, "socket is in use")),
        Err(e) if e.kind() == io::ErrorKind::ConnectionRefused => {
            match std::fs::remove_file(path) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    let listener = UnixListener::bind(path)?;
    listener.set_nonblocking(true)?;
    Ok(listener)
}

/// Remove a socket file at shutdown; one that is already gone is fine.
pub fn cleanup_unix(path: &Path) {
    let _ = std::fs::remove_file(path);
}

#[cfg(test)]
mod tests {
    use super::{bind_unix, unix_path, Listener};
    use std::path::PathBuf;

    /// A socket path unique to this process and test. macOS caps sun_path
    /// at 104 bytes, so the name stays short.
    fn socket_path(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("kino-{}-{name}.sock", std::process::id()));
        let _ = std::fs::remove_file(&path);
        path
    }

    #[test]
    fn unix_path_recognises_only_the_unix_scheme() {
        assert_eq!(
            unix_path("unix:///run/kino.sock").unwrap().to_str(),
            Some("/run/kino.sock")
        );
        assert!(unix_path("127.0.0.1").is_none());
        assert!(unix_path("unix.example.com").is_none());
    }

    #[test]
    fn binds_a_unix_socket_and_reports_no_port() {
        let path = socket_path("bind");
        let listener = Listener::bind(&format!("unix://{}", path.display()), 9292).unwrap();
        assert!(matches!(listener, Listener::Unix(..)));
        assert_eq!(listener.port().unwrap(), 0);
        assert!(std::fs::metadata(&path).is_ok());
        drop(listener);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn reclaims_a_stale_socket_file() {
        let path = socket_path("stale");
        // Dropping a listener closes the socket but leaves its file behind,
        // exactly what a crashed process leaves.
        drop(bind_unix(&path).unwrap());
        assert!(std::fs::metadata(&path).is_ok());
        let reclaimed = bind_unix(&path).unwrap();
        drop(reclaimed);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn refuses_a_socket_someone_is_listening_on() {
        let path = socket_path("live");
        let live = bind_unix(&path).unwrap();
        let err = bind_unix(&path).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::AddrInUse);
        assert!(err.to_string().contains("in use"));
        drop(live);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn binds_tcp_on_an_ephemeral_port() {
        let listener = Listener::bind("127.0.0.1", 0).unwrap();
        assert!(matches!(listener, Listener::Tcp(_)));
        assert_ne!(listener.port().unwrap(), 0);
    }
}
