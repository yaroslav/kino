//! Listening sockets for the main server and the control plane: a TCP
//! `host:port`, or a `unix://path` domain socket (the usual shape behind
//! nginx). Binding is synchronous so an address conflict surfaces at boot.

use std::io;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd};
use std::os::unix::fs::FileTypeExt;
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};

/// The bind scheme that selects a unix domain socket; anything else is a
/// TCP host.
pub const UNIX_SCHEME: &str = "unix://";

/// The socket path of a `unix://` bind, or None for a TCP host.
pub fn unix_path(bind: &str) -> Option<&Path> {
    bind.strip_prefix(UNIX_SCHEME).map(Path::new)
}

/// The client listener's accept queue: as long as the kernel allows (its somaxconn). At std's
/// 128, Linux drops the SYNs of a connect burst past it, and those clients retry a second later.
const CLIENT_BACKLOG: libc::c_int = libc::c_int::MAX;

/// A bound, non-blocking listener of either kind.
pub enum Listener {
    Tcp(std::net::TcpListener),
    Unix(UnixListener, PathBuf),
}

impl Listener {
    /// Bind `bind:port` (TCP; a hostname resolves to its addresses and the
    /// first that binds wins) or `unix://path`, with CLIENT_BACKLOG.
    pub fn bind(bind: &str, port: u16) -> io::Result<Listener> {
        let listener = match unix_path(bind) {
            Some(path) => Listener::Unix(bind_unix(path)?, path.to_path_buf()),
            None => {
                let listener = std::net::TcpListener::bind((bind, port))?;
                listener.set_nonblocking(true)?;
                Listener::Tcp(listener)
            }
        };
        // Failing that, the listener keeps std's queue: no reason to refuse the boot.
        let _ = set_backlog(&listener, CLIENT_BACKLOG);
        Ok(listener)
    }

    /// The bound TCP port; 0 for a unix socket, which has none.
    pub fn port(&self) -> io::Result<u16> {
        match self {
            Listener::Tcp(listener) => Ok(listener.local_addr()?.port()),
            Listener::Unix(..) => Ok(0),
        }
    }
}

impl AsFd for Listener {
    fn as_fd(&self) -> BorrowedFd<'_> {
        match self {
            Listener::Tcp(listener) => listener.as_fd(),
            Listener::Unix(listener, _) => listener.as_fd(),
        }
    }
}

/// listen(2) again on a listening socket: only its queue changes.
pub fn set_backlog(socket: &impl AsFd, backlog: libc::c_int) -> io::Result<()> {
    // SAFETY: a BorrowedFd of `socket`, open for the call; listen(2) reads no memory of ours.
    if unsafe { libc::listen(socket.as_fd().as_raw_fd(), backlog) } == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Bind a unix domain socket at `path`. A socket file already there is a live listener's
/// (refused: never taken) or a stale one a crashed process left (unlinked and reclaimed), which
/// `listening` tells apart. Anything else there is a mistyped path: refused, and never unlinked.
/// Nothing locks the path, so two boots at the same instant on a stale one may both bind it,
/// leaving the first with a socket no path names.
pub fn bind_unix(path: &Path) -> io::Result<UnixListener> {
    match std::fs::symlink_metadata(path) {
        Ok(found) if found.file_type().is_socket() => {
            if listening(path)? {
                return Err(io::Error::new(io::ErrorKind::AddrInUse, "socket is in use"));
            }
            match std::fs::remove_file(path) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("{}: exists and is not a socket", path.display()),
            ))
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    let listener = UnixListener::bind(path)?;
    listener.set_nonblocking(true)?;
    Ok(listener)
}

/// Whether anything listens at `path`, by a connect that never waits. A listener whose accept
/// queue is full answers EAGAIN on Linux, which is in use, where a blocking connect would wait
/// for room; macOS refuses that connect as it refuses one to a stale file, so there a live
/// server with a full queue reads as stale.
fn listening(path: &Path) -> io::Result<bool> {
    match connect_nonblocking(path) {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok(true),
        Err(e) if e.kind() == io::ErrorKind::ConnectionRefused => Ok(false),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}

fn connect_nonblocking(path: &Path) -> io::Result<socket2::Socket> {
    let socket = socket2::Socket::new(socket2::Domain::UNIX, socket2::Type::STREAM, None)?;
    socket.set_nonblocking(true)?;
    socket.connect(&socket2::SockAddr::unix(path)?)?;
    Ok(socket)
}

/// Remove a socket file at shutdown; one that is already gone is fine.
pub fn cleanup_unix(path: &Path) {
    let _ = std::fs::remove_file(path);
}

#[cfg(test)]
mod tests {
    use super::{bind_unix, unix_path, Listener};
    use std::path::PathBuf;
    #[cfg(target_os = "linux")]
    use {
        super::{connect_nonblocking, set_backlog},
        std::io,
        std::time::Duration,
    };

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

    /// On Linux a connect to a full accept queue fails with EAGAIN, which reads as in use, and
    /// a blocking one would wait for room: the bind returns within two seconds, on a thread of
    /// its own. (macOS refuses that connect as it does one to a stale file: a documented limit.)
    #[cfg(target_os = "linux")]
    #[test]
    fn refuses_at_once_a_socket_whose_live_listener_has_a_full_queue_and_leaves_it_its_path() {
        let path = socket_path("full");
        let live = bind_unix(&path).unwrap();
        set_backlog(&live, 8).unwrap();
        // Every client stays open and unaccepted, until the kernel turns one away.
        let mut clients = Vec::new();
        loop {
            match connect_nonblocking(&path) {
                Ok(client) => clients.push(client),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => panic!("filling the accept queue: {e}"),
            }
            assert!(clients.len() <= 32, "a queue of 8 never filled");
        }

        let (tx, rx) = std::sync::mpsc::channel();
        let second = path.clone();
        std::thread::spawn(move || tx.send(bind_unix(&second)));
        let refused = rx
            .recv_timeout(Duration::from_secs(2))
            .expect("bind_unix still waits after 2 s: it should never wait on a listener");
        assert_eq!(refused.unwrap_err().kind(), io::ErrorKind::AddrInUse);

        // Its queue drained, a new client still reaches the live listener at the path.
        drop(clients);
        live.set_nonblocking(true).unwrap();
        while live.accept().is_ok() {}
        let _client = std::os::unix::net::UnixStream::connect(&path).unwrap();
        assert!(live.accept().is_ok(), "the live server lost its path");
        drop(live);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn refuses_a_path_that_holds_anything_but_a_socket_and_leaves_it_there() {
        let path = socket_path("notsock");
        std::fs::write(&path, "an app's data").unwrap();
        let refused = bind_unix(&path).unwrap_err().to_string();
        let named = refused.contains(&path.display().to_string());
        assert!(named && refused.contains("not a socket"), "{refused}");
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "an app's data");
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn binds_tcp_on_an_ephemeral_port() {
        let listener = Listener::bind("127.0.0.1", 0).unwrap();
        assert!(matches!(listener, Listener::Tcp(_)));
        assert_ne!(listener.port().unwrap(), 0);
    }

    /// Linux keeps a finished handshake in a listener's accept queue until
    /// the server accepts it, backlog + 1 of them at most. Past that it
    /// fails a nonblocking unix connect with EAGAIN and drops a TCP SYN, and
    /// every retry of it while the queue stays full (Linux 4.10 on). macOS
    /// caps every backlog at 128 by default, so there is nothing to tell
    /// apart there: Linux only. Skipped, saying so, where somaxconn is not
    /// above the burst.
    #[cfg(target_os = "linux")]
    #[test]
    fn the_client_listener_queues_as_many_as_the_kernel_allows() {
        use std::net::{SocketAddr, TcpStream};
        use std::os::fd::OwnedFd;

        // Clients connected one after another: well past 128, and within
        // the kernel's default somaxconn.
        const BURST: usize = 300;
        let somaxconn = std::fs::read_to_string("/proc/sys/net/core/somaxconn");
        let somaxconn: Option<usize> = somaxconn.ok().and_then(|s| s.trim().parse().ok());
        if !somaxconn.is_some_and(|n| n > BURST) {
            eprintln!("skipped: net.core.somaxconn is {somaxconn:?}, not above {BURST}");
            return;
        }
        let path = socket_path("queue");
        let tcp = Listener::bind("127.0.0.1", 0).unwrap();
        let unix = Listener::bind(&format!("unix://{}", path.display()), 0).unwrap();
        let port = tcp.port().unwrap();
        for unix_socket in [false, true] {
            // A TCP connect a full queue holds back never finishes, as
            // nothing accepts: its SYN and the retries are all dropped; a
            // nonblocking unix one fails at once.
            let connect = || -> io::Result<OwnedFd> {
                if unix_socket {
                    return connect_nonblocking(&path).map(OwnedFd::from);
                }
                let addr = SocketAddr::from(([127, 0, 0, 1], port));
                TcpStream::connect_timeout(&addr, Duration::from_secs(2)).map(OwnedFd::from)
            };
            let mut clients = Vec::with_capacity(BURST);
            let refused = loop {
                match connect() {
                    Ok(client) if clients.len() + 1 < BURST => clients.push(client),
                    Ok(_) => break None,
                    Err(e) => break Some(e),
                }
            };
            assert!(
                refused.is_none(),
                "unix {unix_socket}: the client listener held {} handshakes, then {refused:?}: \
                 its backlog should be net.core.somaxconn, not 128",
                clients.len()
            );
        }
        drop(unix);
        let _ = std::fs::remove_file(&path);
    }
}
