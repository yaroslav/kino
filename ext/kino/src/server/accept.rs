//! Accepting connections: the listeners, what a connection holds, and the accept loop's backoff.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream, UnixListener, UnixStream};
use tokio_util::either::Either;

use super::serve_conn;
use crate::counters::Counted;
use crate::listen::Listener;
use crate::registry::ServerInner;

#[derive(Clone, Copy)]
pub(crate) struct Peer {
    pub remote: SocketAddr,
    pub local: SocketAddr,
}

/// A unix socket's peer is local (127.0.0.1) and has no port, so SERVER_PORT falls back
/// to 80 when the Host header names none.
pub(super) const UNIX_PEER: Peer = Peer {
    remote: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
    local: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 80),
};

/// TCP (optionally behind TLS), or a unix socket for plain HTTP.
pub(crate) enum AsyncListener {
    Tcp(TcpListener),
    Unix(UnixListener),
}

/// An accepted stream, before the protocol layer sees it.
pub(crate) type Conn = Either<TcpStream, UnixStream>;

pub(crate) struct Accepted<C = Conn> {
    pub conn: C,
    pub peer: Peer,
    pub held: Held,
}

impl AsyncListener {
    pub(crate) fn from_std(listener: Listener) -> std::io::Result<AsyncListener> {
        Ok(match listener {
            Listener::Tcp(listener) => AsyncListener::Tcp(TcpListener::from_std(listener)?),
            Listener::Unix(listener, _) => AsyncListener::Unix(UnixListener::from_std(listener)?),
        })
    }

    async fn accept(&self) -> std::io::Result<(Conn, Peer)> {
        match self {
            AsyncListener::Tcp(listener) => {
                let (stream, remote) = listener.accept().await?;
                // Small responses must not wait on Nagle + delayed ACK.
                let _ = stream.set_nodelay(true);
                let local = stream
                    .local_addr()
                    .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 0)));
                Ok((Either::Left(stream), Peer { remote, local }))
            }
            AsyncListener::Unix(listener) => {
                let (stream, _) = listener.accept().await?;
                Ok((Either::Right(stream), UNIX_PEER))
            }
        }
    }
}

/// Its max_connections permit and, on an I/O shard, its count in the shard's load; an
/// upgraded connection keeps both after hyper lets go of it.
pub(crate) struct Held {
    _permit: tokio::sync::OwnedSemaphorePermit,
    pub(crate) shard_load: Option<Counted>,
}

impl Held {
    pub(crate) fn new(
        permit: tokio::sync::OwnedSemaphorePermit,
        shard_load: Option<Counted>,
    ) -> Self {
        Held {
            _permit: permit,
            shard_load,
        }
    }
}

/// The stream with what it holds, through hyper and past an upgrade: `prefix`, read off it before
/// hyper, is read first.
pub(super) struct ConnIo<I> {
    io: I,
    _held: Held,
    prefix: Bytes,
}

impl<I> ConnIo<I> {
    pub(super) fn new(io: I, held: Held, prefix: Bytes) -> Self {
        ConnIo {
            io,
            _held: held,
            prefix,
        }
    }

    /// Only a connection whose first bytes were read before hyper (sniffed after a TLS handshake)
    /// has a prefix, read first; later reads pay one branch.
    #[cold]
    fn read_prefix(&mut self, buf: &mut tokio::io::ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        let n = self.prefix.len().min(buf.remaining());
        buf.put_slice(&self.prefix.split_to(n));
        Poll::Ready(Ok(()))
    }
}

impl<I: AsyncRead + Unpin> AsyncRead for ConnIo<I> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        if !this.prefix.is_empty() {
            return this.read_prefix(buf);
        }
        Pin::new(&mut this.io).poll_read(cx, buf)
    }
}

impl<I: AsyncWrite + Unpin> AsyncWrite for ConnIo<I> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().io).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().io).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_shutdown(cx)
    }
}

/// The wait after an accept that failed for want of a resource.
const ACCEPT_BACKOFF: Duration = Duration::from_millis(10);

/// A connection that failed on its own (aborted, reset, unreachable, or EPROTO, which Linux
/// reports from accept) or an interrupted call frees the next accept. Anything else (EMFILE,
/// ENFILE, out of memory) would fail again at once and spin a runtime thread: wait first.
async fn accept_failed(error: &std::io::Error) {
    use std::io::ErrorKind::{
        ConnectionAborted, ConnectionReset, HostUnreachable, Interrupted, NetworkDown,
        NetworkUnreachable,
    };
    let consumed = matches!(
        error.kind(),
        ConnectionAborted
            | ConnectionReset
            | Interrupted
            | NetworkDown
            | NetworkUnreachable
            | HostUnreachable
    ) || error.raw_os_error() == Some(libc::EPROTO);
    if !consumed {
        tokio::time::sleep(ACCEPT_BACKOFF).await;
    }
}

/// The next connection with its `conn_limit` permit; None once the drain began. The permit
/// comes first, so a flood waits in the kernel backlog instead of holding a task and a socket.
pub(crate) async fn next_accept(
    listener: &AsyncListener,
    conn_limit: &Arc<tokio::sync::Semaphore>,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
) -> Option<Accepted> {
    loop {
        let permit = tokio::select! {
            _ = shutdown_rx.changed() => return None,
            permit = conn_limit.clone().acquire_owned() => permit.ok()?, // closed
        };
        tokio::select! {
            _ = shutdown_rx.changed() => return None,
            accepted = listener.accept() => match accepted {
                Ok((conn, peer)) => {
                    let held = Held::new(permit, None);
                    return Some(Accepted { conn, peer, held });
                }
                Err(error) => {
                    drop(permit);
                    accept_failed(&error).await;
                }
            },
        }
    }
}

pub(super) async fn accept_loop(
    listener: AsyncListener,
    acceptor: Option<tokio_rustls::TlsAcceptor>,
    server: Arc<ServerInner>,
    max_connections: usize,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    let conn_limit = Arc::new(tokio::sync::Semaphore::new(max_connections));
    while let Some(accepted) = next_accept(&listener, &conn_limit, &mut shutdown_rx).await {
        tokio::spawn(serve_conn(accepted, acceptor.clone(), server.clone()));
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Error, ErrorKind};

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn a_failed_accept_waits_before_the_next_unless_only_its_connection_failed() {
        let waited = async |error: Error| {
            let started = tokio::time::Instant::now();
            accept_failed(&error).await;
            started.elapsed()
        };
        // EMFILE and ENFILE, the same numbers on Linux and macOS.
        for errno in [24, 23] {
            let out_of_descriptors = Error::from_raw_os_error(errno);
            assert!(
                waited(out_of_descriptors).await >= ACCEPT_BACKOFF,
                "errno {errno}"
            );
        }
        let consumed = [
            ErrorKind::ConnectionAborted,
            ErrorKind::ConnectionReset,
            ErrorKind::Interrupted,
            ErrorKind::NetworkDown,
            ErrorKind::NetworkUnreachable,
            ErrorKind::HostUnreachable,
        ];
        for error in consumed
            .map(Error::from)
            .into_iter()
            .chain([Error::from_raw_os_error(libc::EPROTO)])
        {
            let what = format!("{error:?}");
            assert_eq!(
                waited(error).await,
                Duration::ZERO,
                "{what}: the next may go at once"
            );
        }
    }
}
