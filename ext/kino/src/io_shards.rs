//! Current-thread Tokio runtimes for HTTP I/O.
//!
//! One accept thread owns the listener and assigns accepted connections to
//! the least-loaded shard. Each shard then owns that connection for its
//! lifetime, avoiding the shared Tokio worker pool on hot HTTP paths.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;

use crate::listen::Listener;
use crate::log::{self, Level};
use crate::registry::{ServerInner, STATE_DRAINING};
use crate::server::{serve_conn, AsyncListener, Conn};

/// A connection in transit from the acceptor to its shard. Tokio streams
/// are bound to the runtime that registered them, so the handoff carries
/// the std stream and the shard re-registers it on arrival.
enum StdConn {
    Tcp(std::net::TcpStream),
    Unix(std::os::unix::net::UnixStream),
}

impl StdConn {
    /// Register with the calling (shard) runtime.
    fn into_tokio(self) -> std::io::Result<Conn> {
        Ok(match self {
            StdConn::Tcp(stream) => Conn::Tcp(tokio::net::TcpStream::from_std(stream)?),
            StdConn::Unix(stream) => Conn::Unix(tokio::net::UnixStream::from_std(stream)?),
        })
    }
}

/// Detach an accepted stream from the acceptor's runtime for the handoff.
fn into_std(conn: Conn) -> std::io::Result<StdConn> {
    Ok(match conn {
        Conn::Tcp(stream) => StdConn::Tcp(stream.into_std()?),
        Conn::Unix(stream) => StdConn::Unix(stream.into_std()?),
    })
}

/// One accepted connection en route to a shard: the detached stream, the
/// addresses hyper reports, and the slot it holds against max_connections.
struct Accepted {
    conn: StdConn,
    remote_addr: SocketAddr,
    local_addr: SocketAddr,
    permit: tokio::sync::OwnedSemaphorePermit,
}

/// Shard count: an explicit `io_threads` wins; the default is half the
/// available CPUs. Framing requests is cheap next to running the app, so
/// the I/O plane gets the smaller share and Ruby workers keep the rest.
pub(crate) fn thread_count(io_threads: usize) -> usize {
    if io_threads > 0 {
        return io_threads;
    }
    default_thread_count(std::thread::available_parallelism().map_or(1, usize::from))
}

fn default_thread_count(cpus: usize) -> usize {
    cpus.div_ceil(2)
}

/// Boot the shard threads, then the acceptor. Any thread that fails to
/// come up fails the whole boot: the already started threads are drained
/// (their senders drop) and joined before the error reaches Ruby.
pub(crate) fn spawn(
    listener: Listener,
    acceptor: Option<tokio_rustls::TlsAcceptor>,
    server: Arc<ServerInner>,
    max_connections: usize,
    accept_shutdown_rx: tokio::sync::watch::Receiver<bool>,
    runtime_shutdown_rx: tokio::sync::watch::Receiver<bool>,
    shard_count: usize,
) -> std::io::Result<Vec<JoinHandle<()>>> {
    let shard_count = shard_count.max(1);
    let mut handles = Vec::with_capacity(shard_count + 1);
    let mut shard_txs = Vec::with_capacity(shard_count);
    let mut loads = Vec::with_capacity(shard_count);

    for i in 0..shard_count {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let load = Arc::new(AtomicUsize::new(0));
        let spawned = spawn_shard(
            i,
            rx,
            acceptor.clone(),
            server.clone(),
            load.clone(),
            runtime_shutdown_rx.clone(),
        );
        match spawned {
            Ok(handle) => {
                shard_txs.push(tx);
                loads.push(load);
                handles.push(handle);
            }
            Err(error) => {
                drop(tx);
                drop(shard_txs);
                join_all(handles);
                return Err(error);
            }
        }
    }

    match spawn_acceptor(listener, server, max_connections, accept_shutdown_rx, shard_txs, loads) {
        Ok(handle) => handles.push(handle),
        Err(error) => {
            join_all(handles);
            return Err(error);
        }
    }
    Ok(handles)
}

fn join_all(handles: Vec<JoinHandle<()>>) {
    for handle in handles {
        let _ = handle.join();
    }
}

fn current_thread_runtime() -> std::io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread().enable_all().build()
}

/// Wait for a just spawned I/O thread to report its runtime up, so a
/// startup failure becomes the boot error Ruby sees instead of a silently
/// dead thread.
fn await_ready(
    handle: JoinHandle<()>,
    ready_rx: std::sync::mpsc::Receiver<std::io::Result<()>>,
    what: &str,
) -> std::io::Result<JoinHandle<()>> {
    match ready_rx.recv() {
        Ok(Ok(())) => Ok(handle),
        Ok(Err(error)) => {
            let _ = handle.join();
            Err(error)
        }
        Err(_) => Err(std::io::Error::other(format!("{what} thread exited during startup"))),
    }
}

fn spawn_shard(
    index: usize,
    rx: tokio::sync::mpsc::UnboundedReceiver<Accepted>,
    acceptor: Option<tokio_rustls::TlsAcceptor>,
    server: Arc<ServerInner>,
    load: Arc<AtomicUsize>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> std::io::Result<JoinHandle<()>> {
    let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
    let handle = std::thread::Builder::new()
        .name(format!("kino-io-{index}"))
        .spawn(move || {
            let runtime = match current_thread_runtime() {
                Ok(runtime) => runtime,
                Err(error) => {
                    let _ = ready_tx.send(Err(error));
                    return;
                }
            };
            let _ = ready_tx.send(Ok(()));
            runtime.block_on(shard_loop(rx, acceptor, server, load, &mut shutdown_rx));
        })?;
    await_ready(handle, ready_rx, "shard")
}

fn spawn_acceptor(
    listener: Listener,
    server: Arc<ServerInner>,
    max_connections: usize,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
    shard_txs: Vec<tokio::sync::mpsc::UnboundedSender<Accepted>>,
    loads: Vec<Arc<AtomicUsize>>,
) -> std::io::Result<JoinHandle<()>> {
    let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
    let handle = std::thread::Builder::new()
        .name("kino-accept".to_string())
        .spawn(move || {
            let runtime = match current_thread_runtime() {
                Ok(runtime) => runtime,
                Err(error) => {
                    let _ = ready_tx.send(Err(error));
                    return;
                }
            };
            runtime.block_on(async move {
                // Registration must happen on this runtime; a failure is
                // routed through the same ready channel as a build error.
                let listener = match AsyncListener::from_std(listener) {
                    Ok(listener) => listener,
                    Err(error) => {
                        let _ = ready_tx.send(Err(error));
                        return;
                    }
                };
                accept_loop(listener, server, max_connections, shutdown_rx, shard_txs, loads, ready_tx)
                    .await;
            });
        })?;
    await_ready(handle, ready_rx, "accept")
}

/// Serve handed-over connections until the acceptor is gone, then let the
/// remaining ones finish. The final teardown signal cuts either phase
/// short: dropping the runtime cancels connection tasks at their next
/// await point.
async fn shard_loop(
    mut rx: tokio::sync::mpsc::UnboundedReceiver<Accepted>,
    acceptor: Option<tokio_rustls::TlsAcceptor>,
    server: Arc<ServerInner>,
    load: Arc<AtomicUsize>,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
) {
    let mut connections = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => return,
            accepted = rx.recv() => {
                let Some(accepted) = accepted else { break };
                let acceptor = acceptor.clone();
                let server = server.clone();
                let guard = LoadGuard(load.clone());
                connections.spawn(async move {
                    let _guard = guard;
                    serve_accepted(accepted, acceptor, server).await;
                });
            }
            // Reap closed connections so the set stays small.
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
        }
    }
    // The acceptor is gone: drain. Every join is one connection closing.
    while !connections.is_empty() {
        tokio::select! {
            _ = shutdown_rx.changed() => return,
            _ = connections.join_next() => {}
        }
    }
}

/// Keeps the shard's connection count honest whichever way the task ends:
/// return, panic, or cancellation at teardown. A plain decrement after the
/// await would never run on the last two.
struct LoadGuard(Arc<AtomicUsize>);

impl Drop for LoadGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

/// The shard's half of the handoff: re-register the stream on this
/// runtime, then run the shared connection pipeline (TLS handshake,
/// protocol layer) exactly as the default runtime would.
async fn serve_accepted(
    accepted: Accepted,
    acceptor: Option<tokio_rustls::TlsAcceptor>,
    server: Arc<ServerInner>,
) {
    // Held for the connection's lifetime; dropping it frees a slot.
    let _permit = accepted.permit;
    let conn = match accepted.conn.into_tokio() {
        Ok(conn) => conn,
        Err(_) => {
            log::emit(Level::Warn, "tokio", "failed to register a stream on an I/O shard");
            return;
        }
    };
    serve_conn(conn, acceptor, server, accepted.remote_addr, accepted.local_addr).await;
}

/// The sharded accept loop. Same backpressure as the default loop: the
/// permit is acquired before accept, so past max_connections the excess
/// waits in the kernel backlog instead of being accepted and dropped.
/// A shard whose channel is gone is marked dead and routed around; with
/// no shard left the loop stops accepting and flips the server to
/// draining, so the control plane stops reporting ready.
async fn accept_loop(
    listener: AsyncListener,
    server: Arc<ServerInner>,
    max_connections: usize,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    shard_txs: Vec<tokio::sync::mpsc::UnboundedSender<Accepted>>,
    loads: Vec<Arc<AtomicUsize>>,
    ready_tx: std::sync::mpsc::SyncSender<std::io::Result<()>>,
) {
    let conn_limit = Arc::new(tokio::sync::Semaphore::new(max_connections));
    let mut live = vec![true; shard_txs.len()];
    let _ = ready_tx.send(Ok(()));
    'accept: loop {
        let permit = tokio::select! {
            _ = shutdown_rx.changed() => break,
            permit = conn_limit.clone().acquire_owned() => match permit {
                Ok(permit) => permit,
                Err(_) => break,
            },
        };
        let (conn, remote_addr, local_addr) = tokio::select! {
            _ = shutdown_rx.changed() => break,
            accepted = listener.accept() => match accepted {
                Ok(accepted) => accepted,
                Err(_) => continue,
            },
        };
        let conn = match into_std(conn) {
            Ok(conn) => conn,
            Err(_) => {
                log::emit(Level::Warn, "tokio", "failed to detach an accepted stream; connection dropped");
                continue;
            }
        };
        let mut accepted = Accepted { conn, remote_addr, local_addr, permit };
        loop {
            let Some(index) = least_loaded(&loads, &live) else {
                log::emit(Level::Error, "tokio", "all I/O shards are down; not accepting connections");
                server.state.store(STATE_DRAINING, Ordering::Relaxed);
                break 'accept;
            };
            loads[index].fetch_add(1, Ordering::Relaxed);
            match shard_txs[index].send(accepted) {
                Ok(()) => break,
                Err(returned) => {
                    loads[index].fetch_sub(1, Ordering::Relaxed);
                    live[index] = false;
                    log::emit(Level::Warn, "tokio", "I/O shard is down; routing around it");
                    accepted = returned.0;
                }
            }
        }
    }
}

/// The live shard with the fewest open connections.
fn least_loaded(loads: &[Arc<AtomicUsize>], live: &[bool]) -> Option<usize> {
    loads
        .iter()
        .enumerate()
        .filter(|(index, _)| live[*index])
        .min_by_key(|(_, load)| load.load(Ordering::Relaxed))
        .map(|(index, _)| index)
}

#[cfg(test)]
mod tests {
    use super::{default_thread_count, least_loaded, thread_count, Arc, AtomicUsize};

    fn loads(counts: &[usize]) -> Vec<Arc<AtomicUsize>> {
        counts.iter().map(|&n| Arc::new(AtomicUsize::new(n))).collect()
    }

    #[test]
    fn explicit_io_threads_win() {
        assert_eq!(thread_count(3), 3);
    }

    #[test]
    fn least_loaded_picks_the_emptiest_live_shard() {
        assert_eq!(least_loaded(&loads(&[3, 0, 1]), &[true, true, true]), Some(1));
    }

    #[test]
    fn least_loaded_routes_around_dead_shards() {
        // The dead shard's count is frozen at 0; it must not win anyway.
        assert_eq!(least_loaded(&loads(&[3, 0, 1]), &[true, false, true]), Some(2));
    }

    #[test]
    fn least_loaded_reports_when_no_shard_is_left() {
        assert_eq!(least_loaded(&loads(&[0, 0]), &[false, false]), None);
    }

    #[test]
    fn default_is_half_the_cpus() {
        assert_eq!(default_thread_count(1), 1);
        assert_eq!(default_thread_count(2), 1);
        assert_eq!(default_thread_count(3), 2);
        assert_eq!(default_thread_count(12), 6);
        assert_eq!(default_thread_count(128), 64);
    }
}
