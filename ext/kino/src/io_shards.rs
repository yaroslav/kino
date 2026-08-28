//! Current-thread Tokio runtimes for HTTP I/O.
//!
//! One accept thread owns the listener and assigns accepted connections to
//! the least-loaded shard. Each shard then owns that connection for its
//! lifetime, avoiding the shared Tokio worker pool on hot HTTP paths.

use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use crate::listen::Listener;
use crate::log::{self, Level};
use crate::registry::{ServerInner, STATE_DRAINING};
use crate::server::{serve_conn, AsyncListener, Conn};

enum StdConn {
    Tcp(std::net::TcpStream),
    Unix(std::os::unix::net::UnixStream),
}

struct Accepted {
    conn: StdConn,
    remote_addr: SocketAddr,
    local_addr: SocketAddr,
    permit: tokio::sync::OwnedSemaphorePermit,
}

pub(crate) fn thread_count(io_threads: usize, _tokio_threads: usize) -> usize {
    if io_threads > 0 {
        return io_threads;
    }
    let cpus = std::thread::available_parallelism().map_or(1, usize::from);
    default_thread_count(cpus)
}

fn default_thread_count(cpus: usize) -> usize {
    cpus.div_ceil(2).max(1)
}

pub(crate) fn spawn(
    listener: Listener,
    acceptor: Option<tokio_rustls::TlsAcceptor>,
    server: Arc<ServerInner>,
    max_connections: usize,
    accept_shutdown_rx: tokio::sync::watch::Receiver<bool>,
    runtime_shutdown_rx: tokio::sync::watch::Receiver<bool>,
    shard_count: usize,
) -> std::io::Result<Vec<std::thread::JoinHandle<()>>> {
    let shard_count = shard_count.max(1);
    let mut handles = Vec::with_capacity(shard_count + 1);
    let mut shard_txs = Vec::with_capacity(shard_count);
    let mut loads = Vec::with_capacity(shard_count);

    for i in 0..shard_count {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let load = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let handle = match spawn_shard(
            i,
            rx,
            acceptor.clone(),
            server.clone(),
            load.clone(),
            runtime_shutdown_rx.clone(),
        ) {
            Ok(handle) => handle,
            Err(error) => {
                drop(shard_txs);
                join_started(handles);
                return Err(error);
            }
        };
        shard_txs.push(tx);
        loads.push(load);
        handles.push(handle);
    }

    let acceptor = match spawn_acceptor(
        listener,
        server,
        max_connections,
        accept_shutdown_rx,
        shard_txs,
        loads,
    ) {
        Ok(handle) => handle,
        Err(error) => {
            join_started(handles);
            return Err(error);
        }
    };
    handles.push(acceptor);
    Ok(handles)
}

fn join_started(handles: Vec<std::thread::JoinHandle<()>>) {
    for handle in handles {
        let _ = handle.join();
    }
}

fn spawn_shard(
    index: usize,
    rx: tokio::sync::mpsc::UnboundedReceiver<Accepted>,
    acceptor: Option<tokio_rustls::TlsAcceptor>,
    server: Arc<ServerInner>,
    load: Arc<std::sync::atomic::AtomicUsize>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> std::io::Result<std::thread::JoinHandle<()>> {
    let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
    let handle = std::thread::Builder::new()
        .name(format!("kino-io-{index}"))
        .spawn(move || {
            let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            else {
                let _ = ready_tx.send(Err(std::io::Error::other("shard runtime failed")));
                return;
            };
            let _ = ready_tx.send(Ok(()));
            runtime.block_on(shard_loop(rx, acceptor, server, load, &mut shutdown_rx));
        })?;
    match ready_rx.recv() {
        Ok(Ok(())) => Ok(handle),
        Ok(Err(error)) => {
            let _ = handle.join();
            Err(error)
        }
        Err(_) => Err(std::io::Error::other("shard thread exited during startup")),
    }
}

fn spawn_acceptor(
    listener: Listener,
    server: Arc<ServerInner>,
    max_connections: usize,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
    shard_txs: Vec<tokio::sync::mpsc::UnboundedSender<Accepted>>,
    loads: Vec<Arc<std::sync::atomic::AtomicUsize>>,
) -> std::io::Result<std::thread::JoinHandle<()>> {
    let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
    let handle = std::thread::Builder::new()
        .name("kino-accept".to_string())
        .spawn(move || {
            let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            else {
                let _ = ready_tx.send(Err(std::io::Error::other("accept runtime failed")));
                return;
            };
            runtime.block_on(async move {
                let listener = match AsyncListener::from_std(listener) {
                    Ok(listener) => listener,
                    Err(error) => {
                        let _ = ready_tx.send(Err(error));
                        return;
                    }
                };
                accept_loop(
                    listener,
                    server,
                    max_connections,
                    shutdown_rx,
                    shard_txs,
                    loads,
                    ready_tx,
                )
                .await;
            });
        })?;
    match ready_rx.recv() {
        Ok(Ok(())) => Ok(handle),
        Ok(Err(error)) => {
            let _ = handle.join();
            Err(error)
        }
        Err(_) => Err(std::io::Error::other("accept thread exited during startup")),
    }
}

async fn shard_loop(
    mut rx: tokio::sync::mpsc::UnboundedReceiver<Accepted>,
    acceptor: Option<tokio_rustls::TlsAcceptor>,
    server: Arc<ServerInner>,
    load: Arc<std::sync::atomic::AtomicUsize>,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
) {
    let mut accepting = true;
    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => break,
            accepted = rx.recv(), if accepting => {
                let Some(accepted) = accepted else {
                    accepting = false;
                    if load.load(Ordering::Relaxed) == 0 {
                        break;
                    }
                    continue;
                };
                let acceptor = acceptor.clone();
                let server = server.clone();
                let load_guard = LoadGuard(load.clone());
                tokio::spawn(async move {
                    serve_accepted(accepted, acceptor, server).await;
                    drop(load_guard);
                });
            }
            _ = tokio::time::sleep(Duration::from_millis(10)), if !accepting => {
                if load.load(Ordering::Relaxed) == 0 {
                    break;
                }
            }
        }
    }
}

struct LoadGuard(Arc<std::sync::atomic::AtomicUsize>);

impl Drop for LoadGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

async fn serve_accepted(
    accepted: Accepted,
    acceptor: Option<tokio_rustls::TlsAcceptor>,
    server: Arc<ServerInner>,
) {
    let _permit = accepted.permit;
    match (accepted.conn, acceptor) {
        (StdConn::Tcp(stream), Some(acceptor)) => {
            let Ok(stream) = tokio::net::TcpStream::from_std(stream) else {
                log::emit(
                    Level::Warn,
                    "tokio",
                    "failed to register TCP stream on I/O shard",
                );
                return;
            };
            serve_conn(
                Conn::Tcp(stream),
                Some(acceptor),
                server,
                accepted.remote_addr,
                accepted.local_addr,
            )
            .await;
        }
        (StdConn::Tcp(stream), None) => {
            let Ok(stream) = tokio::net::TcpStream::from_std(stream) else {
                log::emit(
                    Level::Warn,
                    "tokio",
                    "failed to register TCP stream on I/O shard",
                );
                return;
            };
            serve_conn(
                Conn::Tcp(stream),
                None,
                server,
                accepted.remote_addr,
                accepted.local_addr,
            )
            .await;
        }
        (StdConn::Unix(stream), _) => {
            let Ok(stream) = tokio::net::UnixStream::from_std(stream) else {
                log::emit(
                    Level::Warn,
                    "tokio",
                    "failed to register unix stream on I/O shard",
                );
                return;
            };
            serve_conn(
                Conn::Unix(stream),
                None,
                server,
                accepted.remote_addr,
                accepted.local_addr,
            )
            .await;
        }
    }
}

async fn accept_loop(
    listener: AsyncListener,
    server: Arc<ServerInner>,
    max_connections: usize,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    shard_txs: Vec<tokio::sync::mpsc::UnboundedSender<Accepted>>,
    loads: Vec<Arc<std::sync::atomic::AtomicUsize>>,
    ready_tx: std::sync::mpsc::SyncSender<std::io::Result<()>>,
) {
    let conn_limit = Arc::new(tokio::sync::Semaphore::new(max_connections));
    let mut live_shards = vec![true; shard_txs.len()];
    let _ = ready_tx.send(Ok(()));
    loop {
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
        let conn = match conn {
            Conn::Tcp(stream) => match stream.into_std() {
                Ok(stream) => StdConn::Tcp(stream),
                Err(_) => continue,
            },
            Conn::Unix(stream) => match stream.into_std() {
                Ok(stream) => StdConn::Unix(stream),
                Err(_) => continue,
            },
        };
        let mut accepted = Some(Accepted {
            conn,
            remote_addr,
            local_addr,
            permit,
        });
        while let Some(index) = least_loaded_live(&loads, &live_shards) {
            loads[index].fetch_add(1, Ordering::Relaxed);
            match shard_txs[index].send(accepted.take().expect("accepted connection present")) {
                Ok(()) => break,
                Err(error) => {
                    loads[index].fetch_sub(1, Ordering::Relaxed);
                    live_shards[index] = false;
                    log::emit(Level::Warn, "tokio", "I/O shard is down; routing around it");
                    accepted = Some(error.0);
                }
            }
        }
        if accepted.is_some() {
            log::emit(
                Level::Error,
                "tokio",
                "all I/O shards are down; stopping accept loop",
            );
            server.state.store(STATE_DRAINING, Ordering::Relaxed);
            break;
        }
    }
    drop(server);
}

fn least_loaded_live(
    loads: &[Arc<std::sync::atomic::AtomicUsize>],
    live_shards: &[bool],
) -> Option<usize> {
    loads
        .iter()
        .enumerate()
        .filter(|(index, _)| live_shards[*index])
        .min_by_key(|(_, load)| load.load(Ordering::Relaxed))
        .map(|(index, _)| index)
}

#[cfg(test)]
mod tests {
    use super::{default_thread_count, thread_count};

    #[test]
    fn explicit_io_threads_win() {
        assert_eq!(thread_count(3, 12), 3);
    }

    #[test]
    fn tokio_threads_do_not_size_io_shards() {
        assert_eq!(thread_count(0, 999), thread_count(0, 1));
    }

    #[test]
    fn default_count_is_half_cpus() {
        assert_eq!(default_thread_count(1), 1);
        assert_eq!(default_thread_count(2), 1);
        assert_eq!(default_thread_count(3), 2);
        assert_eq!(default_thread_count(12), 6);
        assert_eq!(default_thread_count(128), 64);
    }
}
