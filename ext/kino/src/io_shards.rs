//! Current-thread Tokio runtimes for HTTP I/O.
//!
//! One accept thread owns the listener and assigns accepted connections to
//! the least-loaded shard. Each shard then owns that connection for its
//! lifetime, avoiding the shared Tokio worker pool on hot HTTP paths.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;

use tokio_util::either::Either;

use crate::counters::Counted;
use crate::listen::Listener;
use crate::log::{self, Level};
use crate::registry::{ServerInner, STATE_DRAINING};
use crate::server::{next_accept, serve_conn, Accepted, AsyncListener, Conn};

/// An accepted stream in transit from the acceptor to its shard. Tokio
/// streams are bound to the runtime that registered them, so the handoff
/// carries the std stream and the shard re-registers it on arrival.
type StdConn = Either<std::net::TcpStream, std::os::unix::net::UnixStream>;

/// Detach an accepted stream from the acceptor's runtime for the handoff.
fn into_std(conn: Conn) -> std::io::Result<StdConn> {
    Ok(match conn {
        Either::Left(stream) => Either::Left(stream.into_std()?),
        Either::Right(stream) => Either::Right(stream.into_std()?),
    })
}

/// Register a handed-over stream with the calling (shard) runtime.
fn into_tokio(conn: StdConn) -> std::io::Result<Conn> {
    Ok(match conn {
        Either::Left(stream) => Either::Left(tokio::net::TcpStream::from_std(stream)?),
        Either::Right(stream) => Either::Right(tokio::net::UnixStream::from_std(stream)?),
    })
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

    match spawn_acceptor(
        listener,
        server,
        max_connections,
        accept_shutdown_rx,
        shard_txs,
        loads,
    ) {
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

/// What an I/O thread reports once its runtime is up, or what kept it
/// from coming up.
type Ready = std::sync::mpsc::SyncSender<std::io::Result<()>>;

/// Spawn thread `name` (the `what` thread) on a current-thread runtime of its own, running
/// `run`, and wait for it to report ready, so a startup failure becomes Ruby's boot error.
fn spawn_io_thread(
    name: String,
    what: &str,
    run: impl FnOnce(tokio::runtime::Runtime, Ready) + Send + 'static,
) -> std::io::Result<JoinHandle<()>> {
    let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
    let handle = std::thread::Builder::new().name(name).spawn(move || {
        match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => run(runtime, ready_tx),
            Err(error) => {
                let _ = ready_tx.send(Err(error));
            }
        }
    })?;
    match ready_rx.recv() {
        Ok(Ok(())) => Ok(handle),
        Ok(Err(error)) => {
            let _ = handle.join();
            Err(error)
        }
        Err(_) => Err(std::io::Error::other(format!(
            "{what} thread exited during startup"
        ))),
    }
}

fn spawn_shard(
    index: usize,
    rx: tokio::sync::mpsc::UnboundedReceiver<Accepted<StdConn>>,
    acceptor: Option<tokio_rustls::TlsAcceptor>,
    server: Arc<ServerInner>,
    load: Arc<AtomicUsize>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> std::io::Result<JoinHandle<()>> {
    spawn_io_thread(
        format!("kino-io-{index}"),
        "shard",
        move |runtime, ready_tx| {
            // Shards run until final teardown, unlike the acceptor.
            let _ = server.io_runtime.set(runtime.handle().clone());
            let _ = ready_tx.send(Ok(()));
            runtime.block_on(shard_loop(rx, acceptor, server, load, &mut shutdown_rx));
        },
    )
}

fn spawn_acceptor(
    listener: Listener,
    server: Arc<ServerInner>,
    max_connections: usize,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
    shard_txs: Vec<tokio::sync::mpsc::UnboundedSender<Accepted<StdConn>>>,
    loads: Vec<Arc<AtomicUsize>>,
) -> std::io::Result<JoinHandle<()>> {
    spawn_io_thread(
        "kino-accept".to_string(),
        "accept",
        move |runtime, ready_tx| {
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
        },
    )
}

/// Serve handed-over connections until the acceptor is gone, then let the
/// remaining ones run until the final teardown signal: dropping the
/// runtime cancels connection tasks at their next await point.
async fn shard_loop(
    mut rx: tokio::sync::mpsc::UnboundedReceiver<Accepted<StdConn>>,
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
                // The acceptor counted it.
                let load = Counted::adopt(load.clone(), 1);
                connections.spawn(serve_accepted(accepted, load, acceptor, server));
            }
            // Reap closed connections so the set stays small.
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
        }
    }
    // The acceptor is gone. Upgraded connections (hijacked bridges, native
    // WebSockets) run in tasks outside the set, so the runtime stays up
    // until teardown even once every connection in it has closed.
    let _ = shutdown_rx.changed().await;
    drop(connections);
}

/// The shard's half of the handoff: re-register the stream on this
/// runtime, then run the shared connection pipeline (TLS handshake,
/// protocol layer) exactly as the default runtime would.
async fn serve_accepted(
    Accepted {
        conn,
        peer,
        mut held,
    }: Accepted<StdConn>,
    load: Counted,
    acceptor: Option<tokio_rustls::TlsAcceptor>,
    server: Arc<ServerInner>,
) {
    let Ok(conn) = into_tokio(conn) else {
        log::emit(
            Level::Warn,
            "tokio",
            "failed to register a stream on an I/O shard",
        );
        return;
    };
    held.shard_load = Some(load);
    serve_conn(Accepted { conn, peer, held }, acceptor, server).await;
}

/// The sharded accept loop, with the default loop's backpressure
/// (next_accept). A shard whose channel is gone is marked dead and
/// routed around; with no shard left the loop stops accepting and flips
/// the server to draining, so the control plane stops reporting ready.
async fn accept_loop(
    listener: AsyncListener,
    server: Arc<ServerInner>,
    max_connections: usize,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    shard_txs: Vec<tokio::sync::mpsc::UnboundedSender<Accepted<StdConn>>>,
    loads: Vec<Arc<AtomicUsize>>,
    ready_tx: Ready,
) {
    let conn_limit = Arc::new(tokio::sync::Semaphore::new(max_connections));
    let mut live = vec![true; shard_txs.len()];
    let _ = ready_tx.send(Ok(()));
    'accept: while let Some(Accepted { conn, peer, held }) =
        next_accept(&listener, &conn_limit, &mut shutdown_rx).await
    {
        let Ok(conn) = into_std(conn) else {
            log::emit(
                Level::Warn,
                "tokio",
                "failed to detach an accepted stream; connection dropped",
            );
            continue;
        };
        let mut accepted = Accepted { conn, peer, held };
        loop {
            let Some(index) = least_loaded(&loads, &live) else {
                log::emit(
                    Level::Error,
                    "tokio",
                    "all I/O shards are down; not accepting connections",
                );
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
    use super::*;
    use crate::testing::{upgrade_request, SWITCHED};
    use hyper_util::rt::TokioIo;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// A client connects to `port`, and the app (the test, playing the
    /// worker) takes the connection over and switches it to another
    /// protocol; once a byte went through, returns both ends.
    async fn upgraded(
        server: &ServerInner,
        port: u16,
    ) -> (TokioIo<hyper::upgrade::Upgraded>, tokio::net::UnixStream) {
        let tcp = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("connect");
        let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(tcp))
            .await
            .expect("h1 handshake");
        tokio::spawn(conn.with_upgrades());
        let pending = tokio::spawn(async move { sender.send_request(upgrade_request()).await });
        let mut ctx = server.req_rx.recv_async().await.expect("the request");
        let app = crate::hijack::take(&mut ctx, None).expect("an Upgrade request can be hijacked");
        app.set_nonblocking(true).expect("nonblocking app end");
        let mut app = tokio::net::UnixStream::from_std(app).expect("app end registered");
        app.write_all(SWITCHED).await.expect("app write");
        let response = pending.await.expect("join").expect("the 101");
        assert_eq!(response.status(), 101);
        let mut client = TokioIo::new(hyper::upgrade::on(response).await.expect("client upgrade"));
        client.write_all(b"x").await.expect("client write");
        app.read_u8().await.expect("the byte through the bridge");
        (client, app)
    }

    fn counts(loads: &[Arc<AtomicUsize>]) -> Vec<usize> {
        loads
            .iter()
            .map(|load| load.load(Ordering::Relaxed))
            .collect()
    }

    #[tokio::test]
    async fn sequential_upgraded_connections_spread_across_shards() {
        // Runs one at a time with the socket and bridge tests, as they do.
        let _serial = crate::testing::SERIAL.lock().await;
        let server = crate::testing::TestServer::shared_queue().build();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        listener
            .set_nonblocking(true)
            .expect("nonblocking listener");
        let port = listener.local_addr().expect("local addr").port();
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let loads = vec![Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0))];
        let mut handles = Vec::new();
        let mut shard_txs = Vec::new();
        for (index, load) in loads.iter().enumerate() {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            let shard = spawn_shard(
                index,
                rx,
                None,
                server.clone(),
                load.clone(),
                shutdown_rx.clone(),
            );
            handles.push(shard.expect("shard"));
            shard_txs.push(tx);
        }
        let listener = Listener::Tcp(listener);
        let acceptor = spawn_acceptor(
            listener,
            server.clone(),
            16,
            shutdown_rx,
            shard_txs,
            loads.clone(),
        );
        handles.push(acceptor.expect("acceptor"));

        let first = upgraded(&server, port).await;
        let second = upgraded(&server, port).await;

        assert_eq!(
            counts(&loads),
            [1, 1],
            "two open upgraded connections must count on different shards"
        );
        drop((first, second));
        let still = std::fmt::from_fn(|f| {
            write!(
                f,
                "closed upgraded connections to stop counting (shard loads {:?})",
                counts(&loads)
            )
        });
        crate::testing::until(still, || counts(&loads) == [0, 0]).await;
        shutdown_tx.send(true).expect("shards listening");
        tokio::task::spawn_blocking(|| join_all(handles))
            .await
            .expect("join");
    }

    fn loads(counts: &[usize]) -> Vec<Arc<AtomicUsize>> {
        counts
            .iter()
            .map(|&n| Arc::new(AtomicUsize::new(n)))
            .collect()
    }

    #[test]
    fn explicit_io_threads_win() {
        assert_eq!(thread_count(3), 3);
    }

    #[test]
    fn least_loaded_picks_the_emptiest_live_shard() {
        assert_eq!(
            least_loaded(&loads(&[3, 0, 1]), &[true, true, true]),
            Some(1)
        );
    }

    #[test]
    fn least_loaded_routes_around_dead_shards() {
        // The dead shard's count is frozen at 0; it must not win anyway.
        assert_eq!(
            least_loaded(&loads(&[3, 0, 1]), &[true, false, true]),
            Some(2)
        );
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
