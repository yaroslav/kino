//! tokio + hyper front-end: owns the listener, the runtime, and request
//! intake. Ruby is never on these threads; the only contact points are the
//! flume queue (in) and each request's Responder (out).

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use http_body_util::BodyExt;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto;
use magnus::{Error, Ruby};
use parking_lot::{Mutex, RwLock};

use crate::listen::Listener;
use crate::registry::{self, BoxedCtx, RuntimeHandle, ServerInner, WorkerSlot};
use crate::request::RequestCtx;
use crate::response::{plain_response, HyperResponse, Responder};

fn io_error(ruby: &Ruby, what: &str, e: std::io::Error) -> Error {
    Error::new(ruby.exception_runtime_error(), format!("{what}: {e}"))
}

/// Required key from the boot-config Hash.
fn cfg<T: magnus::TryConvert>(ruby: &Ruby, config: magnus::RHash, key: &str) -> Result<T, Error> {
    cfg_opt::<T>(ruby, config, key)?.ok_or_else(|| {
        Error::new(
            ruby.exception_arg_error(),
            format!("server_start: missing config key :{key}"),
        )
    })
}

/// Optional key from the boot-config Hash (absent or nil → None).
fn cfg_opt<T: magnus::TryConvert>(
    ruby: &Ruby,
    config: magnus::RHash,
    key: &str,
) -> Result<Option<T>, Error> {
    config.lookup(ruby.to_symbol(key))
}

/// Bind + spawn the accept loop. Takes one config Hash: this runs once
/// at boot, so Hash-lookup cost is irrelevant and the interface stays
/// extensible. Binding is synchronous so address errors raise in Ruby at
/// `start` time; returns the actual port for `port: 0`. TLS config errors
/// (bad cert/key) also raise here, before any traffic. The third element
/// of the return tuple is the control-plane port (nil unless control_bind
/// is configured).
pub fn server_start(ruby: &Ruby, config: magnus::RHash) -> Result<(u64, u16, Option<u16>), Error> {
    let bind: String = cfg(ruby, config, "bind")?;
    let port: u16 = cfg(ruby, config, "port")?;
    let queue_depth: usize = cfg(ruby, config, "queue_depth")?;
    let queue_timeout_ms: u64 = cfg(ruby, config, "queue_timeout_ms")?;
    let request_timeout_ms: u64 = cfg_opt::<u64>(ruby, config, "request_timeout_ms")?.unwrap_or(0);
    let max_body_size: usize = cfg_opt::<usize>(ruby, config, "max_body_size")?.unwrap_or(0);
    let max_connections: usize = cfg_opt::<usize>(ruby, config, "max_connections")?.unwrap_or(1024);
    let io_shards: bool = cfg_opt(ruby, config, "io_shards")?.unwrap_or(false);
    let io_threads: usize = cfg_opt::<usize>(ruby, config, "io_threads")?.unwrap_or(0);
    let tokio_threads: usize = cfg_opt::<usize>(ruby, config, "tokio_threads")?.unwrap_or(0);
    let tls_cert: Option<String> = cfg_opt(ruby, config, "tls_cert")?;
    let tls_key: Option<String> = cfg_opt(ruby, config, "tls_key")?;
    let lanes: bool = cfg_opt(ruby, config, "lanes")?.unwrap_or(false);
    // Default true guards embedders calling the native layer directly.
    let http2: bool = cfg_opt(ruby, config, "http2")?.unwrap_or(true);
    let log_requests: bool = cfg_opt(ruby, config, "log_requests")?.unwrap_or(false);
    let mode: String = cfg_opt(ruby, config, "mode")?.unwrap_or_else(|| "threaded".to_string());
    let workers: usize = cfg_opt(ruby, config, "workers")?.unwrap_or(0);
    let threads: usize = cfg_opt(ruby, config, "threads")?.unwrap_or(0);
    let batch: usize = cfg_opt(ruby, config, "batch")?.unwrap_or(1);
    let acceptor = match (&tls_cert, &tls_key) {
        (Some(cert), Some(key)) => Some(
            crate::tls::build_acceptor(cert, key, http2)
                .map_err(|e| Error::new(ruby.exception_runtime_error(), format!("TLS: {e}")))?,
        ),
        (None, None) => None,
        _ => {
            return Err(Error::new(
                ruby.exception_arg_error(),
                "TLS requires both cert and key",
            ))
        }
    };

    let listener = Listener::bind(&bind, port).map_err(|e| io_error(ruby, "bind failed", e))?;
    // Ruby refuses this combination up front; this guards embedders
    // calling the native layer directly.
    if acceptor.is_some() && matches!(listener, Listener::Unix(..)) {
        return Err(Error::new(
            ruby.exception_arg_error(),
            "TLS is not supported on a unix socket bind",
        ));
    }
    let local_port = listener
        .port()
        .map_err(|e| io_error(ruby, "listener setup failed", e))?;
    let unix_path = match &listener {
        Listener::Unix(_, path) => Some(path.clone()),
        Listener::Tcp(_) => None,
    };

    let control_bind_addr: Option<String> = cfg_opt(ruby, config, "control_bind")?;
    let control_token: Option<String> = cfg_opt(ruby, config, "control_token")?;
    let control_bind = control_bind_addr
        .as_deref()
        .map(|addr| {
            crate::control::bind_control(addr).map_err(|e| io_error(ruby, "control bind failed", e))
        })
        .transpose()?;

    let (req_tx, req_rx) = flume::bounded(queue_depth);
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let server = Arc::new(ServerInner {
        id: registry::next_server_id(),
        req_tx: Mutex::new(Some(req_tx)),
        req_rx,
        shutdown_tx,
        runtime: Mutex::new(RuntimeHandle::None),
        slots: RwLock::new(Vec::new()),
        in_flight: std::sync::atomic::AtomicUsize::new(0),
        served: std::sync::atomic::AtomicU64::new(0),
        rejected: std::sync::atomic::AtomicU64::new(0),
        queue_timeout_ms,
        request_timeout_ms,
        max_body_size,
        timeouts: std::sync::atomic::AtomicU64::new(0),
        state: std::sync::atomic::AtomicU8::new(registry::STATE_BOOTING),
        respawns: std::sync::atomic::AtomicU64::new(0),
        quarantine_replacements: std::sync::atomic::AtomicU64::new(0),
        topology: registry::Topology {
            mode,
            workers,
            threads,
            batch,
        },
        https: acceptor.is_some(),
        http2,
        unix_path,
        access_log: log_requests.then(|| crate::logsink::Sink::new(std::io::stdout())),
        lanes,
        lane_cursor: std::sync::atomic::AtomicUsize::new(0),
        pin_slab: Arc::new(crate::pin::PinSlab::new()),
        queue_histogram: registry::QueueHistogram::new(),
    });

    if io_shards {
        // The shards keep serving accepted connections while the acceptor
        // drains on `shutdown_rx`; this second signal stops them only at
        // final teardown.
        let (runtime_shutdown_tx, runtime_shutdown_rx) = tokio::sync::watch::channel(false);
        let threads = crate::io_shards::spawn(
            listener,
            acceptor,
            server.clone(),
            max_connections,
            shutdown_rx,
            runtime_shutdown_rx,
            crate::io_shards::thread_count(io_threads),
        )
        .map_err(|e| io_error(ruby, "sharded runtime failed", e))?;
        *server.runtime.lock() = RuntimeHandle::Shards {
            shutdown_tx: runtime_shutdown_tx,
            threads,
        };
    } else {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder.enable_all().thread_name("kino-tokio");
        if tokio_threads > 0 {
            builder.worker_threads(tokio_threads);
        }
        let runtime = builder
            .build()
            .map_err(|e| io_error(ruby, "tokio runtime failed", e))?;
        let tokio_listener = {
            let _guard = runtime.enter();
            AsyncListener::from_std(listener)
                .map_err(|e| io_error(ruby, "listener setup failed", e))?
        };
        runtime.spawn(accept_loop(
            tokio_listener,
            acceptor,
            server.clone(),
            max_connections,
            shutdown_rx,
        ));
        *server.runtime.lock() = RuntimeHandle::MultiThread(runtime);
    }

    let id = server.id;
    let control_port = match control_bind {
        // Not yet in the registry: on failure Ruby never learns this id, so
        // nothing could ever reach it to shut it down. The accept loop's
        // task holds its own Arc back to `server` (stored inside its own
        // `runtime` field), so just dropping our handle would leak the
        // runtime forever; take it out and stop it explicitly instead.
        // Safe to block here: this is the plain Ruby thread, no async
        // context above it.
        Some(bind) => match crate::control::start(bind, server.clone(), control_token) {
            Ok(port) => port,
            Err(e) => {
                // Nothing is serving yet, so the bound only matters for a
                // wedged shard thread; the default runtime just cancels
                // its one idle accept task.
                let _ = server.shutdown_tx.send(true);
                std::mem::take(&mut *server.runtime.lock()).shutdown(Duration::from_millis(1_000));
                return Err(io_error(ruby, "control start failed", e));
            }
        },
        None => None,
    };
    registry::insert(server);
    Ok((id, local_port, control_port))
}

/// Slowloris guard for TLS: a client that completes the TCP connect but then
/// stalls the handshake would otherwise hold a connection slot indefinitely
/// (the per-request and header-read deadlines only start once hyper is
/// serving, i.e. after the handshake). A handshake is a few round trips, so
/// this is generous even for a high-latency client. Fixed, like the header
/// timeout: not a knob.
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// What a unix-socket connection reports as its addresses. The peer is
/// local by definition (REMOTE_ADDR 127.0.0.1), and a socket has no port,
/// so SERVER_PORT falls back to http's default when the Host header names
/// none, the way Puma reports unix-socket requests.
const UNIX_PEER: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
const UNIX_LOCAL: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 80);

/// The accept loop's listener: TCP (optionally behind TLS), or a unix
/// socket, which carries plain HTTP only.
pub(crate) enum AsyncListener {
    Tcp(tokio::net::TcpListener),
    Unix(tokio::net::UnixListener),
}

/// One accepted connection, before the protocol layer sees it.
pub(crate) enum Conn {
    Tcp(tokio::net::TcpStream),
    Unix(tokio::net::UnixStream),
}

impl AsyncListener {
    /// Register the bound listener with the current runtime.
    pub(crate) fn from_std(listener: Listener) -> std::io::Result<AsyncListener> {
        Ok(match listener {
            Listener::Tcp(listener) => {
                AsyncListener::Tcp(tokio::net::TcpListener::from_std(listener)?)
            }
            Listener::Unix(listener, _) => {
                AsyncListener::Unix(tokio::net::UnixListener::from_std(listener)?)
            }
        })
    }

    /// The next connection with its (peer, local) addresses.
    pub(crate) async fn accept(&self) -> std::io::Result<(Conn, SocketAddr, SocketAddr)> {
        match self {
            AsyncListener::Tcp(listener) => {
                let (stream, remote_addr) = listener.accept().await?;
                // Small responses must not wait on Nagle + delayed ACK.
                let _ = stream.set_nodelay(true);
                let local_addr = stream
                    .local_addr()
                    .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 0)));
                Ok((Conn::Tcp(stream), remote_addr, local_addr))
            }
            AsyncListener::Unix(listener) => {
                let (stream, _) = listener.accept().await?;
                Ok((Conn::Unix(stream), UNIX_PEER, UNIX_LOCAL))
            }
        }
    }
}

async fn accept_loop(
    listener: AsyncListener,
    acceptor: Option<tokio_rustls::TlsAcceptor>,
    server: Arc<ServerInner>,
    max_connections: usize,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    // Bound concurrent connections: unbounded, a flood spawns a task and holds
    // a socket per connection until file descriptors or memory run out. One
    // permit per live connection; acquiring BEFORE accept leaves the excess in
    // the kernel backlog (backpressure) rather than accepting then dropping.
    let conn_limit = Arc::new(tokio::sync::Semaphore::new(max_connections));
    loop {
        let permit = tokio::select! {
            _ = shutdown_rx.changed() => break,
            permit = conn_limit.clone().acquire_owned() => match permit {
                Ok(permit) => permit,
                Err(_) => break, // semaphore closed
            },
        };
        let (conn, remote_addr, local_addr) = tokio::select! {
            _ = shutdown_rx.changed() => break,
            accepted = listener.accept() => match accepted {
                Ok(accepted) => accepted,
                Err(_) => continue, // transient accept error; permit drops, retry
            },
        };
        let server = server.clone();
        let acceptor = acceptor.clone();
        tokio::spawn(async move {
            // Held for the connection's lifetime; dropping it frees a slot.
            let _permit = permit;
            serve_conn(conn, acceptor, server, remote_addr, local_addr).await;
        });
    }
}

/// Everything between an accepted connection and hyper: the optional TLS
/// handshake, then the protocol layer. Shared by the default accept loop
/// and the sharded one, so connection policy exists exactly once.
pub(crate) async fn serve_conn(
    conn: Conn,
    acceptor: Option<tokio_rustls::TlsAcceptor>,
    server: Arc<ServerInner>,
    remote_addr: SocketAddr,
    local_addr: SocketAddr,
) {
    match (conn, acceptor) {
        (Conn::Tcp(stream), Some(acceptor)) => {
            // Handshake failures (port scans, plain HTTP to a TLS
            // port) and stalled handshakes (slowloris) just drop the
            // connection; the timeout bounds the latter.
            let handshake = tokio::time::timeout(TLS_HANDSHAKE_TIMEOUT, acceptor.accept(stream));
            let Ok(Ok(tls)) = handshake.await else { return };
            serve_connection(tls, server, remote_addr, local_addr).await;
        }
        (Conn::Tcp(stream), None) => {
            serve_connection(stream, server, remote_addr, local_addr).await
        }
        // TLS over a unix socket is refused at bind time.
        (Conn::Unix(stream), _) => serve_connection(stream, server, remote_addr, local_addr).await,
    }
}

/// Slowloris guard: drop a connection that has not sent its complete request
/// headers within this window. Long enough never to trip a real client (even
/// on a slow mobile link), short enough to reap a stalled one. Deliberately a
/// constant, not a config knob: fine-tuning intake limits is the fronting
/// proxy's job; the actual hazard was having no default at all. Guards the
/// HTTP/1 side only: h2 intake is bounded by the TLS-handshake timeout and
/// the h2 codec's own SETTINGS handling.
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(15);

/// The connection builder every data-plane connection is served with,
/// in both accept topologies. `http2: true` is the protocol-auto shape:
/// ALPN-negotiated h2 over TLS, prior-knowledge h2c on plaintext (the
/// builder sniffs the 24-byte preface once per connection), HTTP/1.x
/// for everything else. `http2: false` pins the HTTP/1 codec: no sniff,
/// same wire behavior as a server built without h2.
///
/// No auto Date header on either protocol: it costs a clock read per
/// response (together with timer reads, ~7% of tokio-side cycles in the
/// profile); it's a SHOULD not a MUST, and apps that need it can set it
/// themselves.
///
/// The http1 timer is installed so header_read_timeout actually fires:
/// hyper's slow-header guard is inert without one. It arms only while
/// the request head is being read, so it adds no per-response cost on
/// the hot path. The h2 side gets a timer too so its own timed
/// machinery (keep-alive, shutdown deadlines) can fire if ever enabled.
/// SETTINGS_MAX_CONCURRENT_STREAMS, derived from worker-slot capacity
/// (workers × threads) instead of hyper's flat 200. Two jobs: an
/// h2-aware balancer sees the server's real admission and spreads
/// streams across upstream connections instead of queueing blind, and a
/// hostile client can't multiply one connection into 200 queued
/// requests. The floor keeps tiny topologies browser-friendly (one
/// page's fetches still parallelize; excess streams just queue), the
/// cap bounds per-connection bookkeeping, and an unknown topology (an
/// embedder passing zeros) keeps hyper's default.
fn advertised_streams(workers: usize, threads: usize) -> u32 {
    let slots = workers.saturating_mul(threads);
    if slots == 0 {
        return 200;
    }
    slots.clamp(8, 1024) as u32
}

fn conn_builder(http2: bool, max_streams: u32) -> auto::Builder<TokioExecutor> {
    let mut builder = auto::Builder::new(TokioExecutor::new());
    builder
        .http1()
        .timer(TokioTimer::new())
        .header_read_timeout(HEADER_READ_TIMEOUT)
        .auto_date_header(false);
    // Beyond the stream cap, hyper's h2 defaults (16 KB frames, 1 MB
    // windows) stay: a knob sweep (frame size 64K/256K, adaptive
    // windows, 4/8 MB windows) moved the upload lane nowhere or slightly
    // down once read_body coalesced its channel drain — the crossings
    // were the cost, not the codec. The codec's abuse bounds also ship
    // as defaults: 16 KB header lists, 20 pending remote resets then
    // GOAWAY (rapid reset), reset-churn and empty-frame budgets.
    builder
        .http2()
        .timer(TokioTimer::new())
        .auto_date_header(false)
        .max_concurrent_streams(max_streams);
    if http2 {
        builder
    } else {
        builder.http1_only()
    }
}

async fn serve_connection<I>(
    io: I,
    server: Arc<ServerInner>,
    remote_addr: SocketAddr,
    local_addr: SocketAddr,
) where
    I: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let http2 = server.http2;
    let max_streams = advertised_streams(server.topology.workers, server.topology.threads);
    let service =
        service_fn(move |req| handle_request(server.clone(), remote_addr, local_addr, req));
    let _ = conn_builder(http2, max_streams)
        .serve_connection(TokioIo::new(io), service)
        .await;
}

/// The 503 every rejection path returns; counted for stats. Branding
/// happens at handle_request's single exit.
fn unavailable(server: &ServerInner) -> HyperResponse {
    server.rejected.fetch_add(1, Ordering::Relaxed);
    plain_response(503, "Service Unavailable\n")
}

/// Every response carries `Server: kino` unless the app set its own.
fn branded(mut response: HyperResponse) -> HyperResponse {
    response
        .headers_mut()
        .entry(http::header::SERVER)
        .or_insert(http::HeaderValue::from_static("Kino"));
    response
}

/// A single valid Content-Length as a byte count. hyper has already rejected
/// conflicting/duplicate values, so the first is authoritative; anything
/// unparseable yields None and the streaming cap still applies.
fn content_length(headers: &http::HeaderMap) -> Option<u64> {
    headers
        .get(http::header::CONTENT_LENGTH)?
        .to_str()
        .ok()?
        .trim()
        .parse()
        .ok()
}

/// Idle deadline between request-body frames. A client that stalls mid-body
/// would otherwise hold a worker slot indefinitely (the worker blocks in
/// read_body). Generous: a real upload sends steadily and resets this each
/// frame, so only a silent client trips it. Fixed, like the header timeout.
const BODY_READ_TIMEOUT: Duration = Duration::from_secs(30);

async fn handle_request(
    server: Arc<ServerInner>,
    remote_addr: SocketAddr,
    local_addr: SocketAddr,
    req: hyper::Request<hyper::body::Incoming>,
) -> Result<HyperResponse, std::convert::Infallible> {
    let (parts, body) = req.into_parts();

    // Access-log metadata is captured only when logging is on: one Instant
    // read plus two small Strings per request. The arrival record is
    // queued now, before the app sees the request, so a hang shows as an
    // arrow with no answer; the completion record follows the response.
    let log_meta = server.access_log.as_ref().map(|log| {
        let target = match parts.uri.query() {
            Some(q) => format!("{}?{}", parts.uri.path(), q),
            None => parts.uri.path().to_string(),
        };
        let method = parts.method.to_string();
        log.write_line(crate::access_log::arrival(
            &method,
            &target,
            remote_addr.ip(),
        ));
        (std::time::Instant::now(), method, target)
    });

    // Body-size guard: an honestly-declared oversize body is refused with a
    // 413 below, before any worker runs. Chunked or lying clients are caught
    // by the forwarder, which caps cumulative bytes and flags an overflow so
    // read_body raises instead of letting the app buffer without bound.
    let max_body = server.max_body_size;
    let oversize =
        max_body > 0 && content_length(&parts.headers).is_some_and(|len| len > max_body as u64);

    let (head_tx, head_rx) = tokio::sync::oneshot::channel();
    let responder = Arc::new(Responder::new(head_tx));

    // Stream the request body through a bounded channel: hyper is polled
    // only as fast as the Ruby side consumes (inbound backpressure), and the
    // forwarder dropping the sender is EOF. Bodyless requests (most GETs)
    // skip the channel and the forwarder task entirely: no channel IS the EOF.
    let body_rx = if oversize || hyper::body::Body::is_end_stream(&body) {
        None
    } else {
        let (body_tx, body_rx) = flume::bounded::<bytes::Bytes>(8);
        let responder = responder.clone();
        tokio::spawn(async move {
            let mut body = body;
            let mut total: u64 = 0;
            loop {
                // Idle deadline between frames: a client that stalls mid-body
                // would otherwise pin a worker blocked in read_body. Only the
                // client's silence trips this; a slow APP blocks the forwarder
                // in send_async below instead, which is not timed.
                let frame = match tokio::time::timeout(BODY_READ_TIMEOUT, body.frame()).await {
                    Ok(Some(Ok(frame))) => frame,
                    Ok(Some(Err(_))) | Ok(None) => break, // body error or clean EOF
                    Err(_) => {
                        responder.abandon_body(crate::response::BodyAbandon::TimedOut);
                        break;
                    }
                };
                // Non-data frames (h2 trailers) are dropped by design:
                // Rack has no trailer surface. DATA frames around them
                // still forward, and hyper reports EOF right after.
                if let Ok(data) = frame.into_data() {
                    total += data.len() as u64;
                    if max_body > 0 && total > max_body as u64 {
                        // Past the cap: flag it and stop pulling. Dropping the
                        // sender unblocks read_body, which then raises.
                        responder.abandon_body(crate::response::BodyAbandon::Oversize);
                        break;
                    }
                    if body_tx.send_async(data).await.is_err() {
                        break; // request handle dropped; stop pulling
                    }
                }
            }
        });
        Some(body_rx)
    };

    let now = std::time::Instant::now();
    let ctx = Box::new(RequestCtx {
        method: parts.method,
        uri: parts.uri,
        version: parts.version,
        headers: parts.headers,
        remote_addr,
        local_addr,
        https: server.https,
        body_rx,
        leftover: None,
        slot: None,
        pin_slab: server.pin_slab.clone(),
        responder,
        enqueued_at: now,
        timed: server.access_log.is_some(),
        wait: Duration::ZERO,
        admitted_at: now,
        gc: None,
    });

    // Drop guard, not manual decrement: when a client aborts mid-request,
    // hyper DROPS this future at the next await point: a plain decrement
    // after the await would never run, leaking in_flight upward and making
    // shutdown's drain wait its full deadline (observed as a Ctrl-C "hang").
    struct InFlight(Arc<ServerInner>);
    impl Drop for InFlight {
        fn drop(&mut self) {
            self.0.in_flight.fetch_sub(1, Ordering::Relaxed);
        }
    }
    server.in_flight.fetch_add(1, Ordering::Relaxed);
    let _in_flight = InFlight(server.clone());

    // Single exit point so the access log sees every outcome, 503s included.
    let response: HyperResponse = 'resp: {
        if oversize {
            break 'resp plain_response(413, "Payload Too Large\n");
        }
        if server.lanes {
            if !dispatch_to_lane(&server, ctx).await {
                break 'resp unavailable(&server);
            }
        } else {
            // Clone the sender per request (not per connection) so a
            // long-lived keep-alive connection can't hold the queue open
            // during drain.
            let Some(tx) = server.req_tx.lock().clone() else {
                break 'resp unavailable(&server);
            };

            // try_send first: when the queue has room (the overwhelmingly
            // common case) this skips registering a tokio timer + its
            // clock reads. Only a genuinely full queue pays for the timed
            // wait before the 503.
            match tx.try_send(ctx) {
                Ok(()) => {}
                Err(flume::TrySendError::Disconnected(_)) => {
                    break 'resp unavailable(&server);
                }
                Err(flume::TrySendError::Full(ctx)) => {
                    let timeout = Duration::from_millis(server.queue_timeout_ms);
                    let enqueued = tokio::time::timeout(timeout, tx.send_async(ctx)).await;
                    if !matches!(enqueued, Ok(Ok(()))) {
                        // Timed out or queue closed mid-send; ctx was
                        // dropped unsent either way.
                        break 'resp unavailable(&server);
                    }
                }
            }
        }

        // request_timeout: the response head must arrive within the
        // deadline or the client gets an immediate 504; the worker keeps
        // running and its eventual response is dropped harmlessly (the
        // responder's first-claimant race makes the late send a no-op).
        // Caveat by design: a CPU-stuck handler still occupies its slot
        // until it finishes; interrupting arbitrary Ruby would require
        // Thread#raise-style unsafety.
        if server.request_timeout_ms > 0 {
            let deadline = Duration::from_millis(server.request_timeout_ms);
            match tokio::time::timeout(deadline, head_rx).await {
                Ok(result) => {
                    result.unwrap_or_else(|_| plain_response(500, "Internal Server Error\n"))
                }
                Err(_elapsed) => {
                    server.timeouts.fetch_add(1, Ordering::Relaxed);
                    plain_response(504, "Gateway Timeout\n")
                }
            }
        } else {
            head_rx
                .await
                .unwrap_or_else(|_| plain_response(500, "Internal Server Error\n"))
        }
    };

    if let (Some(log), Some((start, method, target))) = (server.access_log.as_ref(), log_meta) {
        // The worker attached its timing to the response head; a 503 or
        // 504 never reached a worker and carries none.
        let timing = response
            .extensions()
            .get::<crate::access_log::Timing>()
            .copied();
        log.write_line(crate::access_log::completion(
            response.status().as_u16(),
            &method,
            &target,
            start.elapsed(),
            timing,
        ));
    }

    Ok(branded(response))
}

/// One dispatch attempt's outcome; `Full` hands the ctx back for a retry.
enum Dispatch {
    Sent,
    Full(BoxedCtx),
    Closed,
}

/// Lane dispatch: prefer an awake (non-parked) lane with room (a hot
/// worker keeps taking without ever paying a futex wake), then any lane
/// with room. All lanes full = genuine overload: retry briefly up to
/// queue_timeout, then give up (caller 503s).
fn try_dispatch(server: &ServerInner, mut ctx: BoxedCtx) -> Dispatch {
    let slots = server.slots.read();
    let n = slots.len();
    if n == 0 {
        return Dispatch::Full(ctx);
    }
    let start = server.lane_cursor.fetch_add(1, Ordering::Relaxed);
    let mut any_open = false;
    for pass in 0..2 {
        for k in 0..n {
            let slot = &slots[(start + k) % n];
            if pass == 0 && slot.parked.load(Ordering::Relaxed) {
                continue;
            }
            let guard = slot.lane_tx.lock();
            let Some(tx) = guard.as_ref() else { continue };
            any_open = true;
            match tx.try_send(ctx) {
                Ok(()) => return Dispatch::Sent,
                Err(flume::TrySendError::Full(c)) | Err(flume::TrySendError::Disconnected(c)) => {
                    ctx = c;
                }
            }
        }
    }
    if any_open {
        Dispatch::Full(ctx) // overload: every open lane is full
    } else {
        Dispatch::Closed // draining: all lanes closed
    }
}

async fn dispatch_to_lane(server: &Arc<ServerInner>, ctx: BoxedCtx) -> bool {
    let mut pending = ctx;
    let deadline = tokio::time::Instant::now() + Duration::from_millis(server.queue_timeout_ms);
    loop {
        match try_dispatch(server, pending) {
            Dispatch::Sent => return true,
            Dispatch::Closed => return false,
            Dispatch::Full(ctx) => {
                if tokio::time::Instant::now() >= deadline {
                    return false;
                }
                pending = ctx;
                tokio::time::sleep(Duration::from_micros(500)).await;
            }
        }
    }
}

// --- lifecycle natives ---
// All tolerant of an already-removed server: shutdown must be idempotent and
// late-waking workers must see "gone" as a no-op, never an exception.

pub fn register_worker(ruby: &Ruby, server_id: u64) -> Result<usize, Error> {
    Ok(registry::get(ruby, server_id)?.register_worker())
}

pub fn stop_accepting(_ruby: &Ruby, server_id: u64) -> Result<(), Error> {
    if let Some(server) = registry::try_get(server_id) {
        server
            .state
            .store(registry::STATE_DRAINING, Ordering::Relaxed);
        let _ = server.shutdown_tx.send(true);
    }
    Ok(())
}

/// Ruby reports the worker pool up; /ready starts answering 200.
pub fn control_ready(_ruby: &Ruby, server_id: u64) -> Result<(), Error> {
    if let Some(server) = registry::try_get(server_id) {
        server.state.store(registry::STATE_READY, Ordering::Relaxed);
    }
    Ok(())
}

/// One worker respawn, recorded by the Ruby supervisor.
pub fn record_respawn(_ruby: &Ruby, server_id: u64) -> Result<(), Error> {
    if let Some(server) = registry::try_get(server_id) {
        server.respawns.fetch_add(1, Ordering::Relaxed);
    }
    Ok(())
}

pub fn close_queue(_ruby: &Ruby, server_id: u64) -> Result<(), Error> {
    if let Some(server) = registry::try_get(server_id) {
        server.req_tx.lock().take();
        for slot in server.slots.read().iter() {
            slot.lane_tx.lock().take();
        }
    }
    Ok(())
}

pub fn queue_stats(_ruby: &Ruby, server_id: u64) -> Result<(usize, usize), Error> {
    match registry::try_get(server_id) {
        Some(server) => Ok((server.queued(), server.in_flight.load(Ordering::Relaxed))),
        None => Ok((0, 0)),
    }
}

fn abort_slot(slot: &WorkerSlot) {
    for weak in slot.current.lock().drain(..) {
        if let Some(responder) = weak.upgrade() {
            responder.respond_500_if_unsent();
        }
    }
    // A dead worker holds nothing: every request it had is answered above
    // (or already was), so the slot is quiescent from here on. Without
    // this, a crashed worker's slot reports in_flight>=1 forever (the
    // supervisor never reuses a slot after a crash), wedging /stats,
    // /metrics and server.stats with a phantom busy worker.
    slot.in_flight.store(0, Ordering::Relaxed);
    // Lane mode: this worker is dead. Close its lane so the dispatcher
    // skips it, and drain anything queued; dropping each ctx fires the
    // Drop-500 backstop so those clients aren't left hanging.
    slot.lane_tx.lock().take();
    slot.parked.store(true, Ordering::Relaxed);
    if let Some(rx) = slot.lane_rx.as_ref() {
        while rx.try_recv().is_ok() {}
    }
}

pub fn abort_inflight(ruby: &Ruby, server_id: u64, worker_id: usize) -> Result<(), Error> {
    if let Some(server) = registry::try_get(server_id) {
        let slot = server.slot(ruby, worker_id)?;
        abort_slot(&slot);
    }
    Ok(())
}

pub fn abort_all_inflight(_ruby: &Ruby, server_id: u64) -> Result<(), Error> {
    if let Some(server) = registry::try_get(server_id) {
        for slot in server.slots.read().iter() {
            abort_slot(slot);
        }
    }
    Ok(())
}

pub fn interrupt_all_workers(_ruby: &Ruby, server_id: u64) -> Result<(), Error> {
    if let Some(server) = registry::try_get(server_id) {
        for slot in server.slots.read().iter() {
            slot.interrupted.store(true, Ordering::SeqCst);
        }
    }
    Ok(())
}

pub fn shutdown_runtime(_ruby: &Ruby, server_id: u64, timeout_ms: u64) -> Result<(), Error> {
    if let Some(server) = registry::remove(server_id) {
        let _ = server.shutdown_tx.send(true);
        std::mem::take(&mut *server.runtime.lock()).shutdown(Duration::from_millis(timeout_ms));
        // The listener is closed with the runtime; its socket file is not.
        if let Some(path) = &server.unix_path {
            crate::listen::cleanup_unix(path);
        }
    }
    Ok(())
}

/// The GC anchor for this server's zero-copy pins: the Ruby Server holds
/// it for its lifetime, so pinned buffers survive worker-ractor crashes.
pub fn pin_keeper(
    ruby: &Ruby,
    server_id: u64,
) -> Result<magnus::typed_data::Obj<crate::pin::PinKeeper>, Error> {
    let server = registry::get(ruby, server_id)?;
    Ok(ruby.obj_wrap(crate::pin::PinKeeper(server.pin_slab.clone())))
}

/// Full stats snapshot: [queued, in_flight, served, rejected, timeouts,
/// respawns, lane_depths]. lane_depths is nil unless lane dispatch is on.
#[allow(clippy::type_complexity)]
pub fn server_stats(
    _ruby: &Ruby,
    server_id: u64,
) -> Result<(usize, usize, u64, u64, u64, u64, Option<Vec<usize>>), Error> {
    let Some(server) = registry::try_get(server_id) else {
        return Ok((0, 0, 0, 0, 0, 0, None));
    };
    let lane_depths = server.lane_depths();
    let queued = server.req_rx.len() + lane_depths.as_ref().map_or(0, |d| d.iter().sum::<usize>());
    Ok((
        queued,
        server.in_flight.load(Ordering::Relaxed),
        server.served.load(Ordering::Relaxed),
        server.rejected.load(Ordering::Relaxed),
        server.timeouts.load(Ordering::Relaxed),
        server.respawns.load(Ordering::Relaxed),
        lane_depths,
    ))
}

/// Queue-wait count and summed seconds for Server#stats parity. Zeros when
/// the server is gone.
pub fn queue_time(_ruby: &Ruby, server_id: u64) -> Result<(u64, f64), Error> {
    let Some(server) = registry::try_get(server_id) else {
        return Ok((0, 0.0));
    };
    let h = server.queue_histogram.snapshot();
    Ok((h.count, h.sum_seconds()))
}

/// One worker slot's [index, served, in_flight, busy_ms, quarantined] row.
pub type WorkerStatRow = (usize, u64, usize, u64, bool);

/// Per-slot rows for Server#stats parity: [index, served, in_flight,
/// busy_ms, quarantined] each. Empty when the server is gone.
pub fn worker_stats(_ruby: &Ruby, server_id: u64) -> Result<Vec<WorkerStatRow>, Error> {
    let Some(server) = registry::try_get(server_id) else {
        return Ok(Vec::new());
    };
    Ok(crate::control::collect_worker_status(&server)
        .into_iter()
        .map(|w| (w.index, w.served, w.in_flight, w.busy_ms, w.quarantined))
        .collect())
}

/// Mark a slot quarantined (the monitor has abandoned it as wedged).
pub fn quarantine_slot(ruby: &Ruby, server_id: u64, worker_id: usize) -> Result<(), Error> {
    if let Some(server) = registry::try_get(server_id) {
        let slot = server.slot(ruby, worker_id)?;
        slot.quarantined.store(true, Ordering::Relaxed);
    }
    Ok(())
}

/// One replacement spawned by the quarantine monitor.
pub fn record_quarantine_replacement(_ruby: &Ruby, server_id: u64) -> Result<(), Error> {
    if let Some(server) = registry::try_get(server_id) {
        server
            .quarantine_replacements
            .fetch_add(1, Ordering::Relaxed);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{test_server, LANE_DEPTH};
    use crate::request::test_ctx;

    /// Serve one real connection through the production `serve_connection`
    /// over an in-memory pipe, against a queue the test consumes itself (a
    /// stand-in for the Ruby worker). Returns the client end.
    fn spawn_conn(server: Arc<ServerInner>) -> tokio::io::DuplexStream {
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        tokio::spawn(serve_connection(
            server_io,
            server,
            "127.0.0.1:40000".parse().expect("static addr"),
            "127.0.0.1:9292".parse().expect("static addr"),
        ));
        client_io
    }

    async fn take_ctx(server: &Arc<ServerInner>) -> BoxedCtx {
        server.req_rx.recv_async().await.expect("a queued request")
    }

    /// Read the request body the way a worker does: leftover first, then
    /// the forwarder channel until the sender drops (EOF).
    async fn drain_body(ctx: &mut RequestCtx) -> Vec<u8> {
        let mut out = Vec::new();
        if let Some(leftover) = ctx.leftover.take() {
            out.extend_from_slice(&leftover);
        }
        if let Some(rx) = &ctx.body_rx {
            while let Ok(chunk) = rx.recv_async().await {
                out.extend_from_slice(&chunk);
            }
        }
        out
    }

    #[tokio::test]
    async fn h2c_prior_knowledge_reaches_the_queue_as_http2() {
        use http_body_util::{BodyExt, Full};

        let server = test_server(false, 4);
        let client_io = spawn_conn(server.clone());

        let worker = tokio::spawn(async move {
            let ctx = take_ctx(&server).await;
            assert_eq!(ctx.version, http::Version::HTTP_2);
            // The :authority pseudo-header arrives in the URI, where
            // build_env picks it up; no Host header exists on h2.
            assert_eq!(
                ctx.uri.authority().map(|a| a.as_str()),
                Some("kino.test:8443")
            );
            assert!(ctx.headers.get(http::header::HOST).is_none());
            assert!(ctx.responder.send_response(plain_response(200, "ok\n")));
        });

        let (mut sender, conn) = hyper::client::conn::http2::handshake(
            hyper_util::rt::TokioExecutor::new(),
            TokioIo::new(client_io),
        )
        .await
        .expect("h2c prior-knowledge handshake");
        tokio::spawn(conn);
        let request = hyper::Request::builder()
            .uri("http://kino.test:8443/")
            .body(Full::new(bytes::Bytes::new()))
            .expect("request");
        let response = sender.send_request(request).await.expect("h2 response");
        assert_eq!(response.status(), 200);
        assert_eq!(
            response.headers().get("server").expect("branded"),
            "Kino",
            "responses stay branded over h2"
        );
        let body = response.into_body().collect().await.expect("body");
        assert_eq!(&body.to_bytes()[..], b"ok\n");
        worker.await.expect("worker assertions");
    }

    #[tokio::test]
    async fn h1_is_still_served_by_the_auto_builder() {
        use http_body_util::{BodyExt, Full};

        let server = test_server(false, 4);
        let client_io = spawn_conn(server.clone());

        let worker = tokio::spawn(async move {
            let ctx = take_ctx(&server).await;
            assert_eq!(ctx.version, http::Version::HTTP_11);
            assert!(ctx.responder.send_response(plain_response(200, "h1\n")));
        });

        let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(client_io))
            .await
            .expect("h1 handshake");
        tokio::spawn(conn);
        let request = hyper::Request::builder()
            .uri("/")
            .header("host", "kino.test")
            .body(Full::new(bytes::Bytes::new()))
            .expect("request");
        let response = sender.send_request(request).await.expect("h1 response");
        assert_eq!(response.status(), 200);
        let body = response.into_body().collect().await.expect("body");
        assert_eq!(&body.to_bytes()[..], b"h1\n");
        worker.await.expect("worker assertions");
    }

    #[tokio::test]
    async fn http2_off_refuses_the_preface_but_serves_h1() {
        use http_body_util::{BodyExt, Full};
        use hyper::service::service_fn;

        // Driven at the builder seam: conn_builder(false) is what a
        // ServerInner with http2 off serves every connection with.
        let stub = || {
            service_fn(|_req: hyper::Request<hyper::body::Incoming>| async {
                Ok::<_, std::convert::Infallible>(plain_response(200, "pinned\n"))
            })
        };

        // An h2 prior-knowledge client must fail: the pinned h1 codec
        // reads the preface as a malformed request line.
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        tokio::spawn(async move {
            let _ = conn_builder(false, 200)
                .serve_connection(TokioIo::new(server_io), stub())
                .await;
        });
        let refused = async {
            let (mut sender, conn) = hyper::client::conn::http2::handshake(
                hyper_util::rt::TokioExecutor::new(),
                TokioIo::new(client_io),
            )
            .await?;
            tokio::spawn(conn);
            let request = hyper::Request::builder()
                .uri("http://kino.test/")
                .body(Full::new(bytes::Bytes::new()))?;
            sender.send_request(request).await?;
            Ok::<_, Box<dyn std::error::Error>>(())
        }
        .await;
        assert!(refused.is_err(), "h2 must not be served when pinned to h1");

        // The same pinned builder serves a plain h1 client.
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        tokio::spawn(async move {
            let _ = conn_builder(false, 200)
                .serve_connection(TokioIo::new(server_io), stub())
                .await;
        });
        let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(client_io))
            .await
            .expect("h1 handshake");
        tokio::spawn(conn);
        let request = hyper::Request::builder()
            .uri("/")
            .header("host", "kino.test")
            .body(Full::new(bytes::Bytes::new()))
            .expect("request");
        let response = sender.send_request(request).await.expect("h1 response");
        assert_eq!(response.status(), 200);
        let body = response.into_body().collect().await.expect("body");
        assert_eq!(&body.to_bytes()[..], b"pinned\n");
    }

    #[tokio::test]
    async fn h2_upload_forwards_data_and_drops_trailers() {
        use http_body_util::{BodyExt, StreamBody};
        use hyper::body::Frame;

        let server = test_server(false, 4);
        let client_io = spawn_conn(server.clone());

        let worker = tokio::spawn(async move {
            let mut ctx = take_ctx(&server).await;
            assert_eq!(ctx.version, http::Version::HTTP_2);
            let body = drain_body(&mut ctx).await;
            assert_eq!(&body[..], b"hello world");
            assert!(
                ctx.responder.body_abandoned().is_none(),
                "a trailer frame must not abort the body read"
            );
            let response = hyper::Response::builder()
                .status(200)
                .body(crate::response::full_body(bytes::Bytes::from(
                    body.len().to_string(),
                )))
                .expect("response");
            assert!(ctx.responder.send_response(response));
        });

        let (frames_tx, frames_rx) =
            flume::bounded::<Result<Frame<bytes::Bytes>, std::io::Error>>(4);
        frames_tx
            .send(Ok(Frame::data(bytes::Bytes::from_static(b"hello "))))
            .expect("frame");
        frames_tx
            .send(Ok(Frame::data(bytes::Bytes::from_static(b"world"))))
            .expect("frame");
        let mut trailers = http::HeaderMap::new();
        trailers.insert("x-checksum", "ignored".parse().expect("value"));
        frames_tx
            .send(Ok(Frame::trailers(trailers)))
            .expect("frame");
        drop(frames_tx); // EOS

        let (mut sender, conn) = hyper::client::conn::http2::handshake(
            hyper_util::rt::TokioExecutor::new(),
            TokioIo::new(client_io),
        )
        .await
        .expect("h2 handshake");
        tokio::spawn(conn);
        let request = hyper::Request::builder()
            .method("POST")
            .uri("http://kino.test/upload")
            .body(StreamBody::new(frames_rx.into_stream()))
            .expect("request");
        let response = sender.send_request(request).await.expect("h2 response");
        assert_eq!(response.status(), 200);
        let body = response.into_body().collect().await.expect("body");
        assert_eq!(&body.to_bytes()[..], b"11");
        worker.await.expect("worker assertions");
    }

    #[tokio::test]
    async fn h2_streaming_response_arrives_chunked() {
        use http_body_util::{BodyExt, Full};

        let server = test_server(false, 4);
        let client_io = spawn_conn(server.clone());

        let worker = tokio::spawn(async move {
            let ctx = take_ctx(&server).await;
            let started = ctx
                .responder
                .send_stream_head(hyper::Response::builder().status(200))
                .expect("valid head");
            assert!(started);
            let frames = ctx.responder.body_sender().expect("open stream");
            for chunk in [&b"alpha "[..], &b"beta "[..], &b"gamma"[..]] {
                frames
                    .send_async(Ok(hyper::body::Frame::data(bytes::Bytes::from_static(
                        chunk,
                    ))))
                    .await
                    .expect("chunk accepted");
            }
            ctx.responder.finish_stream();
        });

        let (mut sender, conn) = hyper::client::conn::http2::handshake(
            hyper_util::rt::TokioExecutor::new(),
            TokioIo::new(client_io),
        )
        .await
        .expect("h2 handshake");
        tokio::spawn(conn);
        let request = hyper::Request::builder()
            .uri("http://kino.test/stream")
            .body(Full::new(bytes::Bytes::new()))
            .expect("request");
        let response = sender.send_request(request).await.expect("h2 response");
        assert_eq!(response.status(), 200);
        let body = response.into_body().collect().await.expect("body");
        assert_eq!(&body.to_bytes()[..], b"alpha beta gamma");
        worker.await.expect("worker assertions");
    }

    #[tokio::test]
    async fn h2_multiplexes_concurrent_streams_into_the_queue() {
        use http_body_util::{BodyExt, Full};

        let server = test_server(false, 4);
        let client_io = spawn_conn(server.clone());

        // Both streams must be queued before either is answered — that is
        // multiplexing observable at the worker boundary — and answering
        // them in reverse order proves stream completion is not FIFO.
        let worker = tokio::spawn(async move {
            let first = take_ctx(&server).await;
            let second = take_ctx(&server).await;
            let order = [second.uri.path().to_string(), first.uri.path().to_string()];
            assert!(second.responder.send_response(plain_response(200, "two\n")));
            assert!(first.responder.send_response(plain_response(200, "one\n")));
            order
        });

        let (mut sender, conn) = hyper::client::conn::http2::handshake(
            hyper_util::rt::TokioExecutor::new(),
            TokioIo::new(client_io),
        )
        .await
        .expect("h2 handshake");
        tokio::spawn(conn);
        let req = |path: &str| {
            hyper::Request::builder()
                .uri(format!("http://kino.test{path}"))
                .body(Full::new(bytes::Bytes::new()))
                .expect("request")
        };
        let (one, two) = tokio::join!(
            sender.send_request(req("/one")),
            sender.send_request(req("/two"))
        );
        let one = one.expect("first stream");
        let two = two.expect("second stream");
        assert_eq!(one.status(), 200);
        assert_eq!(two.status(), 200);
        let one = one.into_body().collect().await.expect("body").to_bytes();
        let two = two.into_body().collect().await.expect("body").to_bytes();
        assert_eq!(&one[..], b"one\n");
        assert_eq!(&two[..], b"two\n");
        let order = worker.await.expect("worker assertions");
        assert_eq!(order, ["/two".to_string(), "/one".to_string()]);
    }

    #[test]
    fn advertised_streams_tracks_slot_capacity() {
        // Slot capacity, floored for tiny topologies and capped.
        assert_eq!(advertised_streams(8, 3), 24);
        assert_eq!(advertised_streams(2, 1), 8, "floor");
        assert_eq!(advertised_streams(64, 32), 1024, "cap");
        // Unknown topology (embedder passing zeros): hyper's default.
        assert_eq!(advertised_streams(0, 0), 200);
        assert_eq!(advertised_streams(8, 0), 200);
    }

    #[tokio::test]
    async fn streams_beyond_the_advertised_cap_queue_instead_of_failing() {
        use http_body_util::Full;
        use hyper::service::service_fn;

        // Cap of 2: six concurrent requests must all complete — the h2
        // client holds excess streams locally until the server frees a
        // slot; nothing is refused or reset.
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        tokio::spawn(async move {
            let stub = service_fn(|_req: hyper::Request<hyper::body::Incoming>| async {
                Ok::<_, std::convert::Infallible>(plain_response(200, "capped\n"))
            });
            let _ = conn_builder(true, 2)
                .serve_connection(TokioIo::new(server_io), stub)
                .await;
        });
        let (mut sender, conn) = hyper::client::conn::http2::handshake(
            hyper_util::rt::TokioExecutor::new(),
            TokioIo::new(client_io),
        )
        .await
        .expect("h2 handshake");
        tokio::spawn(conn);
        let mut requests = Vec::new();
        for i in 0..6 {
            let request = hyper::Request::builder()
                .uri(format!("http://kino.test/{i}"))
                .body(Full::new(bytes::Bytes::new()))
                .expect("request");
            requests.push(sender.send_request(request));
        }
        for request in requests {
            let response = request.await.expect("queued stream served");
            assert_eq!(response.status(), 200);
        }
    }

    #[tokio::test]
    async fn rapid_stream_resets_do_not_wedge_the_pipeline() {
        use http_body_util::{BodyExt, Full};

        let server = test_server(false, 64);
        let client_io = spawn_conn(server.clone());

        // A worker that answers everything it sees until told to stop;
        // answers to already-reset streams just vanish, as in production.
        let worker = tokio::spawn(async move {
            loop {
                let Ok(ctx) = server.req_rx.recv_async().await else {
                    break;
                };
                let done = ctx.uri.path() == "/done";
                ctx.responder.send_response(plain_response(200, "ok\n"));
                if done {
                    break;
                }
            }
        });

        let (mut sender, conn) = hyper::client::conn::http2::handshake(
            hyper_util::rt::TokioExecutor::new(),
            TokioIo::new(client_io),
        )
        .await
        .expect("h2 handshake");
        tokio::spawn(conn);

        // Fire-and-cancel: dropping the response future resets the
        // stream. The codec bounds reset churn; the server must keep
        // serving afterwards.
        for i in 0..40 {
            let request = hyper::Request::builder()
                .uri(format!("http://kino.test/cancel/{i}"))
                .body(Full::new(bytes::Bytes::new()))
                .expect("request");
            drop(sender.send_request(request));
        }
        let request = hyper::Request::builder()
            .uri("http://kino.test/done")
            .body(Full::new(bytes::Bytes::new()))
            .expect("request");
        let response = sender.send_request(request).await.expect("still served");
        assert_eq!(response.status(), 200);
        let body = response.into_body().collect().await.expect("body");
        assert_eq!(&body.to_bytes()[..], b"ok\n");
        worker.await.expect("worker loop");
    }

    #[tokio::test(start_paused = true)]
    async fn header_read_timeout_still_fires_through_the_auto_builder() {
        use hyper::service::service_fn;
        use tokio::io::AsyncWriteExt;

        let (mut client_io, server_io) = tokio::io::duplex(64 * 1024);
        let conn = tokio::spawn(async move {
            let _ = conn_builder(true)
                .serve_connection(
                    TokioIo::new(server_io),
                    service_fn(|_req: hyper::Request<hyper::body::Incoming>| async {
                        Ok::<_, std::convert::Infallible>(plain_response(200, "never\n"))
                    }),
                )
                .await;
        });
        // A partial h1 request line, then silence: the slow-header guard
        // must reap the connection (paused clock auto-advances past 15s).
        client_io
            .write_all(b"GET / HT")
            .await
            .expect("partial write");
        tokio::time::timeout(Duration::from_secs(60), conn)
            .await
            .expect("connection reaped by header_read_timeout")
            .expect("serve task join");
    }

    #[test]
    fn dispatch_with_no_slots_reports_full() {
        let server = test_server(true, 4);

        assert!(matches!(
            try_dispatch(&server, test_ctx()),
            Dispatch::Full(_)
        ));
    }

    #[test]
    fn dispatch_sends_to_an_open_lane() {
        let server = test_server(true, 4);
        server.register_worker();

        assert!(matches!(try_dispatch(&server, test_ctx()), Dispatch::Sent));
        assert_eq!(server.lane_depths(), Some(vec![1]));
    }

    #[test]
    fn dispatch_hands_the_ctx_back_when_every_lane_is_full() {
        let server = test_server(true, 4);
        server.register_worker();

        for _ in 0..LANE_DEPTH {
            assert!(matches!(try_dispatch(&server, test_ctx()), Dispatch::Sent));
        }
        // The overload path must return the ctx for the timed retry loop.
        assert!(matches!(
            try_dispatch(&server, test_ctx()),
            Dispatch::Full(_)
        ));
    }

    #[test]
    fn dispatch_reports_closed_when_lanes_are_draining() {
        let server = test_server(true, 4);
        server.register_worker();
        server.slots.read()[0].lane_tx.lock().take();

        assert!(matches!(
            try_dispatch(&server, test_ctx()),
            Dispatch::Closed
        ));
    }

    #[test]
    fn branded_adds_the_server_header_unless_the_app_set_one() {
        let response = branded(plain_response(200, "x"));
        assert_eq!(response.headers().get("server").unwrap(), "Kino");

        let mut custom = plain_response(200, "x");
        custom.headers_mut().insert(
            http::header::SERVER,
            http::HeaderValue::from_static("custom"),
        );
        assert_eq!(branded(custom).headers().get("server").unwrap(), "custom");
    }

    #[test]
    fn unavailable_counts_rejections_and_returns_503() {
        let server = test_server(false, 1);

        let response = unavailable(&server);
        assert_eq!(response.status(), 503);
        assert_eq!(server.rejected.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn dispatch_round_robins_across_awake_lanes() {
        let server = test_server(true, 4);
        server.register_worker();
        server.register_worker();

        for _ in 0..4 {
            assert!(matches!(try_dispatch(&server, test_ctx()), Dispatch::Sent));
        }
        // The rotating cursor spreads load instead of pinning one lane.
        assert_eq!(server.lane_depths(), Some(vec![2, 2]));
    }

    #[test]
    fn dispatch_skips_parked_lanes_when_an_awake_one_has_room() {
        let server = test_server(true, 4);
        server.register_worker();
        server.register_worker();
        server.slots.read()[0].parked.store(true, Ordering::Relaxed);

        // Both dispatches land on the awake lane (slot 1), regardless of
        // where the rotating cursor starts.
        assert!(matches!(try_dispatch(&server, test_ctx()), Dispatch::Sent));
        assert!(matches!(try_dispatch(&server, test_ctx()), Dispatch::Sent));
        assert_eq!(server.lane_depths(), Some(vec![0, 2]));
    }
}
