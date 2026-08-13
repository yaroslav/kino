//! tokio + hyper front-end: owns the listener, the runtime, and request
//! intake. Ruby is never on these threads; the only contact points are the
//! flume queue (in) and each request's Responder (out).

use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use http_body_util::BodyExt;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use magnus::{Error, Ruby};
use parking_lot::{Mutex, RwLock};

use crate::registry::{self, BoxedCtx, ServerInner, WorkerSlot};
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
    let tokio_threads: usize = cfg_opt::<usize>(ruby, config, "tokio_threads")?.unwrap_or(0);
    let tls_cert: Option<String> = cfg_opt(ruby, config, "tls_cert")?;
    let tls_key: Option<String> = cfg_opt(ruby, config, "tls_key")?;
    let lanes: bool = cfg_opt(ruby, config, "lanes")?.unwrap_or(false);
    let log_requests: bool = cfg_opt(ruby, config, "log_requests")?.unwrap_or(false);
    let mode: String = cfg_opt(ruby, config, "mode")?.unwrap_or_else(|| "threaded".to_string());
    let workers: usize = cfg_opt(ruby, config, "workers")?.unwrap_or(0);
    let threads: usize = cfg_opt(ruby, config, "threads")?.unwrap_or(0);
    let batch: usize = cfg_opt(ruby, config, "batch")?.unwrap_or(1);
    let acceptor = match (&tls_cert, &tls_key) {
        (Some(cert), Some(key)) => Some(
            crate::tls::build_acceptor(cert, key)
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

    let listener = std::net::TcpListener::bind((bind.as_str(), port))
        .map_err(|e| io_error(ruby, "bind failed", e))?;
    listener
        .set_nonblocking(true)
        .map_err(|e| io_error(ruby, "listener setup failed", e))?;
    let local_port = listener
        .local_addr()
        .map_err(|e| io_error(ruby, "listener setup failed", e))?
        .port();

    let control_bind_addr: Option<String> = cfg_opt(ruby, config, "control_bind")?;
    let control_token: Option<String> = cfg_opt(ruby, config, "control_token")?;
    let control_bind = control_bind_addr
        .as_deref()
        .map(|addr| {
            crate::control::bind_control(addr).map_err(|e| io_error(ruby, "control bind failed", e))
        })
        .transpose()?;

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all().thread_name("kino-tokio");
    if tokio_threads > 0 {
        builder.worker_threads(tokio_threads);
    }
    let runtime = builder
        .build()
        .map_err(|e| io_error(ruby, "tokio runtime failed", e))?;

    let (req_tx, req_rx) = flume::bounded(queue_depth);
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let server = Arc::new(ServerInner {
        id: registry::next_server_id(),
        req_tx: Mutex::new(Some(req_tx)),
        req_rx,
        shutdown_tx,
        runtime: Mutex::new(None),
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
        topology: registry::Topology { mode, workers, threads, batch },
        https: acceptor.is_some(),
        access_log: log_requests.then(|| crate::logsink::Sink::new(std::io::stdout())),
        lanes,
        lane_cursor: std::sync::atomic::AtomicUsize::new(0),
        pin_slab: Arc::new(crate::pin::PinSlab::new()),
        queue_histogram: registry::QueueHistogram::new(),
    });

    let tokio_listener = {
        let _guard = runtime.enter();
        tokio::net::TcpListener::from_std(listener)
            .map_err(|e| io_error(ruby, "listener setup failed", e))?
    };
    runtime.spawn(accept_loop(
        tokio_listener,
        acceptor,
        server.clone(),
        max_connections,
        shutdown_rx,
    ));
    *server.runtime.lock() = Some(runtime);

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
                // A plain drop blocks until the accept loop's task (its
                // only task, idling on accept/shutdown) is torn down; the
                // runtime only ever had this one thing to cancel.
                drop(server.runtime.lock().take());
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

async fn accept_loop(
    listener: tokio::net::TcpListener,
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
        let (stream, remote_addr) = tokio::select! {
            _ = shutdown_rx.changed() => break,
            accepted = listener.accept() => match accepted {
                Ok(pair) => pair,
                Err(_) => continue, // transient accept error; permit drops, retry
            },
        };
        // Small responses must not wait on Nagle + delayed ACK.
        let _ = stream.set_nodelay(true);
        let local_addr = stream
            .local_addr()
            .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 0)));
        let server = server.clone();
        let acceptor = acceptor.clone();
        tokio::spawn(async move {
            // Held for the connection's lifetime; dropping it frees a slot.
            let _permit = permit;
            match acceptor {
                Some(acceptor) => {
                    // Handshake failures (port scans, plain HTTP to a TLS
                    // port) and stalled handshakes (slowloris) just drop the
                    // connection; the timeout bounds the latter.
                    let handshake = tokio::time::timeout(TLS_HANDSHAKE_TIMEOUT, acceptor.accept(stream));
                    let Ok(Ok(tls)) = handshake.await else { return };
                    serve_connection(tls, server, remote_addr, local_addr).await;
                }
                None => serve_connection(stream, server, remote_addr, local_addr).await,
            }
        });
    }
}

/// Slowloris guard: drop a connection that has not sent its complete request
/// headers within this window. Long enough never to trip a real client (even
/// on a slow mobile link), short enough to reap a stalled one. Deliberately a
/// constant, not a config knob: fine-tuning intake limits is the fronting
/// proxy's job; the actual hazard was having no default at all.
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(15);

async fn serve_connection<I>(
    io: I,
    server: Arc<ServerInner>,
    remote_addr: SocketAddr,
    local_addr: SocketAddr,
) where
    I: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let service =
        service_fn(move |req| handle_request(server.clone(), remote_addr, local_addr, req));
    // No auto Date header: it costs a clock read per response (together
    // with timer reads, ~7% of tokio-side cycles in the profile); it's a
    // SHOULD not a MUST, and apps that need it can set it themselves.
    //
    // The timer is installed so header_read_timeout actually fires: hyper's
    // slow-header guard is inert without one. It arms only while the request
    // head is being read, so it adds no per-response cost on the hot path.
    let _ = hyper::server::conn::http1::Builder::new()
        .timer(hyper_util::rt::TokioTimer::new())
        .header_read_timeout(HEADER_READ_TIMEOUT)
        .auto_date_header(false)
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
    // read plus one small String per request.
    let log_meta = server.access_log.as_ref().map(|_| {
        let target = match parts.uri.query() {
            Some(q) => format!("{}?{}", parts.uri.path(), q),
            None => parts.uri.path().to_string(),
        };
        (
            std::time::Instant::now(),
            parts.method.to_string(),
            target,
            parts.version,
        )
    });

    // Body-size guard: an honestly-declared oversize body is refused with a
    // 413 below, before any worker runs. Chunked or lying clients are caught
    // by the forwarder, which caps cumulative bytes and flags an overflow so
    // read_body raises instead of letting the app buffer without bound.
    let max_body = server.max_body_size;
    let oversize =
        max_body > 0 && content_length(&parts.headers).is_some_and(|len| len > max_body as u64);

    // Stream the request body through a bounded channel: hyper is polled
    // only as fast as the Ruby side consumes (inbound backpressure), and the
    // forwarder dropping the sender is EOF. Bodyless requests (most GETs)
    // skip the forwarder task entirely: dropping the sender IS the EOF.
    let (body_tx, body_rx) = flume::bounded::<bytes::Bytes>(8);
    let body_overflow = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let body_timeout = Arc::new(std::sync::atomic::AtomicBool::new(false));
    if oversize || hyper::body::Body::is_end_stream(&body) {
        drop(body_tx);
    } else {
        let overflow = body_overflow.clone();
        let timed_out = body_timeout.clone();
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
                        timed_out.store(true, Ordering::Relaxed);
                        break;
                    }
                };
                if let Ok(data) = frame.into_data() {
                    total += data.len() as u64;
                    if max_body > 0 && total > max_body as u64 {
                        // Past the cap: flag it and stop pulling. Dropping the
                        // sender unblocks read_body, which then raises.
                        overflow.store(true, Ordering::Relaxed);
                        break;
                    }
                    if body_tx.send_async(data).await.is_err() {
                        break; // request handle dropped; stop pulling
                    }
                }
            }
        });
    }

    let (head_tx, head_rx) = tokio::sync::oneshot::channel();
    let responder = Arc::new(Responder::new(head_tx));
    let ctx = Box::new(RequestCtx {
        method: parts.method,
        uri: parts.uri,
        version: parts.version,
        headers: parts.headers,
        remote_addr,
        local_addr,
        https: server.https,
        body_rx,
        body_overflow,
        body_timeout,
        leftover: None,
        slot: None,
        pin_slab: server.pin_slab.clone(),
        responder,
        enqueued_at: std::time::Instant::now(),
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

    if let (Some(log), Some((start, method, target, version))) =
        (server.access_log.as_ref(), log_meta)
    {
        let status = response.status().as_u16();
        let line = format!(
            "{} [{}] \"{method} {target} {version:?}\" {status} {:.1}ms",
            remote_addr.ip(),
            httpdate::fmt_http_date(std::time::SystemTime::now()),
            start.elapsed().as_secs_f64() * 1000.0
        );
        log.write_line(crate::style::status_colored(status, &line));
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
        server.state.store(registry::STATE_DRAINING, Ordering::Relaxed);
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
        if let Some(runtime) = server.runtime.lock().take() {
            runtime.shutdown_timeout(Duration::from_millis(timeout_ms));
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

/// Errors print in red on color terminals. Covers worker errors,
/// supervisor crash reports, and everything apps write to rack.errors.
pub fn log_error(message: String) {
    eprintln!("{}", crate::style::red(&format!("[Kino] {message}")));
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
pub fn worker_stats(
    _ruby: &Ruby,
    server_id: u64,
) -> Result<Vec<WorkerStatRow>, Error> {
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
        server.quarantine_replacements.fetch_add(1, Ordering::Relaxed);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{test_server, LANE_DEPTH};
    use crate::request::test_ctx;

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

        assert!(matches!(try_dispatch(&server, test_ctx()), Dispatch::Closed));
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
        server.slots.read()[0]
            .parked
            .store(true, Ordering::Relaxed);

        // Both dispatches land on the awake lane (slot 1), regardless of
        // where the rotating cursor starts.
        assert!(matches!(try_dispatch(&server, test_ctx()), Dispatch::Sent));
        assert!(matches!(try_dispatch(&server, test_ctx()), Dispatch::Sent));
        assert_eq!(server.lane_depths(), Some(vec![0, 2]));
    }
}
