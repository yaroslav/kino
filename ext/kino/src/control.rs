//! Read-only control plane: stats, metrics and probes served off-runtime;
//! see also server.rs for the data plane.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use parking_lot::Mutex;

use crate::registry::{ServerInner, STATE_DRAINING, STATE_READY};

/// One dispatch slot's sensors, captured for a response. busy_ms is the
/// age of the current in-flight work (0 when idle), the wedge signal.
pub struct WorkerStat {
    pub index: usize,
    pub served: u64,
    pub in_flight: usize,
    pub busy_ms: u64,
}

/// Read every slot's per-worker sensors in one pass under the slots read
/// lock (the same lock lane_depths uses), computing busy_ms against one
/// "now". No Ruby, so it is safe on the control thread and below the GVL.
pub fn collect_worker_status(server: &ServerInner) -> Vec<WorkerStat> {
    let now = crate::mono::mono_ms();
    server
        .slots
        .read()
        .iter()
        .enumerate()
        .map(|(index, slot)| {
            let in_flight = slot.in_flight.load(Ordering::Relaxed);
            let started = slot.last_started_ms.load(Ordering::Relaxed);
            WorkerStat {
                index,
                served: slot.served.load(Ordering::Relaxed),
                in_flight,
                busy_ms: if in_flight > 0 { now.saturating_sub(started) } else { 0 },
            }
        })
        .collect()
}

/// Everything the endpoints report, captured in one pass so a response
/// is internally consistent.
pub struct StatsSnapshot {
    pub mode: String,
    pub lanes: bool,
    pub workers: usize,
    pub threads: usize,
    pub batch: usize,
    pub respawns: u64,
    pub queued: usize,
    pub in_flight: usize,
    pub served: u64,
    pub rejected: u64,
    pub timeouts: u64,
    pub lane_depths: Option<Vec<usize>>,
    pub state: u8,
    pub worker_status: Vec<WorkerStat>,
}

impl StatsSnapshot {
    pub fn take(server: &ServerInner) -> Self {
        StatsSnapshot {
            mode: server.topology.mode.clone(),
            lanes: server.lanes,
            workers: server.topology.workers,
            threads: server.topology.threads,
            batch: server.topology.batch,
            respawns: server.respawns.load(Ordering::Relaxed),
            queued: server.queued(),
            in_flight: server.in_flight.load(Ordering::Relaxed),
            served: server.served.load(Ordering::Relaxed),
            rejected: server.rejected.load(Ordering::Relaxed),
            timeouts: server.timeouts.load(Ordering::Relaxed),
            lane_depths: server.lane_depths(),
            state: server.state.load(Ordering::Relaxed),
            worker_status: collect_worker_status(server),
        }
    }

    pub fn state_name(&self) -> &'static str {
        match self.state {
            STATE_READY => "ready",
            STATE_DRAINING => "draining",
            _ => "booting",
        }
    }
}

/// Same vocabulary as Server#stats (plus state and version); mode and
/// state are fixed identifiers, so no JSON escaping is needed.
pub fn stats_json(s: &StatsSnapshot) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(256);
    write!(
        out,
        r#"{{"mode":"{}","lanes":{},"workers":{},"threads":{},"batch":{},"respawns":{},"queued":{},"in_flight":{},"served":{},"rejected":{},"timeouts":{}"#,
        s.mode, s.lanes, s.workers, s.threads, s.batch, s.respawns,
        s.queued, s.in_flight, s.served, s.rejected, s.timeouts
    )
    .expect("writing to a String cannot fail");
    if let Some(depths) = &s.lane_depths {
        let list = depths.iter().map(|d| d.to_string()).collect::<Vec<_>>().join(",");
        write!(out, r#","lane_depths":[{list}]"#).expect("writing to a String cannot fail");
    }
    out.push_str(r#","worker_status":["#);
    for (i, w) in s.worker_status.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        write!(
            out,
            r#"{{"index":{},"served":{},"in_flight":{},"busy_ms":{}}}"#,
            w.index, w.served, w.in_flight, w.busy_ms
        )
        .expect("writing to a String cannot fail");
    }
    out.push(']');
    write!(
        out,
        r#","state":"{}","version":"{}"}}"#,
        s.state_name(),
        env!("CARGO_PKG_VERSION")
    )
    .expect("writing to a String cannot fail");
    out
}

pub fn metrics_text(s: &StatsSnapshot) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(1024);
    let mut metric = |name: &str, kind: &str, help: &str, value: String| {
        write!(out, "# HELP {name} {help}\n# TYPE {name} {kind}\n{name} {value}\n")
            .expect("writing to a String cannot fail");
    };
    metric("kino_requests_served_total", "counter", "Requests handed to Ruby workers.", s.served.to_string());
    metric("kino_requests_rejected_total", "counter", "Requests rejected with a 503.", s.rejected.to_string());
    metric("kino_request_timeouts_total", "counter", "Responses past the request timeout (client got a 504).", s.timeouts.to_string());
    metric("kino_worker_respawns_total", "counter", "Crashed workers respawned by the supervisor.", s.respawns.to_string());
    metric("kino_queue_depth", "gauge", "Requests waiting for a worker.", s.queued.to_string());
    metric("kino_requests_in_flight", "gauge", "Requests currently inside Ruby workers.", s.in_flight.to_string());
    metric("kino_workers", "gauge", "Configured worker count.", s.workers.to_string());
    metric("kino_threads_per_worker", "gauge", "Configured threads per worker.", s.threads.to_string());
    metric("kino_ready", "gauge", "1 when serving, 0 while booting or draining.",
        if s.state == STATE_READY { "1" } else { "0" }.to_string());
    if let Some(depths) = &s.lane_depths {
        out.push_str("# HELP kino_lane_depth Queued requests in each worker lane.\n# TYPE kino_lane_depth gauge\n");
        for (lane, depth) in depths.iter().enumerate() {
            write!(out, "kino_lane_depth{{lane=\"{lane}\"}} {depth}\n")
                .expect("writing to a String cannot fail");
        }
    }
    out.push_str("# HELP kino_worker_requests_served_total Requests handed to each dispatch slot.\n# TYPE kino_worker_requests_served_total counter\n");
    for w in &s.worker_status {
        write!(out, "kino_worker_requests_served_total{{worker=\"{}\"}} {}\n", w.index, w.served)
            .expect("writing to a String cannot fail");
    }
    out.push_str("# HELP kino_worker_in_flight Requests executing in each dispatch slot.\n# TYPE kino_worker_in_flight gauge\n");
    for w in &s.worker_status {
        write!(out, "kino_worker_in_flight{{worker=\"{}\"}} {}\n", w.index, w.in_flight)
            .expect("writing to a String cannot fail");
    }
    out.push_str("# HELP kino_worker_busy_ms Age in ms of the current in-flight request per slot (0 when idle).\n# TYPE kino_worker_busy_ms gauge\n");
    for w in &s.worker_status {
        write!(out, "kino_worker_busy_ms{{worker=\"{}\"}} {}\n", w.index, w.busy_ms)
            .expect("writing to a String cannot fail");
    }
    out
}

/// Constant-time comparison against "Bearer <token>": the length check is
/// not part of the secret, so it may return early; once the lengths match,
/// every byte of both strings is visited regardless of where they first
/// differ.
pub fn token_ok(expected: &str, authorization: Option<&str>) -> bool {
    let presented = authorization
        .and_then(|h| h.strip_prefix("Bearer "))
        .unwrap_or("");
    let e = expected.as_bytes();
    let p = presented.as_bytes();
    if e.len() != p.len() {
        return false;
    }
    let mut diff = 0u8;
    for i in 0..e.len() {
        diff |= e[i] ^ p[i];
    }
    diff == 0
}

/// (status, content type, body). The token, when configured, guards the
/// data endpoints only; orchestrator probes must never need credentials.
pub fn route(
    method: &str,
    path: &str,
    authorization: Option<&str>,
    token: Option<&str>,
    snapshot: &StatsSnapshot,
) -> (u16, &'static str, String) {
    if method != "GET" && method != "HEAD" {
        return (405, "text/plain", "method not allowed\n".to_string());
    }
    let path = path.split('?').next().unwrap_or(path);
    match path {
        "/live" => (200, "text/plain", "ok\n".to_string()),
        "/ready" => {
            if snapshot.state == STATE_READY {
                (200, "text/plain", "ok\n".to_string())
            } else {
                (503, "text/plain", format!("{}\n", snapshot.state_name()))
            }
        }
        "/stats" | "/metrics" => {
            if let Some(expected) = token {
                if !token_ok(expected, authorization) {
                    return (401, "text/plain", "unauthorized\n".to_string());
                }
            }
            if path == "/stats" {
                (200, "application/json", stats_json(snapshot))
            } else {
                (200, "text/plain; version=0.0.4", metrics_text(snapshot))
            }
        }
        _ => (404, "text/plain", "not found\n".to_string()),
    }
}

/// One request is tiny (request line plus a header or two); anything
/// larger is not a monitoring client.
const CONTROL_MAX_REQUEST_BYTES: usize = 8192;
/// Whole-connection deadline, accept to close. Keep-alive is off, so
/// this bounds exactly one request.
const CONTROL_DEADLINE: Duration = Duration::from_secs(5);
/// Concurrent monitoring connections; probes and scrapers need a
/// handful, connections past the cap are dropped at accept.
const CONTROL_MAX_CONNECTIONS: usize = 16;

pub enum ControlBind {
    Tcp(std::net::TcpListener, u16),
    Unix(std::os::unix::net::UnixListener, std::path::PathBuf),
}

/// Claim the control address. Both arms bind synchronously so a
/// conflict raises at boot, like the main listener.
pub fn bind_control(addr: &str) -> std::io::Result<ControlBind> {
    if let Some(path) = addr.strip_prefix("unix://") {
        let path = std::path::PathBuf::from(path);
        // A path that already exists is either a live listener (refuse: do
        // not steal it) or a stale file left behind by a crashed process
        // (safe to unlink and reclaim). Probe with a connect: a successful
        // connect means someone is accepting on it right now.
        match std::os::unix::net::UnixStream::connect(&path) {
            Ok(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::AddrInUse,
                    "control socket is in use",
                ));
            }
            Err(e) if e.kind() == std::io::ErrorKind::ConnectionRefused => {
                match std::fs::remove_file(&path) {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e),
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        let listener = std::os::unix::net::UnixListener::bind(&path)?;
        listener.set_nonblocking(true)?;
        Ok(ControlBind::Unix(listener, path))
    } else {
        let listener = std::net::TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;
        let port = listener.local_addr()?.port();
        Ok(ControlBind::Tcp(listener, port))
    }
}

struct ControlHandle {
    stop_tx: tokio::sync::watch::Sender<bool>,
    join: Option<std::thread::JoinHandle<()>>,
    unix_path: Option<std::path::PathBuf>,
}

static CONTROL: OnceLock<Mutex<HashMap<u64, ControlHandle>>> = OnceLock::new();

fn control_registry() -> &'static Mutex<HashMap<u64, ControlHandle>> {
    CONTROL.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Spawn the kino-control thread. Returns the TCP port (None for unix
/// sockets). The thread owns its own single-threaded runtime, so the
/// endpoints answer independently of the data plane and the GVL.
pub fn start(
    bind: ControlBind,
    server: Arc<ServerInner>,
    token: Option<String>,
) -> std::io::Result<Option<u16>> {
    let (port, unix_path) = match &bind {
        ControlBind::Tcp(_, port) => (Some(*port), None),
        ControlBind::Unix(_, path) => (None, Some(path.clone())),
    };
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let id = server.id;
    let join = match std::thread::Builder::new()
        .name("kino-control".to_string())
        .spawn(move || run(bind, server, token, stop_rx))
    {
        Ok(join) => join,
        Err(e) => {
            // The thread never started, so control_stop will never run to
            // reclaim the socket file; without this the path is left
            // behind and the next bind_control for it fails outright.
            if let Some(path) = &unix_path {
                let _ = std::fs::remove_file(path);
            }
            return Err(e);
        }
    };
    control_registry().lock().insert(
        id,
        ControlHandle { stop_tx, join: Some(join), unix_path },
    );
    Ok(port)
}

/// Stop the control thread and clean up; a no-op for unknown ids, so
/// shutdown stays idempotent. Called from Ruby after the main runtime
/// is gone (the control thread must be the last thing reporting).
pub fn control_stop(_ruby: &magnus::Ruby, server_id: u64) -> Result<(), magnus::Error> {
    let handle = control_registry().lock().remove(&server_id);
    if let Some(mut handle) = handle {
        let _ = handle.stop_tx.send(true);
        if let Some(join) = handle.join.take() {
            let _ = join.join();
        }
        if let Some(path) = handle.unix_path {
            let _ = std::fs::remove_file(path);
        }
    }
    Ok(())
}

fn run(
    bind: ControlBind,
    server: Arc<ServerInner>,
    token: Option<String>,
    stop_rx: tokio::sync::watch::Receiver<bool>,
) {
    let runtime = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(e) => return crate::server::log_error(format!("control runtime failed: {e}")),
    };
    runtime.block_on(async move {
        match bind {
            ControlBind::Tcp(listener, _) => match tokio::net::TcpListener::from_std(listener) {
                Ok(listener) => serve(TcpOrUnix::Tcp(listener), server, token, stop_rx).await,
                Err(e) => crate::server::log_error(format!("control listener failed: {e}")),
            },
            ControlBind::Unix(listener, _) => match tokio::net::UnixListener::from_std(listener) {
                Ok(listener) => serve(TcpOrUnix::Unix(listener), server, token, stop_rx).await,
                Err(e) => crate::server::log_error(format!("control listener failed: {e}")),
            },
        }
    });
}

enum TcpOrUnix {
    Tcp(tokio::net::TcpListener),
    Unix(tokio::net::UnixListener),
}

async fn serve(
    listener: TcpOrUnix,
    server: Arc<ServerInner>,
    token: Option<String>,
    mut stop_rx: tokio::sync::watch::Receiver<bool>,
) {
    let permits = Arc::new(tokio::sync::Semaphore::new(CONTROL_MAX_CONNECTIONS));
    loop {
        // Accept errors (EMFILE and friends) back off instead of
        // exiting: a dead control loop reads as a dead process to
        // liveness probes, which must never happen while we serve.
        macro_rules! conn {
            ($accepted:expr) => {
                match $accepted {
                    Ok((stream, _)) => {
                        let Ok(permit) = permits.clone().try_acquire_owned() else { continue };
                        spawn_connection(stream, permit, server.clone(), token.clone());
                    }
                    Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
                }
            };
        }
        match &listener {
            TcpOrUnix::Tcp(l) => tokio::select! {
                _ = stop_rx.changed() => return,
                accepted = l.accept() => conn!(accepted),
            },
            TcpOrUnix::Unix(l) => tokio::select! {
                _ = stop_rx.changed() => return,
                accepted = l.accept() => conn!(accepted),
            },
        }
    }
}

fn spawn_connection<S>(
    stream: S,
    permit: tokio::sync::OwnedSemaphorePermit,
    server: Arc<ServerInner>,
    token: Option<String>,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    // A panic inside the task dies with the task; the accept loop and
    // its siblings keep serving.
    tokio::spawn(async move {
        let _permit = permit;
        let service = hyper::service::service_fn(move |req| {
            let server = server.clone();
            let token = token.clone();
            async move { Ok::<_, std::convert::Infallible>(handle(&server, token.as_deref(), &req)) }
        });
        let conn = hyper::server::conn::http1::Builder::new()
            .keep_alive(false)
            .max_buf_size(CONTROL_MAX_REQUEST_BYTES)
            .serve_connection(hyper_util::rt::TokioIo::new(stream), service);
        let _ = tokio::time::timeout(CONTROL_DEADLINE, conn).await;
    });
}

fn handle(
    server: &ServerInner,
    token: Option<&str>,
    req: &hyper::Request<hyper::body::Incoming>,
) -> hyper::Response<http_body_util::Full<bytes::Bytes>> {
    let authorization = req
        .headers()
        .get(hyper::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());
    let snapshot = StatsSnapshot::take(server);
    let (status, content_type, body) =
        route(req.method().as_str(), req.uri().path(), authorization, token, &snapshot);
    let mut builder = hyper::Response::builder()
        .status(status)
        .header("content-type", content_type);
    if status == 401 {
        builder = builder.header("www-authenticate", "Bearer");
    }
    let bytes = if req.method() == hyper::Method::HEAD {
        bytes::Bytes::new()
    } else {
        bytes::Bytes::from(body)
    };
    builder
        .body(http_body_util::Full::new(bytes))
        .expect("static response parts always build")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot(state: u8) -> StatsSnapshot {
        StatsSnapshot {
            mode: "ractor".to_string(), lanes: false, workers: 8, threads: 1,
            batch: 1, respawns: 2, queued: 3, in_flight: 4, served: 100,
            rejected: 5, timeouts: 6, lane_depths: None, state, worker_status: vec![],
        }
    }

    #[test]
    fn stats_json_reports_every_field_and_the_state_name() {
        let json = stats_json(&snapshot(crate::registry::STATE_READY));
        for needle in [
            r#""mode":"ractor""#, r#""lanes":false"#, r#""workers":8"#,
            r#""threads":1"#, r#""batch":1"#, r#""respawns":2"#,
            r#""queued":3"#, r#""in_flight":4"#, r#""served":100"#,
            r#""rejected":5"#, r#""timeouts":6"#, r#""state":"ready""#,
            r#""version":""#,
        ] {
            assert!(json.contains(needle), "missing {needle} in {json}");
        }
        assert!(!json.contains("lane_depths"));
    }

    #[test]
    fn stats_json_includes_lane_depths_when_lanes_are_on() {
        let mut s = snapshot(crate::registry::STATE_READY);
        s.lane_depths = Some(vec![1, 0]);
        assert!(stats_json(&s).contains(r#""lane_depths":[1,0]"#));
    }

    #[test]
    fn metrics_text_is_prometheus_shaped() {
        let text = metrics_text(&snapshot(crate::registry::STATE_READY));
        assert!(text.contains("# TYPE kino_requests_served_total counter"));
        assert!(text.contains("kino_requests_served_total 100"));
        assert!(text.contains("kino_ready 1"));
        let draining = metrics_text(&snapshot(crate::registry::STATE_DRAINING));
        assert!(draining.contains("kino_ready 0"));
    }

    #[test]
    fn metrics_text_includes_lane_depth_samples_when_lanes_are_on() {
        let mut s = snapshot(crate::registry::STATE_READY);
        s.lane_depths = Some(vec![2, 0]);
        let text = metrics_text(&s);
        assert!(text.contains("# HELP kino_lane_depth Queued requests in each worker lane."));
        assert!(text.contains("# TYPE kino_lane_depth gauge"));
        assert!(text.contains(r#"kino_lane_depth{lane="0"} 2"#));
        assert!(text.contains(r#"kino_lane_depth{lane="1"} 0"#));
    }

    #[test]
    fn token_check_wants_the_exact_bearer_token() {
        assert!(token_ok("s3cret", Some("Bearer s3cret")));
        assert!(!token_ok("s3cret", Some("Bearer wrong")));
        assert!(!token_ok("s3cret", Some("s3cret")));
        assert!(!token_ok("s3cret", None));
        assert!(!token_ok("s3cret", Some("Bearer s3cret-and-more")));
    }

    #[test]
    fn token_check_rejects_nul_padded_forgeries() {
        // A truncating length comparison (e.g. casting the XOR of lengths to
        // u8) would wrap a 256-byte overage back to zero and let NUL padding
        // stand in for the missing bytes; the exact-length check must catch
        // both a large and a minimal version of that forgery.
        let padded = format!("Bearer s3cret{}", "\0".repeat(256));
        assert!(!token_ok("s3cret", Some(&padded)));
        assert!(!token_ok("s3cret", Some("Bearer s3cret\0")));
    }

    #[test]
    fn routing_matrix() {
        let ready = snapshot(crate::registry::STATE_READY);
        assert_eq!(route("GET", "/live", None, None, &ready).0, 200);
        assert_eq!(route("HEAD", "/live", None, None, &ready).0, 200);
        assert_eq!(route("GET", "/ready", None, None, &ready).0, 200);
        assert_eq!(route("GET", "/stats", None, None, &ready).0, 200);
        assert_eq!(route("GET", "/metrics", None, None, &ready).0, 200);
        assert_eq!(route("GET", "/nope", None, None, &ready).0, 404);
        assert_eq!(route("POST", "/stats", None, None, &ready).0, 405);
        assert_eq!(route("GET", "/stats?x=1", None, None, &ready).0, 200);

        let booting = snapshot(crate::registry::STATE_BOOTING);
        let (code, _, body) = route("GET", "/ready", None, None, &booting);
        assert_eq!((code, body.as_str()), (503, "booting\n"));

        // The token guards stats and metrics; the probes stay open.
        assert_eq!(route("GET", "/stats", None, Some("t"), &ready).0, 401);
        assert_eq!(route("GET", "/metrics", Some("Bearer t"), Some("t"), &ready).0, 200);
        assert_eq!(route("GET", "/ready", None, Some("t"), &ready).0, 200);
        assert_eq!(route("GET", "/live", None, Some("t"), &ready).0, 200);
    }

    #[test]
    fn stats_json_emits_worker_status_array() {
        let mut s = snapshot(crate::registry::STATE_READY);
        s.worker_status = vec![
            WorkerStat { index: 0, served: 10, in_flight: 1, busy_ms: 4 },
            WorkerStat { index: 1, served: 7, in_flight: 0, busy_ms: 0 },
        ];
        let json = stats_json(&s);
        assert!(json.contains(r#""worker_status":[{"index":0,"served":10,"in_flight":1,"busy_ms":4},{"index":1,"served":7,"in_flight":0,"busy_ms":0}]"#), "got {json}");
    }

    #[test]
    fn stats_json_worker_status_is_empty_array_with_no_slots() {
        let s = snapshot(crate::registry::STATE_READY);
        assert!(stats_json(&s).contains(r#""worker_status":[]"#));
    }

    #[test]
    fn metrics_text_emits_per_worker_series() {
        let mut s = snapshot(crate::registry::STATE_READY);
        s.worker_status = vec![
            WorkerStat { index: 0, served: 10, in_flight: 1, busy_ms: 4 },
            WorkerStat { index: 1, served: 7, in_flight: 0, busy_ms: 0 },
        ];
        let text = metrics_text(&s);
        assert!(text.contains("# TYPE kino_worker_requests_served_total counter"));
        assert!(text.contains(r#"kino_worker_requests_served_total{worker="0"} 10"#));
        assert!(text.contains(r#"kino_worker_in_flight{worker="1"} 0"#));
        assert!(text.contains("# TYPE kino_worker_busy_ms gauge"));
        assert!(text.contains(r#"kino_worker_busy_ms{worker="0"} 4"#));
    }
}
