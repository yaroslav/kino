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
    pub quarantined: bool,
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
            let quarantined = slot.quarantined.load(Ordering::Relaxed);
            let in_flight = slot.in_flight.load(Ordering::Relaxed);
            let started = slot.last_started_ms.load(Ordering::Relaxed);
            WorkerStat {
                index,
                served: slot.served.load(Ordering::Relaxed),
                in_flight,
                // A quarantined slot is a known, handled wedge: report 0 so
                // it never re-trips detection or reads as a live wedge.
                busy_ms: if quarantined || in_flight == 0 {
                    0
                } else {
                    now.saturating_sub(started)
                },
                quarantined,
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
    pub quarantined_count: usize,
    pub quarantine_replacements: u64,
    pub queue_histogram: crate::registry::QueueHistogramSnapshot,
}

impl StatsSnapshot {
    pub fn take(server: &ServerInner) -> Self {
        let worker_status = collect_worker_status(server);
        let quarantined_count = worker_status.iter().filter(|w| w.quarantined).count();
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
            worker_status,
            quarantined_count,
            quarantine_replacements: server.quarantine_replacements.load(Ordering::Relaxed),
            queue_histogram: server.queue_histogram.snapshot(),
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
            r#"{{"index":{},"served":{},"in_flight":{},"busy_ms":{},"quarantined":{}}}"#,
            w.index, w.served, w.in_flight, w.busy_ms, w.quarantined
        )
        .expect("writing to a String cannot fail");
    }
    out.push(']');
    write!(
        out,
        r#","queue_time":{{"count":{},"sum_seconds":{}}}"#,
        s.queue_histogram.count,
        s.queue_histogram.sum_seconds()
    )
    .expect("writing to a String cannot fail");
    write!(
        out,
        r#","quarantined":{},"state":"{}","version":"{}"}}"#,
        s.quarantined_count, s.state_name(),
        env!("CARGO_PKG_VERSION")
    )
    .expect("writing to a String cannot fail");
    out
}

/// One HELP/TYPE/sample triple for a single-value metric.
fn metric(out: &mut String, name: &str, kind: &str, help: &str, value: impl std::fmt::Display) {
    use std::fmt::Write;
    writeln!(out, "# HELP {name} {help}\n# TYPE {name} {kind}\n{name} {value}")
        .expect("writing to a String cannot fail");
}

/// One HELP/TYPE header followed by one `name{label="key"} value` line per
/// row, for the labeled per-lane and per-worker series.
fn series<K: std::fmt::Display, V: std::fmt::Display>(
    out: &mut String,
    name: &str,
    kind: &str,
    help: &str,
    label: &str,
    rows: impl Iterator<Item = (K, V)>,
) {
    use std::fmt::Write;
    writeln!(out, "# HELP {name} {help}\n# TYPE {name} {kind}").expect("writing to a String cannot fail");
    for (key, value) in rows {
        writeln!(out, "{name}{{{label}=\"{key}\"}} {value}").expect("writing to a String cannot fail");
    }
}

/// Prometheus text exposition (version 0.0.4) for every stat in `s`.
pub fn metrics_text(s: &StatsSnapshot) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(1024);
    metric(&mut out, "kino_requests_served_total", "counter", "Requests handed to Ruby workers.", s.served);
    metric(&mut out, "kino_requests_rejected_total", "counter", "Requests rejected with a 503.", s.rejected);
    metric(&mut out, "kino_request_timeouts_total", "counter", "Responses past the request timeout (client got a 504).", s.timeouts);
    metric(&mut out, "kino_worker_respawns_total", "counter", "Crashed workers respawned by the supervisor.", s.respawns);
    metric(&mut out, "kino_queue_depth", "gauge", "Requests waiting for a worker.", s.queued);
    metric(&mut out, "kino_requests_in_flight", "gauge", "Requests currently inside Ruby workers.", s.in_flight);
    metric(&mut out, "kino_workers", "gauge", "Configured worker count.", s.workers);
    metric(&mut out, "kino_threads_per_worker", "gauge", "Configured threads per worker.", s.threads);
    metric(&mut out, "kino_ready", "gauge", "1 when serving, 0 while booting or draining.",
        if s.state == STATE_READY { "1" } else { "0" });
    if let Some(depths) = &s.lane_depths {
        series(&mut out, "kino_lane_depth", "gauge", "Queued requests in each worker lane.", "lane",
            depths.iter().enumerate().map(|(lane, depth)| (lane, *depth)));
    }
    series(&mut out, "kino_worker_requests_served_total", "counter", "Requests handed to each dispatch slot.", "worker",
        s.worker_status.iter().map(|w| (w.index, w.served)));
    series(&mut out, "kino_worker_in_flight", "gauge", "Requests executing in each dispatch slot.", "worker",
        s.worker_status.iter().map(|w| (w.index, w.in_flight)));
    series(&mut out, "kino_worker_busy_ms", "gauge", "Age in ms of the current in-flight request per slot (0 when idle).", "worker",
        s.worker_status.iter().map(|w| (w.index, w.busy_ms)));
    metric(&mut out, "kino_quarantined_workers", "gauge", "Dispatch slots abandoned as wedged.", s.quarantined_count);
    metric(&mut out, "kino_quarantine_replacements_total", "counter", "Replacement workers spawned after a wedge.", s.quarantine_replacements);
    let h = &s.queue_histogram;
    out.push_str("# HELP kino_request_queue_seconds Seconds requests waited in the queue before a worker admitted them.\n# TYPE kino_request_queue_seconds histogram\n");
    let mut cumulative = 0u64;
    for (i, bound_us) in crate::registry::QUEUE_BOUNDS_US.iter().enumerate() {
        cumulative += h.buckets[i];
        let le = *bound_us as f64 / 1_000_000.0;
        writeln!(out, "kino_request_queue_seconds_bucket{{le=\"{le}\"}} {cumulative}")
            .expect("writing to a String cannot fail");
    }
    let total = cumulative + h.overflow;
    writeln!(out, "kino_request_queue_seconds_bucket{{le=\"+Inf\"}} {total}")
        .expect("writing to a String cannot fail");
    writeln!(out, "kino_request_queue_seconds_sum {}", h.sum_seconds())
        .expect("writing to a String cannot fail");
    writeln!(out, "kino_request_queue_seconds_count {total}")
        .expect("writing to a String cannot fail");
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

/// A bound control listener: TCP with its resolved port, or a unix socket
/// with its path.
pub enum ControlBind {
    Tcp(std::net::TcpListener, u16),
    Unix(std::os::unix::net::UnixListener, std::path::PathBuf),
}

/// Claim the control address. Both arms bind synchronously so a
/// conflict raises at boot, like the main listener.
pub fn bind_control(addr: &str) -> std::io::Result<ControlBind> {
    if let Some(path) = crate::listen::unix_path(addr) {
        let listener = crate::listen::bind_unix(path)?;
        Ok(ControlBind::Unix(listener, path.to_path_buf()))
    } else {
        let listener = std::net::TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;
        let port = listener.local_addr()?.port();
        Ok(ControlBind::Tcp(listener, port))
    }
}

struct ControlHandle {
    stop_tx: tokio::sync::watch::Sender<bool>,
    join: std::thread::JoinHandle<()>,
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
        ControlHandle { stop_tx, join, unix_path },
    );
    Ok(port)
}

/// Stop the control thread and clean up; a no-op for unknown ids, so
/// shutdown stays idempotent. Called from Ruby after the main runtime
/// is gone (the control thread must be the last thing reporting).
pub fn control_stop(_ruby: &magnus::Ruby, server_id: u64) -> Result<(), magnus::Error> {
    let handle = control_registry().lock().remove(&server_id);
    if let Some(handle) = handle {
        let _ = handle.stop_tx.send(true);
        let _ = handle.join.join();
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
    // Accept errors (EMFILE and friends) back off instead of exiting: a dead
    // control loop reads as a dead process to liveness probes, which must
    // never happen while we serve.
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
    loop {
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
            quarantined_count: 0, quarantine_replacements: 0,
            queue_histogram: crate::registry::QueueHistogramSnapshot { buckets: [0; crate::registry::QUEUE_BOUNDS_US.len()], overflow: 0, sum_us: 0, count: 0 },
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
            WorkerStat { index: 0, served: 10, in_flight: 1, busy_ms: 4, quarantined: false },
            WorkerStat { index: 1, served: 7, in_flight: 0, busy_ms: 0, quarantined: false },
        ];
        let json = stats_json(&s);
        assert!(json.contains(r#""worker_status":[{"index":0,"served":10,"in_flight":1,"busy_ms":4,"quarantined":false},{"index":1,"served":7,"in_flight":0,"busy_ms":0,"quarantined":false}]"#), "got {json}");
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
            WorkerStat { index: 0, served: 10, in_flight: 1, busy_ms: 4, quarantined: false },
            WorkerStat { index: 1, served: 7, in_flight: 0, busy_ms: 0, quarantined: false },
        ];
        let text = metrics_text(&s);
        assert!(text.contains("# TYPE kino_worker_requests_served_total counter"));
        assert!(text.contains(r#"kino_worker_requests_served_total{worker="0"} 10"#));
        assert!(text.contains(r#"kino_worker_in_flight{worker="1"} 0"#));
        assert!(text.contains("# TYPE kino_worker_busy_ms gauge"));
        assert!(text.contains(r#"kino_worker_busy_ms{worker="0"} 4"#));
    }

    #[test]
    fn quarantined_slot_reports_zero_busy_ms() {
        let server = crate::registry::test_server(false, 4);
        server.register_worker();
        {
            let slots = server.slots.read();
            slots[0].in_flight.store(1, std::sync::atomic::Ordering::Relaxed);
            slots[0].last_started_ms.store(0, std::sync::atomic::Ordering::Relaxed);
            slots[0].quarantined.store(true, std::sync::atomic::Ordering::Relaxed);
        }
        let rows = collect_worker_status(&server);
        assert!(rows[0].quarantined);
        assert_eq!(rows[0].busy_ms, 0);
    }

    #[test]
    fn stats_json_reports_quarantine() {
        let mut s = snapshot(crate::registry::STATE_READY);
        s.quarantined_count = 1;
        s.worker_status = vec![
            WorkerStat { index: 0, served: 3, in_flight: 1, busy_ms: 0, quarantined: true },
            WorkerStat { index: 1, served: 9, in_flight: 1, busy_ms: 5, quarantined: false },
        ];
        let json = stats_json(&s);
        assert!(json.contains(r#""quarantined":1"#), "top-level count: {json}");
        assert!(json.contains(r#"{"index":0,"served":3,"in_flight":1,"busy_ms":0,"quarantined":true}"#), "{json}");
        assert!(json.contains(r#""quarantined":false"#));
    }

    #[test]
    fn metrics_text_reports_quarantine() {
        let mut s = snapshot(crate::registry::STATE_READY);
        s.quarantined_count = 2;
        s.quarantine_replacements = 7;
        let text = metrics_text(&s);
        assert!(text.contains("# TYPE kino_quarantined_workers gauge"));
        assert!(text.contains("kino_quarantined_workers 2"));
        assert!(text.contains("# TYPE kino_quarantine_replacements_total counter"));
        assert!(text.contains("kino_quarantine_replacements_total 7"));
    }

    #[test]
    fn metrics_text_emits_a_cumulative_queue_histogram() {
        let mut s = snapshot(crate::registry::STATE_READY);
        let mut buckets = [0u64; crate::registry::QUEUE_BOUNDS_US.len()];
        buckets[0] = 3; // <= 0.0005s
        buckets[2] = 1; // <= 0.0025s
        s.queue_histogram = crate::registry::QueueHistogramSnapshot {
            buckets, overflow: 1, sum_us: 3 * 100 + 2_000 + 20_000_000, count: 5,
        };
        let text = metrics_text(&s);
        assert!(text.contains("# TYPE kino_request_queue_seconds histogram"));
        assert!(text.contains(r#"kino_request_queue_seconds_bucket{le="0.0005"} 3"#));
        // cumulative: le=0.0025 includes buckets 0..=2 = 3 + 0 + 1 = 4
        assert!(text.contains(r#"kino_request_queue_seconds_bucket{le="0.0025"} 4"#));
        assert!(text.contains(r#"kino_request_queue_seconds_bucket{le="+Inf"} 5"#));
        assert!(text.contains("kino_request_queue_seconds_count 5"));
        assert!(text.contains("kino_request_queue_seconds_sum "));
    }

    #[test]
    fn stats_json_reports_queue_time() {
        let mut s = snapshot(crate::registry::STATE_READY);
        s.queue_histogram = crate::registry::QueueHistogramSnapshot {
            buckets: [0; crate::registry::QUEUE_BOUNDS_US.len()], overflow: 0, sum_us: 1_500_000, count: 2,
        };
        let json = stats_json(&s);
        assert!(json.contains(r#""queue_time":{"count":2,"sum_seconds":1.5}"#), "{json}");
    }
}
