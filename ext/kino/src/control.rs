//! Read-only control plane: stats, metrics and probes served off-runtime;
//! the data plane is server/, and what the endpoints report is stats.rs.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use parking_lot::Mutex;

use crate::registry::{ServerInner, STATE_READY};
use crate::stats::Snapshot;

/// Constant-time comparison against "Bearer <token>": the length check is
/// not part of the secret, so it may return early; once the lengths match,
/// every byte of both strings is visited regardless of where they first
/// differ.
fn token_ok(expected: &str, authorization: Option<&str>) -> bool {
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

/// (status, content type, body) for a request to `path`, the URI's path alone. The token, when
/// configured, guards the data endpoints only; orchestrator probes must never need credentials.
fn route(
    method: &str,
    path: &str,
    authorization: Option<&str>,
    token: Option<&str>,
    snapshot: &Snapshot,
) -> (u16, &'static str, String) {
    if method != "GET" && method != "HEAD" {
        return (405, "text/plain", "method not allowed\n".to_string());
    }
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
                (200, "application/json", snapshot.json())
            } else {
                (200, "text/plain; version=0.0.4", snapshot.metrics())
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
/// The TCP control listener's accept queue, pinned at std's 128: monitors connect a few at a
/// time, and past it, at a stuck control thread, a probe fails at connect rather than wait on a
/// handshake nobody answers. A unix control socket keeps the kernel's maximum: on Linux a full
/// unix queue holds a blocking connect rather than fail it.
const CONTROL_BACKLOG: libc::c_int = 128;

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
        let _ = crate::listen::set_backlog(&listener, CONTROL_BACKLOG);
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
        ControlHandle {
            stop_tx,
            join,
            unix_path,
        },
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
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => return native_error(format!("control runtime failed: {e}")),
    };
    runtime.block_on(async move {
        match bind {
            ControlBind::Tcp(listener, _) => match tokio::net::TcpListener::from_std(listener) {
                Ok(listener) => serve(TcpOrUnix::Tcp(listener), server, token, stop_rx).await,
                Err(e) => native_error(format!("control listener failed: {e}")),
            },
            ControlBind::Unix(listener, _) => match tokio::net::UnixListener::from_std(listener) {
                Ok(listener) => serve(TcpOrUnix::Unix(listener), server, token, stop_rx).await,
                Err(e) => native_error(format!("control listener failed: {e}")),
            },
        }
    });
}

/// A failure inside the native layer itself, reported as the "native"
/// source: no Ruby ractor or thread spoke.
fn native_error(message: String) {
    crate::log::emit(crate::log::Level::Error, "native", &message);
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
                    let Ok(permit) = permits.clone().try_acquire_owned() else {
                        continue;
                    };
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
    let snapshot = Snapshot::take(server);
    let (status, content_type, body) = route(
        req.method().as_str(),
        req.uri().path(),
        authorization,
        token,
        &snapshot,
    );
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

    // The rest of the routes, the probes' states and the token's guard: spec/control_plane_spec.rb.
    #[test]
    fn a_head_is_routed_as_a_get_and_the_token_guards_the_metrics_too() {
        let ready = Snapshot {
            state: STATE_READY,
            ..Snapshot::default()
        };
        assert_eq!(route("HEAD", "/live", None, None, &ready).0, 200);
        let token = Some("t");
        assert_eq!(route("GET", "/metrics", None, token, &ready).0, 401);
        assert_eq!(
            route("GET", "/metrics", Some("Bearer t"), token, &ready).0,
            200
        );
    }
}
