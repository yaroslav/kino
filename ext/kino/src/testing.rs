//! What the crate's tests share: the one test server, a client for the
//! connections it serves, the lock that runs the tests reading
//! process-wide counters one at a time, and polling for what another
//! thread or task does.

use std::fmt::Display;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::Full;
use hyper::client::conn::http1::SendRequest;
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::registry::{BoxedCtx, ServerInner};
use crate::server::{ServerConfig, Topology};
use crate::slot::{WorkerSlot, WS_INBOX};

/// Tests that read process-wide counters (sockets, bridges, cut
/// streams) run one at a time.
pub static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// How long `eventually` keeps polling before the test fails.
const PATIENCE: Duration = Duration::from_secs(5);

/// Poll `ready` every millisecond, the way a worker keeps coming back to
/// its queue, until it yields; panics naming `what` after five seconds,
/// of the runtime's clock (a paused one advances while this sleeps).
/// `what` is rendered only then, so a `fmt::from_fn` can show what the
/// test last saw.
pub async fn eventually<T>(what: impl Display, mut ready: impl FnMut() -> Option<T>) -> T {
    let deadline = tokio::time::Instant::now() + PATIENCE;
    loop {
        if let Some(value) = ready() {
            return value;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// `eventually`, for a condition.
pub async fn until(what: impl Display, mut done: impl FnMut() -> bool) {
    eventually(what, || done().then_some(())).await
}

/// `eventually` outside a runtime: the calling thread sleeps between
/// polls.
pub fn eventually_blocking<T>(what: impl Display, mut ready: impl FnMut() -> Option<T>) -> T {
    let deadline = std::time::Instant::now() + PATIENCE;
    loop {
        if let Some(value) = ready() {
            return value;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// Run `work` on its own thread and fail naming `what` if it has not
/// returned within the patience. A call that blocks a runtime thread
/// cannot be cut short by `tokio::time::timeout`; this can. A stuck
/// thread is left behind, the test fails.
pub fn finishes_in_time(what: &str, work: impl FnOnce() + Send + 'static) {
    let (done, finished) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        work();
        let _ = done.send(());
    });
    finished
        .recv_timeout(PATIENCE)
        .unwrap_or_else(|_| panic!("{what} blocked for over {PATIENCE:?}"));
}

/// SERIAL, taken outside a runtime once no socket of an earlier test is
/// open (its runtime may still be dropping them).
pub fn serial_blocking() -> tokio::sync::MutexGuard<'static, ()> {
    let serial = SERIAL.blocking_lock();
    eventually_blocking("the sockets of earlier tests to end", || {
        (crate::ws::snapshot().open == 0).then_some(())
    });
    serial
}

/// The worker slot at `index` of `server`.
pub fn slot(server: &ServerInner, index: usize) -> Arc<WorkerSlot> {
    server.slots.read()[index].clone()
}

/// The dispatcher's part: a request lands in `slot`'s lane.
pub fn assign(slot: &WorkerSlot) {
    let request = crate::registry::LaneItem::Request(crate::request::test_ctx());
    assert!(slot.send_lane(request).is_ok(), "lane open, with room");
}

/// A ServerInner with no runtime of its own: lane dispatch or the shared
/// queue, 4 deep, a 10 ms queue timeout, and `workers` worker slots.
/// Ids are unique, so tests that register theirs can run in parallel.
pub struct TestServer {
    config: ServerConfig,
    workers: usize,
    inbox: usize,
}

impl TestServer {
    /// Requests go to each worker's lane.
    pub fn lanes() -> Self {
        Self::new(true)
    }

    /// Requests wait in the shared queue.
    pub fn shared_queue() -> Self {
        Self::new(false)
    }

    fn new(lanes: bool) -> Self {
        TestServer {
            config: ServerConfig {
                queue_depth: 4,
                queue_timeout_ms: 10,
                http2: true,
                lanes,
                topology: Topology {
                    mode: "threaded".to_string(),
                    batch: 1,
                    ..Topology::default()
                },
                ..ServerConfig::default()
            },
            workers: 0,
            inbox: WS_INBOX,
        }
    }

    pub fn workers(mut self, count: usize) -> Self {
        self.workers = count;
        self
    }

    /// Each worker's WebSocket inbox holds `events`.
    pub fn inbox(mut self, events: usize) -> Self {
        self.inbox = events;
        self
    }

    /// The WebSocket budgets, as `set` changes the defaults.
    pub fn ws(mut self, set: impl FnOnce(&mut crate::ws::Limits)) -> Self {
        let mut limits = crate::ws::Limits::default();
        set(&mut limits);
        self.config.ws = Arc::new(limits);
        self
    }

    /// Any other setting.
    pub fn with(mut self, set: impl FnOnce(&mut ServerConfig)) -> Self {
        set(&mut self.config);
        self
    }

    pub fn build(self) -> Arc<ServerInner> {
        let lanes = self.config.lanes;
        let server = Arc::new(ServerInner::new(self.config));
        for _ in 0..self.workers {
            let slot = WorkerSlot::new(lanes, self.inbox);
            server.slots.write().push(Arc::new(slot));
        }
        server
    }

    /// Built for a test that serves connections, once no socket of an
    /// earlier one is open (its runtime may still be dropping them), and
    /// registered as a running server is, its bridges on this test's
    /// runtime.
    pub async fn running(self) -> (Arc<ServerInner>, Running) {
        let serial = SERIAL.lock().await;
        until("the sockets of earlier tests to end", || {
            crate::ws::snapshot().open == 0
        })
        .await;
        let server = self.build();
        crate::registry::insert(server.clone());
        let id = server.id;
        (
            server,
            Running {
                _serial: serial,
                id,
            },
        )
    }
}

/// What a test serving connections holds until it ends: the lock that
/// keeps other such tests out, as they read process-wide counts, and its
/// server's registration.
pub struct Running {
    _serial: tokio::sync::MutexGuard<'static, ()>,
    id: u64,
}

impl Drop for Running {
    fn drop(&mut self) {
        crate::registry::remove(self.id);
    }
}

/// A client's response, still on its way.
pub type Pending = tokio::task::JoinHandle<hyper::Result<hyper::Response<hyper::body::Incoming>>>;

/// A GET of `path` with no body.
pub fn get(path: &str) -> hyper::Request<Full<Bytes>> {
    hyper::Request::get(path)
        .header(http::header::HOST, "kino.test")
        .body(Full::new(Bytes::new()))
        .expect("request")
}

/// An HTTP/2 GET of `path`: no Host header, its authority in the URI.
pub fn h2_get(path: &str) -> hyper::Request<Full<Bytes>> {
    let uri = format!("http://kino.test:8443{path}");
    hyper::Request::get(uri)
        .body(Full::new(Bytes::new()))
        .expect("request")
}

/// A request asking to switch to a protocol Kino knows nothing about.
pub fn upgrade_request() -> hyper::Request<Full<Bytes>> {
    hyper::Request::get("/chat")
        .header(http::header::HOST, "kino.test")
        .header(http::header::UPGRADE, "chat")
        .header(http::header::CONNECTION, "Upgrade")
        .body(Full::new(Bytes::new()))
        .expect("upgrade request")
}

/// An app's hijacked answer switching to that protocol.
pub const SWITCHED: &[u8] =
    b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: chat\r\nConnection: Upgrade\r\n\r\n";

/// An HTTP/1.1 client connection to `server`, served the production way.
pub async fn connect(server: &Arc<ServerInner>) -> SendRequest<Full<Bytes>> {
    let io = TokioIo::new(crate::server::testing::spawn_conn(server.clone()));
    let (sender, conn) = hyper::client::conn::http1::handshake(io)
        .await
        .expect("h1 handshake");
    tokio::spawn(conn.with_upgrades());
    sender
}

/// A client sends `request` on a connection of its own.
pub async fn send(server: &Arc<ServerInner>, request: hyper::Request<Full<Bytes>>) -> Pending {
    let mut sender = connect(server).await;
    tokio::spawn(async move { sender.send_request(request).await })
}

/// The next request in the shared queue, as a worker takes it.
pub async fn take_ctx(server: &ServerInner) -> BoxedCtx {
    server.req_rx.recv_async().await.expect("a queued request")
}

/// The response `pending` waits for; panics after five seconds.
pub async fn response_to(pending: Pending) -> hyper::Response<hyper::body::Incoming> {
    tokio::time::timeout(PATIENCE, pending)
        .await
        .expect("no response within 5s")
        .expect("join")
        .expect("a response")
}

/// An HTTP/1 head off the wire, up to its blank line; panics after five
/// seconds.
pub async fn read_head(io: &mut (impl AsyncRead + Unpin)) -> String {
    let mut head = Vec::new();
    let read = async {
        while !head.ends_with(b"\r\n\r\n") {
            head.push(io.read_u8().await.expect("the rest of a head"));
        }
    };
    tokio::time::timeout(PATIENCE, read)
        .await
        .expect("no head within 5s");
    String::from_utf8(head).expect("an ASCII head")
}

/// Everything until the other end closes; panics naming `what` if it
/// stays open past five seconds.
pub async fn read_to_close(io: &mut (impl AsyncRead + Unpin + ?Sized), what: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    tokio::time::timeout(PATIENCE, io.read_to_end(&mut bytes))
        .await
        .unwrap_or_else(|_| panic!("{what}: still open after 5s"))
        .unwrap_or_else(|e| panic!("{what}: {e}"));
    bytes
}
