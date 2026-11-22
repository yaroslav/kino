//! The inline tests' harness: servers that serve inline or leave every
//! connection to hyper, loopback clients connected to an inline
//! connection or to one the accept loop serves, the app's and hyper's
//! sides of a request, and a client that reads what comes back (Wire,
//! Event).

use std::io::{ErrorKind, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::fd::{AsFd, RawFd};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio_util::either::Either;

use super::{InlineConn, Next};
use crate::registry::{BoxedCtx, ServerInner};
use crate::request::RequestCtx;
use crate::server::{Held, Peer};
use crate::slot::WorkerSlot;
pub use crate::testing::permit;
use crate::testing::{eventually, eventually_blocking, TestServer};

/// A classic (queue) server with inline serving on and one worker slot;
/// hyper's requests land in its queue, and hand-backs run on the
/// caller's runtime.
pub fn server() -> Arc<ServerInner> {
    configured_server(|_| {})
}

/// server(), as `set` changes its config.
pub fn configured_server(set: impl FnOnce(&mut crate::server::ServerConfig)) -> Arc<ServerInner> {
    let test = TestServer::shared_queue().workers(1).with(|c| {
        c.queue_depth = 8;
        c.inline = true;
        set(c);
    });
    let server = test.build();
    let _ = server.io_runtime.set(tokio::runtime::Handle::current());
    server
}

/// A worker_loop server serving inline, with `workers` workers that each
/// run up to `capacity` requests at once (their serve started).
pub fn inline_loop_server(workers: usize, capacity: usize) -> Arc<ServerInner> {
    loop_server_as(workers, capacity, |test| test)
}

/// inline_loop_server, as `adjust` changes its TestServer.
pub fn loop_server_as(
    workers: usize,
    capacity: usize,
    adjust: impl FnOnce(TestServer) -> TestServer,
) -> Arc<ServerInner> {
    let test = TestServer::worker_loop().workers(workers).with(|c| {
        c.queue_depth = 8;
        c.inline = true;
    });
    let server = adjust(test).build();
    for slot in server.slots.read().iter() {
        slot.load.capacity.store(capacity, Ordering::Relaxed);
    }
    let _ = server.io_runtime.set(tokio::runtime::Handle::current());
    server
}

/// `count` connections as an accept loop has them, each with its client
/// and what it holds, ready to place.
pub async fn accepted(count: usize) -> Vec<(TcpStream, tokio::net::TcpStream, Held, Peer)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let mut accepted = Vec::with_capacity(count);
    for _ in 0..count {
        let client = TcpStream::connect(addr).expect("connect");
        client
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("read timeout");
        let (stream, remote) = listener.accept().await.expect("accept");
        let local = stream.local_addr().expect("local addr");
        let peer = Peer { remote, local };
        accepted.push((client, stream, Held::new(permit(), None), peer));
    }
    accepted
}

/// One connection as accepted(1) has it, placed on `server` as the
/// accept loop places it: its client.
pub async fn placed(server: &Arc<ServerInner>) -> TcpStream {
    let (client, stream, held, peer) = accepted(1).await.pop().expect("one");
    assert!(super::place(server, Either::Left(stream), held, peer).is_ok());
    client
}

/// placed(), on the first worker: its siblings have no room meanwhile.
pub async fn placed_on_first(server: &Arc<ServerInner>) -> TcpStream {
    let siblings = server.slots.read()[1..].to_vec();
    let room = |slot: &Arc<WorkerSlot>| slot.load.capacity.swap(0, Ordering::Relaxed);
    let rooms: Vec<usize> = siblings.iter().map(room).collect();
    let client = placed(server).await;
    for (slot, room) in siblings.iter().zip(rooms) {
        slot.load.capacity.store(room, Ordering::Relaxed);
    }
    client
}

/// `slot`'s worker is given up on as wedged.
pub fn wedge(slot: &WorkerSlot) {
    assert!(slot.quarantine_at(slot.life.generation.load(Ordering::SeqCst)));
}

/// A connected client, and the server end as an inline connection of the
/// first worker slot, accepted on the caller's runtime.
pub fn loopback(server: &Arc<ServerInner>) -> (TcpStream, Box<InlineConn>) {
    loopback_holding(server, permit())
}

/// loopback, the connection holding `permit` as its max_connections one.
pub fn loopback_holding(
    server: &Arc<ServerInner>,
    permit: tokio::sync::OwnedSemaphorePermit,
) -> (TcpStream, Box<InlineConn>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let client = TcpStream::connect(listener.local_addr().expect("addr")).expect("connect");
    let (stream, _) = listener.accept().expect("accept");
    inline_conn(server, permit, client, stream)
}

/// loopback, with the socket buffers between the client and the
/// connection narrowed to 4 KiB, so an answer of any size waits on its
/// client's reads. Narrowed before the handshake for Linux, which keeps
/// the window and segment size it set up (narrowed later, an answer
/// crawls out a window probe at a time), and again after it for macOS,
/// which sizes both buffers anew at the handshake.
pub fn narrowed(server: &Arc<ServerInner>) -> (TcpStream, Box<InlineConn>) {
    use socket2::{Domain, SockRef, Socket, Type};

    let sends = |socket: SockRef<'_>| socket.set_send_buffer_size(4096).expect("narrowed");
    let reads = |socket: SockRef<'_>| socket.set_recv_buffer_size(4096).expect("narrowed");
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    // The connection's socket, once accepted, takes the listener's.
    sends(SockRef::from(&listener));
    let socket = Socket::new(Domain::IPV4, Type::STREAM, None).expect("a socket");
    reads(SockRef::from(&socket));
    socket
        .connect(&listener.local_addr().expect("addr").into())
        .expect("connect");
    let client = TcpStream::from(socket);
    let (stream, _) = listener.accept().expect("accept");
    reads(SockRef::from(&client));
    sends(SockRef::from(&stream));
    inline_conn(server, permit(), client, stream)
}

/// `client`'s connection, the server's end `stream`, as an inline
/// connection of the first worker slot holding `permit`.
fn inline_conn(
    server: &Arc<ServerInner>,
    permit: tokio::sync::OwnedSemaphorePermit,
    client: TcpStream,
    stream: TcpStream,
) -> (TcpStream, Box<InlineConn>) {
    // A test that hangs fails instead, on its next read.
    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("read timeout");
    stream.set_nonblocking(true).expect("nonblocking");
    let peer = Peer {
        remote: stream.peer_addr().expect("remote addr"),
        local: stream.local_addr().expect("local addr"),
    };
    let (seat, held) = (
        super::take_seat(&server.slots.read()[0]),
        Held::new(permit, None),
    );
    let home = tokio::runtime::Handle::current();
    let conn = InlineConn::new(server, seat, stream.into(), held, peer, home);
    conn.list();
    (client, conn)
}

impl From<TcpStream> for crate::server::StdConn {
    fn from(stream: TcpStream) -> Self {
        let (socket, unix) = (stream.into(), false);
        crate::server::StdConn { socket, unix }
    }
}

/// What tests read of a connection past its job's looks.
impl InlineConn {
    /// Whether its client's bytes have arrived, for a look that must
    /// find them.
    pub fn readable(&self) -> bool {
        self.io.state.lock().unread()
    }

    /// The size of its write buffer.
    pub fn write_buffered(&self) -> usize {
        self.io.state.lock().out.buf.len()
    }

    /// What its write buffer holds that the socket did not take yet.
    pub fn unsent(&self) -> usize {
        let state = self.io.state.lock();
        state.out.buf.len() - state.out.pos
    }
}

/// inline_loop_server, its spin on, a lone connection's job spinning
/// for up to `budget`, in runs as long.
pub fn spinning_server(workers: usize, budget: Duration) -> Arc<ServerInner> {
    let mut server = inline_loop_server(workers, 8);
    let own = Arc::get_mut(&mut server).and_then(|server| server.inline.as_mut());
    let inline = own.and_then(Arc::get_mut).expect("a server of its own");
    (inline.spin_budget, inline.spin_run) = (budget, budget);
    *inline.spin_on.get_mut() = true;
    server
}

/// `client` sends each write at once, and closes abortively: neither
/// end waits in TIME_WAIT, so the many connections tests open never run
/// the host out of ports. Without the first, a client's second small
/// write waits for its first's ack, which Linux delays.
pub fn abortive(client: &TcpStream) {
    client.set_nodelay(true).expect("nodelay");
    let socket = socket2::SockRef::from(client);
    socket.set_linger(Some(Duration::ZERO)).expect("linger off");
}

/// Everything `client` reads until the server ends the connection.
pub fn read_to_end(client: &TcpStream) -> Vec<u8> {
    let mut read = Vec::new();
    match (&*client).read_to_end(&mut read) {
        Ok(_) => read,
        // Shut down with bytes of its own unread, the server resets.
        Err(e) if e.kind() == ErrorKind::ConnectionReset => read,
        Err(e) => panic!("the end never came: {e}"),
    }
}

/// `client` sends a GET of `path`, asking the connection to end after
/// its answer when `close` is set.
pub fn send_get(client: &TcpStream, path: &str, close: bool) {
    let close = if close { "connection: close\r\n" } else { "" };
    let head = format!("GET {path} HTTP/1.1\r\nhost: x\r\n{close}\r\n");
    (&*client).write_all(head.as_bytes()).unwrap();
}

/// A request for `target` with a body of `length` bytes, the body's
/// first `sent` bytes included.
pub fn with_body(target: &str, length: usize, sent: usize) -> Vec<u8> {
    let head = format!("POST {target} HTTP/1.1\r\nhost: x\r\ncontent-length: {length}\r\n\r\n");
    let mut request = head.into_bytes();
    request.extend((0..sent).map(|i| b'a' + (i % 26) as u8));
    request
}

/// What `conn`'s job sees once the answer it wrote last is out, past
/// the looks that wait for its client to take it.
pub fn once_written(conn: &mut InlineConn) -> Next {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match conn.next(true) {
            Next::Writable => {
                assert!(Instant::now() < deadline, "the answer never went out");
                if let Some(fd) = conn.fd() {
                    super::await_writable(fd, Duration::from_millis(10));
                }
            }
            next => return next,
        }
    }
}

/// What `conn`'s job finds as it looks with what its client sent by now
/// in hand: past a look that waits on a socket that reads ready already,
/// as the first look after a request waits its turn before it reads, and
/// the app's scheduler then wakes the job at once.
pub fn look(conn: &mut InlineConn, room: bool) -> Next {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match conn.next(room) {
            Next::Readable if conn.fd().is_some_and(|fd| reads_within(fd, 0)) => {
                assert!(
                    Instant::now() < deadline,
                    "the job never read its socket's bytes"
                );
            }
            next => return next,
        }
    }
}

/// Whether socket `fd` reads ready (bytes, or its peer's end) within `ms`
/// (0: now).
pub fn reads_within(fd: RawFd, ms: libc::c_int) -> bool {
    super::io::poll_for(fd, libc::POLLIN, ms).is_some()
}

/// `server`'s inline clock, `by` further on: its connections' deadlines
/// come due as if that long had passed. From there its ticker's looks
/// (tick) run on with real time.
pub fn advance(server: &ServerInner, by: Duration) {
    let inline = server.inline.as_ref().expect("inline on");
    let by = u64::try_from(by.as_micros()).expect("a test's span");
    let clock = inline.clock.0.fetch_add(by, Ordering::Relaxed) + by;
    let ahead = clock.saturating_sub(crate::mono::mono_us());
    inline.ahead.store(ahead, Ordering::Relaxed);
}

/// The ticker's look at `server`'s inline connections (inline::tick),
/// once `by` more has passed on its inline clock.
pub fn elapse(server: &ServerInner, by: Duration) {
    advance(server, by);
    super::tick(server);
}

/// Waits until `conn`'s client's bytes have arrived.
pub fn await_bytes(conn: &InlineConn) {
    eventually_blocking("the client's bytes", || conn.readable().then_some(()));
}

/// The body an inline request carries, all of it there before the app
/// runs: its chunks, then the end.
pub fn body_of(ctx: &RequestCtx) -> Vec<u8> {
    let Some(body_rx) = &ctx.body_rx else {
        return Vec::new();
    };
    let mut body = Vec::new();
    loop {
        match body_rx.try_recv() {
            Ok(chunk) => body.extend_from_slice(&chunk),
            Err(flume::TryRecvError::Disconnected) => return body,
            Err(flume::TryRecvError::Empty) => panic!("the app would wait for the body's end"),
        }
    }
}

/// The next request on `conn`, waiting for its bytes.
pub fn next_request(conn: &mut InlineConn) -> BoxedCtx {
    eventually_blocking("the next request", || match conn.next(true) {
        Next::Request(ctx) => Some(ctx),
        Next::Readable => None,
        Next::Writable => panic!("a response is still going out"),
        Next::Closed => panic!("the connection ended before a request"),
    })
}

/// Answers `ctx` inline, as InlineIo::respond writes it: a 200 with `body`.
pub fn answer(ctx: &RequestCtx, body: &[u8]) {
    let io = ctx.inline.as_deref().expect("an inline request");
    let answered = io.respond(
        &ctx.responder,
        http::StatusCode::OK,
        &http::HeaderMap::new(),
        body,
    );
    assert!(answered, "something answered {} first", ctx.uri);
}

/// Hyper's side of a hand-back on a worker_loop server: the requests for
/// `paths` that hyper sent to the workers' lanes, each answered with a
/// 200 once its body is read, its body naming it: "hyper:<path>", then
/// ":<body>" if it had one. What else the lanes hold is taken and
/// dropped.
pub async fn answer_dispatched(server: &Arc<ServerInner>, paths: &[&str]) {
    let slots = server.slots.read().clone();
    let mut left = paths.to_vec();
    while !left.is_empty() {
        let ctx = eventually(format!("hyper sending {left:?} to a lane"), || {
            slots.iter().find_map(|slot| loop {
                match crate::queue::Taker::new(server, slot).ready() {
                    crate::queue::Take::Item(crate::queue::Item::Ctx(ctx)) => return Some(ctx),
                    crate::queue::Take::Item(_) => {}
                    crate::queue::Take::Closed | crate::queue::Take::Nothing => return None,
                }
            })
        });
        let ctx: BoxedCtx = ctx.await;
        let Some(at) = left.iter().position(|path| *path == ctx.uri.path()) else {
            continue;
        };
        left.swap_remove(at);
        let mut body = Vec::new();
        if let Some(body_rx) = &ctx.body_rx {
            while let Ok(chunk) = body_rx.recv_async().await {
                body.extend_from_slice(&chunk);
            }
        }
        let mut text = format!("hyper:{}", ctx.uri.path()).into_bytes();
        if !body.is_empty() {
            text.push(b':');
            text.extend(body);
        }
        let body = crate::response::full_body(Bytes::from(text));
        let response = hyper::Response::builder().body(body).expect("a response");
        assert!(ctx.responder.send_response(response), "answered first");
    }
}

/// How many connections `server` handed back to hyper.
pub fn handbacks(server: &ServerInner) -> u64 {
    server.inline.as_ref().expect("inline on").handbacks()
}

/// `server`'s inline stats now.
pub fn inline_stats(server: &ServerInner) -> crate::stats::InlineSnapshot {
    crate::stats::Snapshot::take(server)
        .inline
        .expect("inline on")
}

/// How many inline connections `server` cut off.
pub fn cut_offs(server: &ServerInner) -> u64 {
    inline_stats(server).closed
}

/// How much of its answers a client read so far (Wire counts it).
#[derive(Clone, Default)]
pub struct Taken(pub Arc<AtomicUsize>);

/// What a client reads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// An answer: its status line, its header lines (lower-case names,
    /// sorted, no date) and its body as framed.
    Answer {
        status: String,
        headers: Vec<String>,
        body: Vec<u8>,
    },
    /// An answer whose body was cut off: its head, and what came of the
    /// body.
    Cut {
        status: String,
        headers: Vec<String>,
        body: Vec<u8>,
    },
    /// Bytes that are no whole HTTP/1 head, up to the end.
    Raw(Vec<u8>),
    /// The follow-up request was answered: the connection stayed open.
    Open,
    /// The server ended the connection.
    Closed,
    /// The server reset the connection (Wire::end; elsewhere a reset is
    /// Closed).
    Reset,
    /// Nothing came for the client's read timeout.
    Hung,
}

/// Why a read stopped: the end, or a pause.
struct Stop;

/// A client's end (TCP or unix), read ahead, its reads counted in
/// `taken`.
pub struct Wire {
    stream: socket2::Socket,
    buf: Vec<u8>,
    pub taken: Taken,
    hung: bool,
    reset: bool,
}

impl Wire {
    pub fn of(client: &impl AsFd) -> Wire {
        let stream = client.as_fd().try_clone_to_owned().expect("a clone");
        Wire {
            stream: stream.into(),
            buf: Vec::new(),
            taken: Taken::default(),
            hung: false,
            reset: false,
        }
    }

    /// The next answer, whole: its status line, header lines and body.
    pub fn response(&mut self) -> (String, Vec<String>, Vec<u8>) {
        match self.answer(false) {
            Event::Answer {
                status,
                headers,
                body,
                ..
            } => (status, headers, body),
            other => panic!("no whole answer: {other:?}"),
        }
    }

    /// More bytes, or the end (a reset too: closed with bytes of the
    /// client's unread), or a pause past the client's read timeout (hung).
    fn more(&mut self) -> Result<(), Stop> {
        let mut chunk = [0u8; 64 * 1024];
        let read = loop {
            match (&self.stream).read(&mut chunk) {
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                read => break read,
            }
        };
        let paused =
            |e: &std::io::Error| matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut);
        self.hung = read.as_ref().is_err_and(paused);
        self.reset = read
            .as_ref()
            .is_err_and(|e| e.kind() == ErrorKind::ConnectionReset);
        let n = read.unwrap_or(0);
        self.buf.extend_from_slice(&chunk[..n]);
        self.taken.0.fetch_add(n, Ordering::Relaxed);
        if n > 0 {
            Ok(())
        } else {
            Err(Stop)
        }
    }

    /// What comes after the answers read, as answer() has it, but a reset
    /// is Reset: Closed is the server's end alone.
    pub fn end(&mut self) -> Event {
        match self.answer(false) {
            Event::Closed if self.reset => Event::Reset,
            event => event,
        }
    }

    /// The offset just past the first `needle` read ahead from `from`.
    fn find(&mut self, from: usize, needle: &[u8]) -> Result<usize, Stop> {
        loop {
            let mut ahead = self.buf[from.min(self.buf.len())..].windows(needle.len());
            if let Some(at) = ahead.position(|window| window == needle) {
                return Ok(from + at + needle.len());
            }
            self.more()?;
        }
    }

    /// At least `n` bytes read ahead, then the first `n` taken.
    fn take(&mut self, n: usize) -> Result<Vec<u8>, Stop> {
        while self.buf.len() < n {
            self.more()?;
        }
        Ok(self.buf.drain(..n).collect())
    }

    /// The next answer, to a HEAD if `head_only`, past any interim ones.
    pub fn answer(&mut self, head_only: bool) -> Event {
        loop {
            if self.buf.is_empty() && self.more().is_err() {
                return if self.hung {
                    Event::Hung
                } else {
                    Event::Closed
                };
            }
            let seen = self.buf.len().min(7);
            let Ok(end) = self.find(0, b"\r\n\r\n") else {
                return Event::Raw(std::mem::take(&mut self.buf));
            };
            if self.buf[..seen] != b"HTTP/1."[..seen] {
                while self.more().is_ok() {}
                return Event::Raw(std::mem::take(&mut self.buf));
            }
            let head = String::from_utf8_lossy(&self.buf[..end - 4]).into_owned();
            self.buf.drain(..end);
            let mut head_lines = head.split("\r\n");
            let status = head_lines.next().unwrap_or_default().to_string();
            let mut headers: Vec<String> = head_lines
                .map(|line| match line.split_once(':') {
                    Some((name, value)) => format!("{}:{value}", name.to_ascii_lowercase()),
                    None => line.to_string(),
                })
                .filter(|line| !line.starts_with("date:"))
                .collect();
            headers.sort();
            let code: u16 = status.get(9..12).and_then(|c| c.parse().ok()).unwrap_or(0);
            if (100..200).contains(&code) && code != 101 {
                continue;
            }
            let header = |name: &str| {
                let value = headers.iter().find_map(|line| line.strip_prefix(name));
                value.map(str::trim)
            };
            let chunked = header("transfer-encoding:").is_some_and(|te| te.ends_with("chunked"));
            let length = header("content-length:").and_then(|cl| cl.parse::<usize>().ok());
            let body = if head_only || [101, 204, 304].contains(&code) {
                Ok(Vec::new())
            } else if chunked {
                self.chunked()
            } else if let Some(length) = length {
                self.take(length)
            } else {
                // Framed by its end.
                while self.more().is_ok() {}
                Ok(std::mem::take(&mut self.buf))
            };
            return match body {
                Ok(body) => Event::Answer {
                    status,
                    headers,
                    body,
                },
                Err(Stop) => Event::Cut {
                    status,
                    headers,
                    body: std::mem::take(&mut self.buf),
                },
            };
        }
    }

    /// A chunked body, as framed.
    fn chunked(&mut self) -> Result<Vec<u8>, Stop> {
        let mut at = 0;
        loop {
            let line_end = self.find(at, b"\r\n")?;
            let line = String::from_utf8_lossy(&self.buf[at..line_end - 2]).into_owned();
            let size = line.split(';').next().unwrap_or("").trim();
            let size = usize::from_str_radix(size, 16).unwrap_or(0);
            if size == 0 {
                let end = self.find(line_end - 2, b"\r\n\r\n")?;
                return self.take(end);
            }
            at = line_end + size + 2;
            while self.buf.len() < at {
                self.more()?;
            }
        }
    }
}
