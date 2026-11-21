//! WebSockets through a served connection: a test plays the client and
//! the workers, whose takes go through queue::Taker::ready (what a worker
//! takes without parking).

use http_body_util::Full;
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};

use super::testing::{client_frame, closes_since, encode, handshake, refusals_since};
use super::*;
use crate::queue::{Item, Take, Taker};
use crate::registry::BoxedCtx;
use crate::request::RequestCtx;
use crate::slot::WS_INBOX;
use crate::testing::{
    eventually, read_head, read_to_close, response_to, send, slot, until, Running, TestServer,
};

/// The frame budget of the tests that fill a socket's queue.
const QUEUE: usize = 4;
const PING: Option<(u8, Vec<u8>)> = Some((0x89, Vec::new()));

/// A lanes server with `workers` workers, its socket budgets as `limits`
/// sets them.
async fn serving(workers: usize, limits: impl FnOnce(&mut Limits)) -> (Arc<ServerInner>, Running) {
    TestServer::lanes()
        .workers(workers)
        .ws(limits)
        .running()
        .await
}

fn upgrade() -> hyper::Request<Full<Bytes>> {
    handshake()
        .body(Full::new(Bytes::new()))
        .expect("handshake")
}

/// A client's accepted socket: its id, and the client's end of the
/// upgraded connection.
struct Socket {
    id: u64,
    io: TokioIo<hyper::upgrade::Upgraded>,
}

impl Socket {
    async fn send(&mut self, wire: &[u8]) {
        self.io.write_all(wire).await.expect("client write");
    }
}

/// A client opens a socket the worker on slot `index` accepts by `accept`.
async fn accept_socket(
    server: &Arc<ServerInner>,
    index: usize,
    accept: impl FnOnce(&mut RequestCtx) -> Accept,
) -> Socket {
    let pending = send(server, upgrade()).await;
    let mut ctx = take_request(server, index).await;
    let Accept::Opened(id) = accept(&mut ctx) else {
        panic!("a handshake on a lanes slot must be accepted");
    };
    assert!(ctx.responder.switch(), "the 101 goes out once on_open ran");
    let response = response_to(pending).await;
    assert_eq!(response.status(), 101, "an accepted handshake answers 101");
    let upgraded = hyper::upgrade::on(response).await.expect("client upgrade");
    Socket {
        id,
        io: TokioIo::new(upgraded),
    }
}

async fn open_socket(server: &Arc<ServerInner>, index: usize) -> Socket {
    accept_socket(server, index, |ctx| accept(ctx, &[], false)).await
}

/// A client that sends a handshake on a raw connection, `then` in the
/// same write: its end of the connection.
async fn raw_handshake(server: &Arc<ServerInner>, then: &[u8]) -> tokio::io::DuplexStream {
    let mut io = crate::server::testing::spawn_conn(server.clone());
    let head = b"GET /ws HTTP/1.1\r\nHost: kino.test\r\nUpgrade: websocket\r\n\
          Connection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
          Sec-WebSocket-Version: 13\r\n\r\n";
    io.write_all(&[&head[..], then].concat())
        .await
        .expect("client write");
    io
}

/// The worker on slot `index` takes its next request (from its lane or
/// stolen) and admits it as its own, as `admit` does.
async fn take_request(server: &Arc<ServerInner>, index: usize) -> BoxedCtx {
    let slot = slot(server, index);
    let mut ctx = eventually("a request", || match Taker::new(server, &slot).ready() {
        Take::Nothing => None,
        taken => Some(crate::queue::testing::expect_request(taken)),
    })
    .await;
    ctx.slot = Some(slot);
    ctx
}

/// The worker on slot `index` takes its next socket event and is still
/// in its callback.
async fn take_unhandled(server: &Arc<ServerInner>, index: usize) -> WsEvent {
    let slot = slot(server, index);
    eventually("a socket event", || {
        match Taker::new(server, &slot).ready() {
            Take::Nothing => None,
            Take::Item(Item::Ws(event)) => Some(event),
            _ => panic!("expected a socket event, took a request, a wake or the lane's end"),
        }
    })
    .await
}

/// The worker on slot `index` takes its next socket event, its callback
/// returning at once.
async fn take_event(server: &Arc<ServerInner>, index: usize) -> WsEvent {
    let event = take_unhandled(server, index).await;
    if let WsEvent::Message { id, .. } = event {
        handled(id);
    }
    event
}

/// Whether the worker on slot `index` has nothing to take.
fn nothing_for(server: &Arc<ServerInner>, index: usize) -> bool {
    matches!(
        Taker::new(server, &slot(server, index)).ready(),
        Take::Nothing
    )
}

fn inbox_of(server: &ServerInner, index: usize) -> flume::Receiver<WsEvent> {
    slot(server, index).lane.ws_rx.clone().expect("inbox")
}

fn message(id: u64, text: &str) -> WsEvent {
    WsEvent::Message {
        id,
        data: Bytes::copy_from_slice(text.as_bytes()),
        text: true,
    }
}

/// The next server frame as (FIN and opcode byte, payload); None once
/// the connection is closed. Server frames are unmasked.
async fn read_frame(io: &mut (impl AsyncRead + Unpin)) -> Option<(u8, Vec<u8>)> {
    let frame = async {
        let mut head = [0u8; 2];
        io.read_exact(&mut head).await.ok()?;
        let len = match head[1] & 0x7F {
            126 => usize::from(io.read_u16().await.ok()?),
            127 => io.read_u64().await.ok()? as usize,
            n => usize::from(n),
        };
        let mut payload = vec![0; len];
        io.read_exact(&mut payload).await.ok()?;
        Some((head[0], payload))
    };
    tokio::time::timeout(Duration::from_secs(5), frame)
        .await
        .expect("no frame from the server within 5s")
}

fn text(text: &str) -> Option<(u8, Vec<u8>)> {
    Some((0x81, text.as_bytes().to_vec()))
}

/// A server close frame carrying `code`, as read_frame returns it.
fn closing(code: u16) -> Option<(u8, Vec<u8>)> {
    Some((0x88, code.to_be_bytes().to_vec()))
}

/// The client of socket `id` has stopped reading: write until its
/// writer is stuck on the full pipe and its queue of QUEUE frames is
/// full behind it, every write going through; how many went.
async fn fill_queue(id: u64) -> usize {
    let mut written = 0;
    while pending(id) <= QUEUE {
        assert!(
            write(id, Opcode::Binary, &[b'x'; 16 * 1024]),
            "write {written} refused"
        );
        written += 1;
        tokio::task::yield_now().await;
    }
    written
}

/// Reads the `written` frames fill_queue queued.
async fn catch_up(io: &mut (impl AsyncRead + Unpin), written: usize) {
    for n in 0..written {
        let frame = read_frame(io).await;
        assert_eq!(frame.map(|(op, _)| op), Some(0x82), "frame {n}");
    }
}

#[tokio::test]
async fn a_frame_sent_with_the_handshake_is_read_once_the_socket_opens() {
    let (server, _running) = serving(1, |_| {}).await;
    // hyper reads the frame with the head and hands it over on the upgrade.
    let mut client = raw_handshake(&server, &client_frame(0x81, b"early")).await;
    let mut ctx = take_request(&server, 0).await;
    let Accept::Opened(id) = accept(&mut ctx, &[], false) else {
        panic!("a handshake on a lanes slot must be accepted");
    };
    assert!(ctx.responder.switch());
    assert!(read_head(&mut client).await.starts_with("HTTP/1.1 101 "));
    assert_eq!(take_event(&server, 0).await, message(id, "early"));
}

#[tokio::test(start_paused = true)]
async fn a_handshake_whose_request_timeout_passes_in_on_open_gets_the_504_and_no_101() {
    let (server, _running) = TestServer::lanes()
        .workers(1)
        .with(|config| config.request_timeout_ms = 50)
        .running()
        .await;
    let before = snapshot();
    let mut client = raw_handshake(&server, b"").await;
    let mut ctx = take_request(&server, 0).await;
    let Accept::Opened(id) = accept(&mut ctx, &[], false) else {
        panic!("a handshake on a lanes slot must be accepted");
    };

    // The worker is still in on_open.
    let closed = WsEvent::Close { id };
    let why = "on_close must not wait for on_open or the client";
    assert_eq!(take_event(&server, 0).await, closed, "{why}");
    assert!(!ctx.responder.switch(), "no 101 may follow the 504");
    assert_eq!(closes_since(&before), [("handshake_failed", 1)]);
    until("the socket to stop counting", || server.open_sockets() == 0).await;
    let head = read_head(&mut client).await;
    let closes = head.starts_with("HTTP/1.1 504 ") && head.contains("connection: close");
    assert!(closes, "{head}");
    read_to_close(&mut client, &head).await;
}

#[tokio::test]
async fn a_socket_whose_queue_is_full_is_dropped_by_an_abort_or_a_close_it_cannot_queue() {
    let (server, _running) = serving(1, |limits| limits.queue = QUEUE).await;
    for how in ["abort", "close"] {
        let mut socket = open_socket(&server, 0).await;
        fill_queue(socket.id).await;

        if how == "abort" {
            assert!(abort(socket.id));
        } else {
            let worker = slot(&server, 0);
            assert_eq!(close_slot(&worker, GOING_AWAY, Cause::GoingAway), 1);
        }
        assert_eq!(
            take_event(&server, 0).await,
            WsEvent::Close { id: socket.id },
            "{how}: on_close follows at once, the client reading nothing"
        );
        assert!(
            !write(socket.id, Opcode::Text, b"late"),
            "{how}: the socket is gone"
        );
        while read_frame(&mut socket.io).await.is_some() {} // closed by then
    }
}

#[tokio::test]
async fn a_socket_is_read_no_further_than_its_inbound_credit_of_unhandled_messages() {
    let (server, _running) = serving(1, |limits| limits.inbound = 3).await;
    let mut flooding = open_socket(&server, 0).await;
    let mut quiet = open_socket(&server, 0).await;
    let inbox = inbox_of(&server, 0);
    let pauses = snapshot().count(WsCount::InboundPauses);
    let sent: Vec<String> = (0..6).map(|i| format!("m{i}")).collect();
    let wire: Vec<u8> = sent
        .iter()
        .flat_map(|m| client_frame(0x81, m.as_bytes()))
        .collect();
    flooding.send(&wire).await;
    until(
        "its reader to pause, once until a message is handled",
        || inbox.len() == 3 && snapshot().count(WsCount::InboundPauses) == pauses + 1,
    )
    .await;

    let quiet_one = client_frame(0x81, b"quiet");
    quiet.send(&quiet_one).await;
    until("the quiet socket's message", || inbox.len() == 4).await;
    assert_eq!(take_unhandled(&server, 0).await, message(flooding.id, "m0"));
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        inbox.len(),
        3,
        "a message whose callback runs still counts against the socket"
    );
    for m in &sent[1..3] {
        assert_eq!(take_event(&server, 0).await, message(flooding.id, m));
    }
    assert_eq!(
        take_event(&server, 0).await,
        message(quiet.id, "quiet"),
        "the quiet socket waits behind the other's inbound credit, not all of its messages"
    );

    handled(flooding.id);
    for m in &sent[3..] {
        assert_eq!(take_event(&server, 0).await, message(flooding.id, m));
    }
}

#[tokio::test]
async fn a_raw_write_goes_out_as_built_and_counts_as_one_message() {
    let (server, _running) = serving(1, |_| {}).await;
    let mut socket = open_socket(&server, 0).await;
    let before = snapshot();
    let raw = [encode(Opcode::Text, b"x"), encode(Opcode::Binary, b"y")].concat();

    // An empty raw write queues nothing, and the socket keeps writing.
    assert!(write_raw(socket.id, b"") && write_raw(socket.id, &raw));
    assert_eq!(read_frame(&mut socket.io).await, text("x"));
    assert_eq!(
        read_frame(&mut socket.io).await,
        Some((0x82, b"y".to_vec()))
    );
    until("the write to count", || {
        let now = snapshot();
        let sent = now.count(WsCount::MessagesSent) - before.count(WsCount::MessagesSent);
        (
            sent,
            now.count(WsCount::SentBytes) - before.count(WsCount::SentBytes),
        ) == (1, raw.len() as u64)
    })
    .await;
}

/// A socket the worker on slot 0 accepts and `end` ends, which must
/// count as opened, then once as closed for `cause`.
async fn ends_as(
    server: &Arc<ServerInner>,
    cause: &str,
    how: &str,
    end: impl AsyncFnOnce(&mut Socket),
) {
    let before = snapshot();
    let mut socket = open_socket(server, 0).await;
    assert_eq!(
        snapshot().count(WsCount::Opened) - before.count(WsCount::Opened),
        1,
        "{how}: opened"
    );
    end(&mut socket).await;
    let closed = WsEvent::Close { id: socket.id };
    assert_eq!(
        take_event(server, 0).await,
        closed,
        "{how}: once, nothing before"
    );
    assert_eq!(closes_since(&before), [(cause, 1)], "{how}");
    assert_eq!(snapshot().open, before.open, "{how}: still open");
}

#[tokio::test]
async fn each_socket_closes_once_under_the_cause_that_ended_it() {
    let (server, _running) = serving(1, |_| {}).await;
    let worker = slot(&server, 0);
    let close = |code: u16| client_frame(0x88, &code.to_be_bytes());

    ends_as(&server, "normal", "the client's close", async |s| {
        s.send(&close(1001)).await
    })
    .await;
    ends_as(&server, "normal", "the app's close", async |s| {
        assert!(super::close(s.id, 4000));
        assert_eq!(read_frame(&mut s.io).await, closing(4000));
        s.send(&close(4000)).await;
    })
    .await;
    ends_as(&server, "normal", "the app's abort", async |s| {
        assert!(abort(s.id));
    })
    .await;
    ends_as(&server, "client_gone", "a hang-up mid-frame", async |s| {
        s.send(&client_frame(0x81, b"hello")[..4]).await;
        s.io.shutdown().await.expect("client shutdown");
    })
    .await;
    ends_as(&server, "crash", "a callback that raised", async |s| {
        assert!(fail(s.id));
    })
    .await;
    // Kino's own closes stand, however the client answers them.
    for (code, cause, name) in [
        (GOING_AWAY, Cause::GoingAway, "going_away"),
        (SERVER_ERROR, Cause::Crash, "crash"),
    ] {
        ends_as(&server, name, &format!("Kino's {code}"), async |s| {
            assert_eq!(close_slot(&worker, code, cause), 1);
            assert_eq!(read_frame(&mut s.io).await, closing(code));
            s.send(&close(1000)).await;
        })
        .await;
        worker.sockets.reset();
    }
}

#[test]
fn a_socket_cut_by_runtime_teardown_is_forgotten() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let (_running, before, socket) = runtime.block_on(async {
        let (server, running) = serving(1, |_| {}).await;
        let before = snapshot();
        (running, before, open_socket(&server, 0).await)
    });

    // The client is still connected: only the teardown ends the socket.
    drop(runtime);

    assert!(
        !registered(socket.id),
        "a socket whose task was dropped must leave the socket table"
    );
    assert_eq!(snapshot().open, 0, "nor count as open");
    assert_eq!(
        closes_since(&before),
        [("going_away", 1)],
        "an open socket went away with the server"
    );
}

#[tokio::test]
async fn an_abandoned_worker_does_not_hold_the_drains_1001() {
    let (server, _running) = serving(1, |_| {}).await;
    let mut socket = accept_socket(&server, 0, |ctx| accept(ctx, &[], true)).await;

    leave_all(&server, Duration::ZERO);
    // The worker takes the event and never comes back from on_shutdown.
    let shutdown = WsEvent::Shutdown { id: socket.id };
    assert_eq!(take_event(&server, 0).await, shutdown);
    abandon(&slot(&server, 0));
    assert_eq!(read_frame(&mut socket.io).await, closing(GOING_AWAY));
}

/// A handshake the worker on slot `index` takes, once `meanwhile` ran,
/// and accepts, which Kino refuses with a 503 that closes the connection.
async fn refused(server: &Arc<ServerInner>, index: usize, meanwhile: impl FnOnce()) {
    let pending = send(server, upgrade()).await;
    until("the handshake in a lane", || server.queued() == 1).await;
    meanwhile();
    let mut ctx = take_request(server, index).await;
    assert_eq!(accept(&mut ctx, &[], false), Accept::Refused);
    let response = response_to(pending).await;
    assert_eq!(response.status(), 503, "a refused handshake gets a 503");
    assert_eq!(response.headers()[http::header::CONNECTION], "close");
}

#[tokio::test]
async fn a_handshake_is_refused_while_its_worker_closes_its_sockets_or_the_server_drains() {
    {
        let (server, _running) = serving(1, |_| {}).await;
        let mut leaving = open_socket(&server, 0).await;
        assert_eq!(
            close_slot(&slot(&server, 0), GOING_AWAY, Cause::GoingAway),
            1
        );
        assert_eq!(read_frame(&mut leaving.io).await, closing(GOING_AWAY));

        let before = snapshot();
        refused(&server, 0, || {}).await;
        assert_eq!(refusals_since(&before), [("worker_closing", 1)]);
        let opened = snapshot().count(WsCount::Opened) - before.count(WsCount::Opened);
        assert_eq!(opened, 0, "the refused handshake opened no socket");
    }
    // The drain begins (stop_accepting, then leave_all), and then a worker
    // joins, as a crashed one's replacement would.
    let (server, _running) = serving(1, |_| {}).await;
    refused(&server, 1, || {
        let _ = server.shutdown_tx.send(true);
        leave_all(&server, Duration::ZERO);
        server.register_worker();
    })
    .await;
    assert_eq!(server.open_sockets(), 0);
}

#[tokio::test(start_paused = true)]
async fn a_close_its_client_never_completes_lets_go_of_the_socket_after_the_close_timeout() {
    let (server, _running) = serving(1, |limits| limits.queue = QUEUE).await;
    // One client says goodbye but never reads again: the socket ends, its
    // writer stuck behind what the client will not take. The other never
    // answers the app's close.
    let mut stuck = open_socket(&server, 0).await;
    let mut unanswered = open_socket(&server, 0).await;
    fill_queue(stuck.id).await;
    stuck
        .send(&client_frame(0x88, &1000u16.to_be_bytes()))
        .await;
    let ended = WsEvent::Close { id: stuck.id };
    let why = "on_close does not wait for the writer";
    assert_eq!(take_event(&server, 0).await, ended, "{why}");
    // Whether the server let go of the connection: a write then fails.
    let let_go = async |io: &mut TokioIo<hyper::upgrade::Upgraded>| {
        let write = tokio::time::timeout(Duration::from_millis(1), io.write_all(b"?"));
        matches!(write.await, Ok(Err(_)))
    };
    let why = "the stuck writer holds the connection until the close timeout";
    assert!(!let_go(&mut stuck.io).await, "{why}");

    assert!(close(unanswered.id, 1000));
    assert_eq!(read_frame(&mut unanswered.io).await, closing(1000));
    tokio::time::sleep(CLOSE_TIMEOUT - Duration::from_millis(1)).await;
    let why = "the client still has time to answer";
    assert!(nothing_for(&server, 0), "{why}");
    let dropped = WsEvent::Close { id: unanswered.id };
    assert_eq!(take_event(&server, 0).await, dropped);
    tokio::time::timeout(Duration::from_secs(5), async {
        while !let_go(&mut stuck.io).await {}
    })
    .await
    .expect("the stuck writer kept the connection past the close timeout");
}

#[tokio::test(start_paused = true)]
async fn a_silent_client_is_pinged_after_the_interval_and_dropped_after_two_and_one_that_answers_stays(
) {
    let interval = Duration::from_secs(2);
    let (server, _running) = serving(1, |limits| limits.ping_interval = interval).await;
    let before = snapshot();
    let mut silent = open_socket(&server, 0).await;
    let opened = tokio::time::Instant::now();
    let mut answering = open_socket(&server, 0).await;

    assert_eq!(read_frame(&mut silent.io).await, PING);
    assert!(opened.elapsed() >= interval, "pinged before its interval");
    tokio::time::sleep_until(opened + interval * 2 - Duration::from_millis(5)).await;
    let why = "a client has one more interval to answer the ping";
    assert!(nothing_for(&server, 0), "{why}");
    for _ in 0..5 {
        assert_eq!(read_frame(&mut answering.io).await, PING);
        answering.send(&client_frame(0x8A, b"")).await;
    }
    let dropped = WsEvent::Close { id: silent.id };
    let why = "a client silent for two intervals is a dead peer";
    assert_eq!(take_event(&server, 0).await, dropped, "{why}");
    assert!(nothing_for(&server, 0), "a client that answers is alive");
    assert_eq!(closes_since(&before), [("dead_peer", 1)]);
}

#[tokio::test(start_paused = true)]
async fn a_socket_whose_reader_waits_on_its_worker_is_not_a_dead_peer() {
    let interval = Duration::from_secs(2);
    // The worker sits in m0's callback, the socket's one message of
    // credit, or is away while m0 fills its inbox of one; for five
    // intervals the reader waits and reads nothing.
    for in_callback in [true, false] {
        let (server, _running) = TestServer::lanes()
            .workers(1)
            .inbox(if in_callback { WS_INBOX } else { 1 })
            .ws(|limits| {
                limits.ping_interval = interval;
                limits.inbound = if in_callback { 1 } else { INBOUND_CREDIT };
            })
            .running()
            .await;
        let mut socket = open_socket(&server, 0).await;
        let sent = ["m0", "m1", "m2"];
        let wire = sent.map(|m| client_frame(0x81, m.as_bytes())).concat();
        socket.send(&wire).await;
        if in_callback {
            assert_eq!(take_unhandled(&server, 0).await, message(socket.id, "m0"));
        } else {
            let inbox = inbox_of(&server, 0);
            until("a full inbox", || inbox.is_full()).await;
        }

        tokio::time::sleep(interval * 5).await;
        if in_callback {
            assert!(
                nothing_for(&server, 0),
                "a socket whose worker is slow stays"
            );
            handled(socket.id);
        }
        for m in &sent[usize::from(in_callback)..] {
            let what = format!("in callback {in_callback}");
            assert_eq!(
                take_event(&server, 0).await,
                message(socket.id, m),
                "{what}"
            );
        }
    }
}

#[tokio::test(start_paused = true)]
async fn under_a_full_queue_a_client_reading_slowly_stays_and_one_that_stopped_reading_is_dropped()
{
    let interval = Duration::from_secs(2);
    let (server, _running) = serving(1, |limits| {
        limits.queue = QUEUE;
        limits.ping_interval = interval;
    })
    .await;
    let mut reading = open_socket(&server, 0).await;
    let stopped = open_socket(&server, 0).await;
    fill_queue(reading.id).await;
    fill_queue(stopped.id).await;

    // One client only reads, a frame every three quarters of an interval,
    // and the app keeps its queue full: six reads span more than two
    // intervals. A writer that cannot get through proves nothing.
    let (mut reads, filled) = (0, tokio::time::Instant::now());
    while reads < 6 {
        tokio::time::sleep(interval * 3 / 4).await;
        let early = filled.elapsed() < interval * 2;
        assert!(!early || inbox_of(&server, 0).is_empty(), "dropped early");
        match read_frame(&mut reading.io).await {
            Some((0x82, _)) => reads += 1,
            Some((0x89, _)) => {} // a ping, left unanswered
            other => panic!("the slow client was dropped after {reads} reads: {other:?}"),
        }
        write(reading.id, Opcode::Binary, &[b'x'; 16 * 1024]);
    }

    let dropped = WsEvent::Close { id: stopped.id };
    assert_eq!(take_event(&server, 0).await, dropped);
    assert!(nothing_for(&server, 0), "a client that reads is alive");
}

#[tokio::test]
async fn once_its_full_queue_drained_the_worker_hears_of_the_write_close_or_publish_it_refused() {
    let (server, _running) = serving(1, |limits| limits.queue = QUEUE).await;
    for refused in ["write", "close", "publish"] {
        let mut socket = open_socket(&server, 0).await;
        assert_eq!(pending(socket.id), 0, "{refused}");
        let written = fill_queue(socket.id).await;
        let before = snapshot();
        match refused {
            "write" => assert!(!write(socket.id, Opcode::Text, b"refused")),
            "close" => assert!(!close(socket.id, 1000)),
            _ => {
                let topic = b"once_its_full_queue_drained";
                let when_full = topics::WhenFull::Skip;
                assert!(testing::subscribe(socket.id, topic, b"", b"", when_full));
                assert!(topics::publish(topic, b"skipped"));
                until("the publish to find the queue full", || {
                    snapshot().count(WsCount::MessagesSkipped)
                        > before.count(WsCount::MessagesSkipped)
                })
                .await;
            }
        }
        let now = snapshot();
        let counted = (
            now.count(WsCount::WritesRefused) - before.count(WsCount::WritesRefused),
            now.count(WsCount::MessagesSkipped) - before.count(WsCount::MessagesSkipped),
        );
        let skipped = refused == "publish";
        assert_eq!(
            counted,
            (u64::from(!skipped), u64::from(skipped)),
            "{refused}"
        );
        assert!(nothing_for(&server, 0), "{refused}: nothing drained yet");

        catch_up(&mut socket.io, written).await;
        let drained = WsEvent::Drained { id: socket.id };
        assert_eq!(take_event(&server, 0).await, drained, "{refused}");
        assert!(
            write(socket.id, Opcode::Text, b"after"),
            "{refused}: room again"
        );
        assert_eq!(read_frame(&mut socket.io).await, text("after"));
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            nothing_for(&server, 0),
            "{refused}: a write that went through does not drain again"
        );
        assert!(close(socket.id, 1000), "{refused}: and a close goes out");
        assert_eq!(read_frame(&mut socket.io).await, closing(1000));
        drop(socket.io);
        let closed = WsEvent::Close { id: socket.id };
        assert_eq!(take_event(&server, 0).await, closed, "{refused}");
    }
}

#[tokio::test]
async fn the_writer_never_waits_for_room_to_say_a_queue_drained() {
    let (server, _running) = TestServer::lanes()
        .workers(1)
        .inbox(1)
        .ws(|limits| limits.queue = QUEUE)
        .running()
        .await;
    let mut socket = open_socket(&server, 0).await;
    // The worker is busy: the client's message fills its inbox.
    let busy = client_frame(0x81, b"busy");
    socket.send(&busy).await;
    let inbox = inbox_of(&server, 0);
    until("a full inbox", || inbox.is_full()).await;

    let written = fill_queue(socket.id).await;
    assert!(!write(socket.id, Opcode::Text, b"refused"));
    catch_up(&mut socket.io, written).await;
    assert!(write(socket.id, Opcode::Text, b"after"));
    assert_eq!(
        read_frame(&mut socket.io).await,
        text("after"),
        "the writer must keep writing while the worker is busy"
    );

    assert_eq!(take_event(&server, 0).await, message(socket.id, "busy"));
    let drained = WsEvent::Drained { id: socket.id };
    assert_eq!(take_event(&server, 0).await, drained);
}

#[tokio::test]
async fn a_crashed_workers_sockets_close_with_1011_whatever_their_readers_waited_on() {
    // An inbox nobody takes from any more, or the credit of a message
    // whose callback died with the worker.
    for in_callback in [false, true] {
        let (server, _running) = TestServer::lanes()
            .workers(1)
            .inbox(if in_callback { WS_INBOX } else { 1 })
            .ws(|limits| limits.inbound = if in_callback { 1 } else { INBOUND_CREDIT })
            .running()
            .await;
        let mut socket = open_socket(&server, 0).await;
        let wire = [client_frame(0x81, b"one"), client_frame(0x81, b"two")].concat();
        socket.send(&wire).await;
        if in_callback {
            assert_eq!(take_unhandled(&server, 0).await, message(socket.id, "one"));
        } else {
            let inbox = inbox_of(&server, 0);
            until("a full inbox", || inbox.is_full()).await;
        }

        assert_eq!(
            slot(&server, 0).crash(),
            1,
            "the crashed worker had one socket"
        );
        assert_eq!(read_frame(&mut socket.io).await, closing(SERVER_ERROR));
        let answered = std::time::Instant::now();
        let answer = client_frame(0x88, &SERVER_ERROR.to_be_bytes());
        socket.send(&answer).await;
        until("the socket to end", || server.open_sockets() == 0).await;
        assert!(
            answered.elapsed() < CLOSE_TIMEOUT / 2,
            "in callback {in_callback}: the client's answer must be read, not waited out"
        );
        assert!(
            inbox_of(&server, 0).is_empty(),
            "the dead worker's events must not wait for it"
        );
    }
}
