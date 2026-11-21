//! rack.hijack through a served connection: a test plays the client and
//! the app, which writes its response on its end of the bridge as a Rack
//! app would.

use std::time::Duration;

use http::header::CONNECTION;
use http::HeaderValue;
use http_body_util::{BodyExt, Full};

use super::*;
use crate::registry::{BoxedCtx, ServerInner};
use crate::request::RequestCtx;
use crate::response::{full_body, plain_response};
use crate::server::ServerConfig;
use crate::testing::{
    read_to_close, response_to, send, slot, take_ctx, until, upgrade_request, Pending, Running,
    TestServer, SWITCHED,
};

/// The app's end of a hijacked connection.
type AppEnd = tokio::net::UnixStream;
/// How the app takes the connection over.
type End = fn(&mut RequestCtx) -> AppEnd;

/// A server without lanes (a hijack needs none), running.
async fn hijack_server(set: impl FnOnce(&mut ServerConfig)) -> (Arc<ServerInner>, Running) {
    TestServer::shared_queue().with(set).running().await
}

fn registered(io: UnixStream) -> AppEnd {
    io.set_nonblocking(true).expect("nonblocking app end");
    AppEnd::from_std(io).expect("app end registered")
}

/// Take `ctx`'s connection over, as rack.hijack does.
fn app_end(ctx: &mut RequestCtx) -> AppEnd {
    registered(take(ctx, None).expect("an Upgrade request can be hijacked"))
}

/// The app's 101 to the chat protocol, answered with a callable.
fn chat_switch() -> crate::response::HyperResponse {
    hyper::Response::builder()
        .status(101)
        .header(http::header::UPGRADE, "chat")
        .header(CONNECTION, "Upgrade")
        .body(full_body(Bytes::new()))
        .expect("101 head")
}

/// A client sends `request`, and the app takes its connection over by
/// `end`: the client's pending response, the request as its worker
/// holds it, and the app's end.
async fn hijacked_request(
    server: &Arc<ServerInner>,
    request: hyper::Request<Full<Bytes>>,
    end: End,
) -> (Pending, BoxedCtx, AppEnd) {
    let pending = send(server, request).await;
    let mut ctx = take_ctx(server).await;
    let app = end(&mut ctx);
    (pending, ctx, app)
}

/// hijacked_request, for an Upgrade request taken over as rack.hijack does.
async fn hijacked(server: &Arc<ServerInner>) -> (Pending, BoxedCtx, AppEnd) {
    hijacked_request(server, upgrade_request(), app_end).await
}

/// The client's end, once its Upgrade request got the chat 101.
async fn switched_client(pending: Pending) -> TokioIo<hyper::upgrade::Upgraded> {
    let response = response_to(pending).await;
    assert_eq!(response.status(), 101);
    assert_eq!(
        response.headers()[http::header::UPGRADE],
        "chat",
        "the app's headers reach the client"
    );
    TokioIo::new(hyper::upgrade::on(response).await.expect("client upgrade"))
}

/// Kino answered a hijacked request itself, with `status`, and closed the
/// connection, which is the app's.
async fn kino_answered(pending: Pending, status: u16, what: &str) {
    let response = response_to(pending).await;
    assert_eq!(response.status(), status, "{what}");
    assert_eq!(
        response.headers().get(CONNECTION),
        Some(&HeaderValue::from_static("close")),
        "{what}: a hijacked connection must not serve another request"
    );
}

#[tokio::test]
async fn any_other_answer_with_a_content_length_goes_out_whole_or_not_at_all_then_closes() {
    let (server, _running) = hijack_server(|_| {}).await;
    // The app keeps its end open, or closes it short of the body, which
    // aborts the connection.
    let cases: [(&str, &[u8], bool); 2] = [
        ("content-length", b"content-length: 5\r\n\r\nhello", true),
        ("cut short", b"content-length: 10\r\n\r\nhello", false),
    ];
    for (framing, rest, whole) in cases {
        let (pending, _ctx, mut app) = hijacked(&server).await;
        let head = b"HTTP/1.1 400 Bad Request\r\ncontent-type: text/plain\r\n";
        app.write_all(&[head, rest].concat())
            .await
            .expect("app write");
        if !whole {
            app.shutdown().await.expect("app closes its end");
            // The abort may come before or after the head reaches the client.
            let received = tokio::time::timeout(Duration::from_secs(5), async {
                pending.await.expect("join")?.into_body().collect().await
            });
            let received = received.await.expect("the response ended within 5s");
            assert!(
                received.is_err(),
                "{framing}: must not reach the client as complete"
            );
            continue;
        }

        let response = response_to(pending).await;
        assert_eq!(response.status(), 400, "{framing}");
        assert_eq!(response.headers()[http::header::CONTENT_TYPE], "text/plain");
        assert_eq!(
            response.headers()[CONNECTION],
            "close",
            "{framing}: the connection must close after the answer"
        );
        let body = tokio::time::timeout(Duration::from_secs(5), response.into_body().collect())
            .await
            .unwrap_or_else(|_| panic!("{framing}: the body did not end within 5s"))
            .expect("body");
        assert_eq!(body.to_bytes(), "hello", "{framing}");
        // The bridge lets go of the app's end once the body is through.
        let what = format!("{framing}: the app's end");
        assert_eq!(read_to_close(&mut app, &what).await, b"", "{framing}");
    }
}

/// A hijack of `request` whose bridge waits for the app, the app's end
/// able to take a whole answer at once: a write never yields to the
/// runtime, which would let it see the bytes (a macOS socket holds 8 KiB
/// by default).
async fn waiting_hijack(
    server: &Arc<ServerInner>,
    request: hyper::Request<Full<Bytes>>,
) -> (Pending, BoxedCtx, AppEnd) {
    let hijack = hijacked_request(server, request, app_end).await;
    socket2::SockRef::from(&hijack.2)
        .set_send_buffer_size(1 << 20)
        .expect("app end send buffer");
    until("the bridge to wait for the app", || {
        server.bridges_running.load(Ordering::SeqCst) == 1
    })
    .await;
    hijack
}

/// What the client gets for `request` when its app writes `written`
/// and, if `closes`, closes its end, and the workers then stop before
/// the runtime has seen any of it (the drain wakes the bridge first):
/// the status and body, or what went wrong.
async fn answer_at_drain(
    request: hyper::Request<Full<Bytes>>,
    written: &[u8],
    closes: bool,
) -> Result<(u16, Bytes), String> {
    let (server, _running) = hijack_server(|_| {}).await;
    let (pending, _ctx, mut app) = waiting_hijack(&server, request).await;
    app.write_all(written).await.expect("app write");
    let _open = (!closes).then_some(app);
    close(&server, Closed::All);
    let response = tokio::time::timeout(Duration::from_secs(5), pending)
        .await
        .map_err(|_| "no response within 5s".to_string())?
        .expect("join")
        .map_err(|e| format!("no answer at all ({e})"))?;
    let status = response.status().as_u16();
    let body = response.into_body().collect().await;
    let body = body.map_err(|e| format!("the body was cut ({e})"))?;
    Ok((status, body.to_bytes()))
}

/// An answer the app writes: what it is, its bytes, whether the app's end
/// then closes, and the status and body the client gets.
type Answer<'a> = (&'a str, &'a [u8], bool, u16, &'a [u8]);

#[tokio::test]
async fn an_answer_the_app_wrote_before_the_workers_stopped_goes_out_if_whole_else_the_503() {
    let long = [
        b"HTTP/1.1 200 OK\r\ncontent-length: 65536\r\n\r\n".as_slice(),
        &[b'x'; 65536],
    ]
    .concat();
    let unavailable: &[u8] = b"Service Unavailable\n";
    // One row per case; rustfmt would split each row field by field.
    #[rustfmt::skip]
    let cases: [Answer<'_>; 8] = [
        ("a content-length", b"HTTP/1.1 200 OK\r\ncontent-length: 8\r\n\r\nanswered", false, 200,
            b"answered"),
        ("a content-length of 64 KiB", &long, true, 200, &[b'x'; 65536]),
        ("half a content-length", b"HTTP/1.1 200 OK\r\ncontent-length: 16\r\n\r\nanswered", true,
            503, unavailable),
        ("part of a head", b"HTTP/1.1 2", false, 503, unavailable),
        // No body, whatever the head declares.
        ("a 204 with a content-length", b"HTTP/1.1 204 No Content\r\ncontent-length: 8\r\n\r\n",
            false, 204, b""),
        ("a 304 with a content-length", b"HTTP/1.1 304 Not Modified\r\ncontent-length: 8\r\n\r\n",
            false, 304, b""),
        ("a 204 without a content-length", b"HTTP/1.1 204 No Content\r\n\r\n", false, 204, b""),
        ("the same content-length twice",
            b"HTTP/1.1 200 OK\r\ncontent-length: 8\r\ncontent-length: 8\r\n\r\nanswered", false, 200,
            b"answered"),
    ];
    for (answer, written, closes, status, body) in cases {
        let (got_status, got) = answer_at_drain(upgrade_request(), written, closes)
            .await
            .unwrap_or_else(|e| panic!("{answer}: {e}"));
        assert_eq!(got_status, status, "{answer}");
        assert!(
            got == body,
            "{answer}: a body of {} bytes, not the {} expected",
            got.len(),
            body.len()
        );
    }

    // An answer to a HEAD has no body either, whatever its head declares.
    let mut head = upgrade_request();
    *head.method_mut() = http::Method::HEAD;
    let written = b"HTTP/1.1 200 OK\r\ncontent-length: 8\r\n\r\n";
    let got = answer_at_drain(head, written, false).await;
    let got = got.unwrap_or_else(|e| panic!("a HEAD request: {e}"));
    assert_eq!(got, (200, Bytes::new()), "a HEAD request");
}

#[tokio::test]
async fn a_body_the_app_finished_before_the_workers_stopped_goes_out_and_one_it_did_not_is_cut() {
    // The body, the head that goes out the usual way, and the rest, which
    // the app then writes before the workers stop, unseen by the runtime;
    // whether the app's end closes, and whether the body is whole.
    // One row per case; rustfmt would split each row field by field.
    #[rustfmt::skip]
    let cases: [(&str, &[u8], bool, bool); 2] = [
        ("a content-length", b"HTTP/1.1 200 OK\r\ncontent-length: 8\r\n\r\n", true, true),
        ("half its length", b"HTTP/1.1 200 OK\r\ncontent-length: 16\r\n\r\n", false, false),
    ];
    for (body, head, closes, whole) in cases {
        let (server, _running) = hijack_server(|_| {}).await;
        let (pending, _ctx, mut app) = waiting_hijack(&server, upgrade_request()).await;
        app.write_all(head).await.expect("app head");
        let response = response_to(pending).await;
        assert_eq!(response.status(), 200, "{body}");
        app.write_all(b"answered").await.expect("app rest");
        let _open = (!closes).then_some(app);
        close(&server, Closed::All);

        let got = response.into_body().collect().await;
        if whole {
            let got = got.unwrap_or_else(|e| panic!("{body}: the body was cut ({e})"));
            assert_eq!(got.to_bytes(), "answered", "{body}");
        } else {
            assert!(
                got.is_err(),
                "{body}: a body short of its length must be cut"
            );
        }
    }
}

#[tokio::test]
async fn a_101_the_app_wrote_before_the_workers_stopped_goes_out_and_its_connection_closes() {
    let (server, _running) = hijack_server(|_| {}).await;
    let (pending, _ctx, mut app) = waiting_hijack(&server, upgrade_request()).await;
    app.write_all(&[SWITCHED, b"welcome"].concat())
        .await
        .expect("app write");
    close(&server, Closed::All);

    // Requests drained before the workers stopped, so the switched
    // connection, the app's alone, closes as the 101 goes out.
    let mut client = switched_client(pending).await;
    read_to_close(&mut client, "the client after the 101").await;
}

#[tokio::test]
async fn once_requests_drain_switched_bridges_close_and_unanswered_ones_wait_for_the_workers() {
    let (server, _running) = hijack_server(|_| {}).await;
    let (pending, _ctx, mut written_app) = hijacked(&server).await;
    written_app.write_all(SWITCHED).await.expect("app write");
    let mut written_client = switched_client(pending).await;
    // The app answers with chat_switch, whose callable gets this end.
    let callable_end =
        |ctx: &mut RequestCtx| registered(take(ctx, Some(chat_switch())).expect("switched"));
    let (pending, _ctx, mut callable) =
        hijacked_request(&server, upgrade_request(), callable_end).await;
    let mut callable_client = switched_client(pending).await;
    let (unanswered, _ctx, mut answering) = hijacked(&server).await;
    let (long_poll, _ctx, _polling) = hijacked(&server).await;

    close(&server, Closed::Switched);

    let ends: [(&str, &mut (dyn AsyncRead + Unpin)); 4] = [
        ("the app's end after its own 101", &mut written_app),
        ("the client of the app's own 101", &mut written_client),
        ("a callable's end", &mut callable),
        ("the client of a callable's 101", &mut callable_client),
    ];
    for (what, end) in ends {
        assert_eq!(read_to_close(end, what).await, b"", "{what}");
    }
    let answer = b"HTTP/1.1 200 OK\r\ncontent-length: 8\r\n\r\nanswered";
    answering.write_all(answer).await.expect("app write");
    let response = response_to(unanswered).await;
    assert_eq!(response.status(), 200, "the app's own answer, not a 503");
    let body = response.into_body().collect().await.expect("body");
    assert_eq!(body.to_bytes(), "answered");
    assert!(
        !long_poll.is_finished(),
        "a hijack the app has not answered waits past the switched ones closing"
    );

    close(&server, Closed::All);
    kino_answered(long_poll, 503, "a hijack unanswered once the workers stop").await;
}

#[tokio::test]
async fn a_hijack_counts_against_its_worker_and_server_until_its_bridge_ends() {
    let (server, _running) = hijack_server(|_| {}).await;
    server.register_worker();
    let pending = send(&server, upgrade_request()).await;
    let mut ctx = take_ctx(&server).await;
    ctx.slot = Some(slot(&server, 0));
    let app = app_end(&mut ctx);
    let counted = || {
        let worker = slot(&server, 0).hijacked.load(Ordering::SeqCst);
        let stats = crate::stats::Snapshot::take(&server);
        (worker, stats.hijacked_connections)
    };
    assert_eq!(counted(), (1, 1), "counted from the hijack on");

    drop(app);
    kino_answered(pending, 502, "an app's end closed unanswered").await;
    until("the bridge to stop counting", || counted() == (0, 0)).await;
}

#[tokio::test]
async fn a_head_the_bridge_cannot_read_or_whose_body_has_no_content_length_answers_502() {
    let (server, _running) = hijack_server(|_| {}).await;
    let padding = "x".repeat(16 * 1024);
    let oversized = format!("HTTP/1.1 200 OK\r\nx-padding: {padding}\r\n\r\n");
    let informational = b"HTTP/1.1 103 Early Hints\r\nlink: </style.css>\r\n\r\n";
    // One row per case; rustfmt would split the long rows.
    #[rustfmt::skip]
    let cases: [(&str, &[u8]); 10] = [
        ("a chunked body", b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n"),
        ("a body until close", b"HTTP/1.1 200 OK\r\n\r\nhello"),
        ("a content-length that is no number", b"HTTP/1.1 200 OK\r\ncontent-length: five\r\n\r\nhello"),
        ("a signed content-length", b"HTTP/1.1 200 OK\r\ncontent-length: +5\r\n\r\nhello"),
        ("two content-lengths that differ", b"HTTP/1.1 200 OK\r\ncontent-length: 5\r\ncontent-length: 50\r\n\r\nhello"),
        ("a bodiless answer with a signed content-length", b"HTTP/1.1 204 No Content\r\ncontent-length: +5\r\n\r\n"),
        ("cut short by the app closing", b"HTTP/1.1 101 Swi"),
        ("longer than 16 KiB", oversized.as_bytes()),
        ("not an HTTP response", b"hello\r\n\r\n"),
        ("an informational head", informational),
    ];

    for (head, written) in cases {
        let (pending, _ctx, mut app) = hijacked(&server).await;
        let written = written.to_vec();
        // The bridge may stop reading before all of it is written.
        tokio::spawn(async move {
            let _ = app.write_all(&written).await;
            let _ = app.shutdown().await;
        });
        kino_answered(pending, 502, head).await;
    }
}

#[tokio::test]
async fn once_hijacked_only_the_app_answers() {
    let (server, _running) = hijack_server(|_| {}).await;
    let (pending, mut ctx, mut app) = hijacked(&server).await;

    assert_eq!(
        take(&mut ctx, None).map(drop),
        Err(Refused::AlreadyHijacked),
        "a connection is hijacked once"
    );
    let triple = plain_response(200, "the app's triple\n");
    assert!(
        !ctx.responder.send_response(triple),
        "the request handle can no longer answer"
    );
    // The Drop backstop, as when Ruby collects the request handle before
    // the app has written anything.
    drop(ctx);
    app.write_all(SWITCHED).await.expect("app write");
    assert_eq!(response_to(pending).await.status(), 101);
}

#[tokio::test]
async fn a_bridge_lets_go_once_its_request_is_answered_without_it() {
    let (server, _running) = hijack_server(|config| config.request_timeout_ms = 50).await;
    let (pending, _ctx, mut app) = hijacked(&server).await;

    kino_answered(pending, 504, "a hijacked request past its deadline").await;
    assert_eq!(
        read_to_close(&mut app, "the app's end after a 504").await,
        b""
    );
    let in_flight = server.counters.in_flight.load(Ordering::Relaxed);
    assert_eq!(in_flight, 0, "counted out once, when hijacked");
}
