//! Inline serving against hyper. Each request of a corpus goes to a
//! connection hyper serves and to an inline one, both answered by one
//! app: what their clients read must agree, and the inline connection
//! goes back to hyper for the reason its row names, or serves it all
//! itself where the row names none. Then the subset parser is fuzzed: it
//! never panics, and what it accepts httparse, the http crate and hyper
//! read the same way.

use std::io::Write;
use std::net::TcpStream;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use http::StatusCode;

use super::testing::{abortive, accepted, body_of, loopback, server, Event, Taken, Wire};
use super::{InlineConn, Next, Wrote};
use crate::registry::{BoxedCtx, ServerInner};
use crate::request::RequestCtx;
use crate::response::{BodyFrame, Cut};

/// How the app answers each request of a row.
#[derive(Clone, Copy)]
struct Reply {
    status: u16,
    headers: &'static [(&'static str, &'static str)],
    body: Body,
}

#[derive(Clone, Copy)]
enum Body {
    Full(&'static [u8]),
    /// A stream: its head, these writes, then its end.
    Stream(&'static [&'static [u8]]),
    /// A stream the app raises out of after these writes.
    Raise(&'static [&'static [u8]]),
    /// A stream that ends after these writes, short of the app's length.
    Stops(&'static [&'static [u8]]),
}

const TEXT: &[(&str, &str)] = &[("content-type", "text/plain")];
const CL: (&str, &str) = ("content-length", "5");
const TE: (&str, &str) = ("transfer-encoding", "chunked");
const LENGTH_FIVE: &[(&str, &str)] = &[CL];
const HELLO: Reply = full(200, TEXT, b"hello");

const fn full(
    status: u16,
    headers: &'static [(&'static str, &'static str)],
    body: &'static [u8],
) -> Reply {
    Reply {
        status,
        headers,
        body: Body::Full(body),
    }
}

const fn streamed(
    status: u16,
    headers: &'static [(&'static str, &'static str)],
    writes: &'static [&'static [u8]],
) -> Reply {
    Reply {
        status,
        headers,
        body: Body::Stream(writes),
    }
}

/// How long an app waits on its client's reads.
const PATIENCE: Duration = Duration::from_secs(5);

/// Once the client `taken` counts for read past `seen` (or a few seconds
/// on): how much. hyper drops what it holds of a stream that ends early
/// at once, and writes it out when the stream's next write is not there
/// yet: an app whose stream ends so goes on only once its client read
/// what it sent.
fn past(taken: &Taken, seen: usize) -> usize {
    let deadline = Instant::now() + PATIENCE;
    while taken.0.load(Ordering::Relaxed) <= seen && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
    taken.0.load(Ordering::Relaxed)
}

async fn past_async(taken: &Taken, seen: usize) -> usize {
    let deadline = Instant::now() + PATIENCE;
    while taken.0.load(Ordering::Relaxed) <= seen && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    taken.0.load(Ordering::Relaxed)
}

/// The app's answer to `ctx` (a follow-up's is HELLO), its headers with
/// what the app saw of the request (x-saw): the request line as parsed,
/// its headers in order, and its body.
fn answer_for(ctx: &RequestCtx, body: &[u8], reply: Reply) -> (Reply, http::HeaderMap) {
    let reply = if ctx.uri.path() == "/follow-up" {
        HELLO
    } else {
        reply
    };
    let mut headers = http::HeaderMap::new();
    for (name, value) in reply.headers {
        let name = http::HeaderName::from_bytes(name.as_bytes()).expect("a name");
        headers.append(
            name,
            http::HeaderValue::from_bytes(value.as_bytes()).expect("a value"),
        );
    }
    let mut saw = format!("{} {} {:?}", ctx.method, ctx.uri, ctx.version);
    for (name, value) in &ctx.headers {
        saw.push_str(&format!(" {name}={}", value.as_bytes().escape_ascii()));
    }
    let shown = &body[..body.len().min(32)];
    saw.push_str(&format!(" body={}:{}", body.len(), shown.escape_ascii()));
    headers.append(
        "x-saw",
        http::HeaderValue::from_str(&saw).expect("an escaped value"),
    );
    (reply, headers)
}

/// The app answers an inline request, as request.rs writes it. A stream
/// that ends early goes on only once its client read what it sent, as
/// hyper drops what it holds of such a stream at once.
fn answer_inline(ctx: &RequestCtx, reply: Reply, taken: &Taken) {
    let io = ctx.inline.as_deref().expect("an inline request");
    let (reply, headers) = answer_for(ctx, &body_of(ctx), reply);
    let status = StatusCode::from_u16(reply.status).expect("a status");
    let (writes, waits) = match reply.body {
        Body::Full(body) => {
            io.respond(&ctx.responder, status, &headers, body);
            return;
        }
        Body::Stream(writes) => (writes, false),
        Body::Raise(writes) | Body::Stops(writes) => (writes, true),
    };
    let mut seen = taken.0.load(Ordering::Relaxed);
    if !io.start_stream(&ctx.responder, status, &headers) {
        return;
    }
    if waits {
        seen = past(taken, seen);
    }
    'writes: for data in writes {
        loop {
            match io.write_chunk(&ctx.responder, data) {
                Wrote::Done => break,
                Wrote::Wait(fd) => {
                    super::await_writable(fd, Duration::from_millis(10));
                    io.waited();
                }
                Wrote::Gone | Wrote::Closed => break 'writes,
            }
        }
        if waits {
            seen = past(taken, seen);
        }
    }
    match reply.body {
        Body::Raise(_) => ctx.responder.give_up(Cut::Crash),
        _ => io.finish_stream(&ctx.responder),
    }
}

/// The app answers a request hyper read, as request.rs sends it.
async fn answer_hyper(ctx: BoxedCtx, reply: Reply, taken: &Taken) {
    let mut body = Vec::new();
    if let Some(body_rx) = &ctx.body_rx {
        while let Ok(chunk) = body_rx.recv_async().await {
            body.extend_from_slice(&chunk);
        }
    }
    let (reply, headers) = answer_for(&ctx, &body, reply);
    let mut builder = hyper::Response::builder().status(reply.status);
    for (name, value) in &headers {
        builder = builder.header(name, value);
    }
    let (writes, waits) = match reply.body {
        Body::Full(body) => {
            let sent = crate::request::frame_by_length(
                &mut builder,
                reply.status,
                &ctx.method,
                body.len(),
            );
            let body = if sent {
                Bytes::from_static(body)
            } else {
                Bytes::new()
            };
            let response = builder
                .body(crate::response::full_body(body))
                .expect("a response");
            ctx.responder.send_response(response);
            return;
        }
        Body::Stream(writes) => (writes, false),
        Body::Raise(writes) | Body::Stops(writes) => (writes, true),
    };
    let mut seen = taken.0.load(Ordering::Relaxed);
    let head = ctx.method == http::Method::HEAD;
    let sent = match crate::framing::Bodiless::of(reply.status, head) {
        Some(bodiless) => ctx.responder.send_bodiless_head(builder, bodiless),
        None => ctx.responder.send_stream_head(builder),
    };
    if !matches!(sent, Ok(true)) {
        return;
    }
    if waits {
        seen = past_async(taken, seen).await;
    }
    if let Some(frames) = ctx.responder.body_sender() {
        for data in writes {
            if frames
                .send_async(Ok(BodyFrame::data(Bytes::from_static(data))))
                .await
                .is_err()
            {
                break;
            }
            if waits {
                seen = past_async(taken, seen).await;
            }
        }
    }
    match reply.body {
        Body::Raise(_) => ctx.responder.give_up(Cut::Crash),
        _ => ctx.responder.finish_stream(),
    }
}

/// `server`'s app for what hyper reads: each request its queue gets,
/// answered with `reply`, until aborted.
fn app(server: &Arc<ServerInner>, reply: Reply, taken: Taken) -> tokio::task::JoinHandle<()> {
    let server = server.clone();
    tokio::spawn(async move {
        while let Ok(ctx) = server.req_rx.recv_async().await {
            answer_hyper(ctx, reply, &taken).await;
        }
    })
}

/// An inline connection's job: each request it reads answered with
/// `reply`, until it closes or goes back to hyper, or ten seconds pass
/// with nothing to run. The requests it ran, as "METHOD target".
fn job(
    mut conn: Box<InlineConn>,
    reply: Reply,
    taken: Taken,
) -> std::thread::JoinHandle<Vec<String>> {
    std::thread::spawn(move || {
        let mut ran = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match conn.next(true) {
                Next::Request(ctx) => {
                    ran.push(format!("{} {}", ctx.method, ctx.uri));
                    answer_inline(&ctx, reply, &taken);
                }
                Next::Readable | Next::Writable if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(1));
                }
                Next::Readable | Next::Writable => {
                    ran.push("(the job gave up waiting)".to_string());
                    return ran;
                }
                Next::Closed => return ran,
            }
        }
    })
}

/// A raw exchange: what the client sends, in pieces `pause` apart, and
/// the answers it reads, true for each to a HEAD.
#[derive(Clone)]
struct Row {
    name: &'static str,
    sent: Vec<Vec<u8>>,
    pause: Duration,
    reads: Vec<bool>,
    reply: Reply,
    /// A recorded deviation, named: what the inline client reads instead
    /// (lines, x-saw aside).
    deviation: Option<(&'static str, Vec<&'static str>)>,
    /// Why an inline connection hands it back to hyper, if it does (the
    /// server counts a total: the reason is the reader's).
    handed_back: Option<&'static str>,
}

fn row(name: &'static str, sent: impl Into<Vec<u8>>) -> Row {
    Row {
        name,
        sent: vec![sent.into()],
        pause: Duration::ZERO,
        reads: vec![false],
        reply: HELLO,
        deviation: None,
        handed_back: None,
    }
}

impl Row {
    fn reads(mut self, reads: &[bool]) -> Self {
        self.reads = reads.to_vec();
        self
    }

    fn reply(mut self, reply: Reply) -> Self {
        self.reply = reply;
        self
    }

    /// What it sends goes in pieces of `size` bytes, `pause` apart.
    fn pieces(mut self, size: usize, pause: Duration) -> Self {
        let sent = self.sent.concat();
        self.sent = sent.chunks(size).map(<[u8]>::to_vec).collect();
        self.pause = pause;
        self
    }

    fn handed_back(mut self, why: &'static str) -> Self {
        self.handed_back = Some(why);
        self
    }

    fn deviation(mut self, why: &'static str, inline: &[&'static str]) -> Self {
        self.deviation = Some((why, inline.to_vec()));
        self
    }
}

/// What `client` reads of `row`, its reads counted in `taken`: it sends
/// the row, reads its answers, then, unless the connection ended, a
/// follow-up's (Event::Open if answered).
fn view(client: TcpStream, row: &Row, taken: &Taken) -> Vec<Event> {
    let mut wire = Wire::of(&client);
    wire.taken = taken.clone();
    for (i, piece) in row.sent.iter().enumerate() {
        if i > 0 {
            std::thread::sleep(row.pause);
        }
        if (&client).write_all(piece).is_err() {
            break;
        }
    }
    let mut events = Vec::new();
    for &head_only in &row.reads {
        let event = wire.answer(head_only);
        let answered = matches!(event, Event::Answer { .. });
        let cut = matches!(event, Event::Raw(_) | Event::Cut { .. });
        events.push(event);
        if !answered {
            if cut {
                events.push(wire.answer(false));
            }
            return events;
        }
    }
    if (&client)
        .write_all(b"GET /follow-up HTTP/1.1\r\nhost: x\r\n\r\n")
        .is_err()
    {
        events.push(Event::Closed);
        return events;
    }
    events.push(match wire.answer(false) {
        Event::Answer { headers, .. }
            if headers
                .iter()
                .any(|line| line.starts_with("x-saw: GET /follow-up")) =>
        {
            Event::Open
        }
        other => other,
    });
    events
}

/// What a client of a connection hyper serves reads of `row`.
async fn hyper_view(row: &Row) -> Vec<Event> {
    let test = crate::testing::TestServer::shared_queue().workers(1);
    let server = test.with(|c| (c.queue_depth, c.http2) = (8, true)).build();
    let taken = Taken::default();
    let app = app(&server, row.reply, taken.clone());
    let (client, stream, held, peer) = accepted(1).await.pop().expect("one");
    abortive(&client);
    let accepted = crate::server::Accepted {
        conn: tokio_util::either::Either::Left(stream),
        peer,
        held,
    };
    tokio::spawn(crate::server::serve_conn(accepted, None, server.clone()));
    let events = tokio::task::block_in_place(|| view(client, row, &taken));
    app.abort();
    events
}

/// `events` as lines: each answer's status, headers (x-saw only if
/// `saw`) and body, and how the connection ended.
fn lines(events: &[Event], saw: bool) -> Vec<String> {
    let shown = |bytes: &[u8]| match bytes.len() {
        0..=96 => bytes.escape_ascii().to_string(),
        n => format!("{}... ({n} bytes)", bytes[..96].escape_ascii()),
    };
    let mut lines = Vec::new();
    for event in events {
        let (cut, status, headers, body) = match event {
            Event::Answer {
                status,
                headers,
                body,
            } => ("", status, headers, body),
            Event::Cut {
                status,
                headers,
                body,
            } => ("cut short: ", status, headers, body),
            other => {
                lines.push(format!("{other:?}").to_lowercase());
                continue;
            }
        };
        lines.push(format!("{cut}{status}"));
        let kept = headers
            .iter()
            .filter(|line| saw || !line.starts_with("x-saw:"));
        lines.extend(kept.cloned());
        lines.push(format!("body: {}", shown(body)));
    }
    lines
}

/// What a client of an inline connection reads of `row`, the requests
/// its job ran, and whether it went back to hyper.
async fn inline_view(row: &Row) -> (Vec<Event>, Vec<String>, bool) {
    let server = server();
    let taken = Taken::default();
    let app = app(&server, row.reply, taken.clone());
    let (client, conn) = loopback(&server);
    abortive(&client);
    let job = job(conn, row.reply, taken.clone());
    let (events, ran) = tokio::task::block_in_place(|| {
        let events = view(client, row, &taken);
        (events, job.join().expect("the job"))
    });
    app.abort();
    (events, ran, super::testing::handbacks(&server) > 0)
}

/// A request for `target` with `headers` (each line's end included).
fn get(target: &str, headers: &str) -> Vec<u8> {
    format!("GET {target} HTTP/1.1\r\nhost: x\r\n{headers}\r\n").into_bytes()
}

/// A POST of `body`, framed by its length.
fn post(body: &[u8]) -> Vec<u8> {
    let head = format!(
        "POST /up HTTP/1.1\r\nhost: x\r\ncontent-length: {}\r\n\r\n",
        body.len()
    );
    [head.as_bytes(), body].concat()
}

/// The corpus: raw requests, and how the app answers them. A row marks a
/// boundary where inline serving and hyper take different code: framing,
/// why a connection goes back to hyper, whether it stays open, a stream
/// cut short, and the recorded differences (doc/architecture.md).
#[rustfmt::skip] // One row per line: a table of exchanges.
fn corpus() -> Vec<Row> {
    let many_headers = |n: usize| (0..n).map(|i| format!("x-h{i}: v\r\n")).collect::<String>();
    // Heads of 16 KiB, the inline cap, and a byte past it (within hyper's).
    let head_of = |len: usize| {
        let filler = len - get("/", "x-fill: \r\n").len();
        get("/", &format!("x-fill: {}\r\n", "f".repeat(filler)))
    };
    let (at_cap, past_cap) = (head_of(16 * 1024), head_of(16 * 1024 + 1));
    let head = "HEAD / HTTP/1.1\r\nhost: x\r\n\r\n";
    let chunked_put = "PUT /2 HTTP/1.1\r\nhost: x\r\ntransfer-encoding: chunked\r\n\r\n2\r\nhi\r\n0\r\n\r\n";

    vec![
        // Requests served inline, and whether the connection stays open.
        row("a GET that closes", get("/", "connection: close\r\n")),
        row("a POST", post(b"hello")),
        row("100 headers", get("/", &many_headers(99))),
        row("a head of 16 KiB", at_cap),
        row("three at once, sent a byte at a time", [get("/1", ""), post(b"2"), get("/3", "")].concat()).reads(&[false; 3]).pieces(1, Duration::ZERO),
        // The app's answers, buffered.
        row("a HEAD answered as Rack::Head answers it", head).reads(&[true]).reply(full(200, LENGTH_FIVE, b"")),
        row("a 304 with the app's length and a body", get("/", "")).reply(full(304, LENGTH_FIVE, b"hello")),
        row("the app's connection: close", get("/", "")).reply(full(200, &[("connection", "close")], b"bye")),
        row("an app length shorter than its body", get("/", "")).reply(full(200, &[("content-length", "2")], b"hello")),
        row("the app's transfer-encoding on a buffered body", get("/", "")).reply(full(200, &[TE], b"hello")),
        // The app's answers, streamed, and cut short.
        row("a streamed answer", get("/", "")).reply(streamed(200, TEXT, &[b"hel", b"lo"])),
        row("a streamed answer to a HEAD", head).reads(&[true]).reply(streamed(200, TEXT, &[b"hello"])),
        row("a 204 streamed with the app's length", get("/", "")).reply(streamed(204, LENGTH_FIVE, &[b"hello"])),
        row("a streamed answer past the app's length in two writes", get("/", "")).reply(streamed(200, LENGTH_FIVE, &[b"hel", b"lo!!"])),
        row("a streamed answer with the app's length not a number", get("/", "")).reply(streamed(200, &[("content-length", "five")], &[b"hello"])),
        row("a streamed length cut short", get("/", "")).reply(Reply { status: 200, headers: &[("content-length", "10")], body: Body::Stops(&[b"hello"]) }),
        row("a stream the app raises out of", get("/", "")).reply(Reply { status: 200, headers: TEXT, body: Body::Raise(&[b"hel"]) }),
        // The recorded differences.
        row("the app's transfer-encoding on a stream", get("/", "")).reply(streamed(200, &[("transfer-encoding", "gzip")], &[b"hello"]))
            .deviation("inline drops an app's transfer-encoding and frames the body itself",
                &["HTTP/1.1 200 OK", "server: Kino", "transfer-encoding: chunked", "body: 5\\r\\nhello\\r\\n0\\r\\n\\r\\n", "open"]),
        row("a streamed answer to a HEAD with the app's length, then its encoding", head).reads(&[true]).reply(streamed(200, &[CL, TE], &[b"hello"]))
            .deviation("inline writes the length, where hyper's refusal depends on the order",
                &["HTTP/1.1 200 OK", "content-length: 5", "server: Kino", "body: ", "open"]),
        row("the app's close in a value past ASCII", get("/", "")).reply(full(200, &[("connection", "close, caf\u{e9}")], b"hello"))
            .deviation("inline ends the connection on a close hyper cannot read in a value past ASCII",
                &["HTTP/1.1 200 OK", "connection: close, caf\u{e9}", "content-length: 5", "server: Kino", "body: hello", "closed"]),
        // Handed back, a row a reason, the bytes read replayed to hyper.
        row("a POST past 64 KiB", post(&vec![b'b'; 64 * 1024 + 1])).handed_back("body"),
        row("a GET, a chunked PUT, then a GET that closes", [&get("/1", "")[..], chunked_put.as_bytes(), &get("/3", "connection: close\r\n")].concat()).reads(&[false; 3]).handed_back("chunked"),
        row("HTTP/1.0", "GET / HTTP/1.0\r\n\r\n").handed_back("version"),
        row("101 headers", get("/", &many_headers(100))).handed_back("malformed"),
        row("a head of 16 KiB and a byte", past_cap.clone()).handed_back("too_large"),
        row("a slow head past 16 KiB", past_cap).pieces(1024, Duration::from_millis(2)).handed_back("too_large"),
        row("absolute-form", get("http://x.test/a?b=1", "")).handed_back("target"),
        row("expect: 100-continue", "POST /up HTTP/1.1\r\nhost: x\r\nexpect: 100-continue\r\ncontent-length: 5\r\n\r\nhello").handed_back("expect"),
        row("upgrade", get("/", "upgrade: websocket\r\n")).handed_back("upgrade"),
        // hyper answers a CONNECT with no body and ends the connection:
        // what follows it is a tunnel's, never a request.
        row("a CONNECT with a request behind it", "CONNECT /x HTTP/1.1\r\nhost: x\r\n\r\nGET /y HTTP/1.1\r\nhost: x\r\n\r\n").handed_back("upgrade"),
    ]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inline_and_hyper_answer_the_corpus_the_same() {
    // Some rows cut streams, which tests of the process-wide counts see.
    let _serial = crate::testing::SERIAL.lock().await;
    let rows = corpus();
    let mut mismatches = Vec::new();
    for row in &rows {
        let hyper = hyper_view(row).await;
        let (inline, ran, handed) = inline_view(row).await;
        let sent = row.sent.concat();
        let show = || {
            format!(
                "{}\n  sent: {}\n  ran inline: {ran:?}, handed back: {handed}\n  inline: {:#?}\n  hyper: {:#?}",
                row.name,
                sent[..sent.len().min(400)].escape_ascii(),
                lines(&inline, true),
                lines(&hyper, true),
            )
        };
        if handed != row.handed_back.is_some() {
            mismatches.push(format!("{} (handed back: {:?})", show(), row.handed_back));
            continue;
        }
        match &row.deviation {
            None if inline == hyper => {}
            None => mismatches.push(show()),
            Some((deviation, pinned)) if lines(&inline, false) != *pinned => {
                mismatches.push(format!("{} ({deviation})", show()));
            }
            Some(_) => {}
        }
    }
    assert!(
        mismatches.is_empty(),
        "{} of {} rows differ:\n\n{}",
        mismatches.len(),
        rows.len(),
        mismatches.join("\n\n")
    );
}

/// The subset parser, fuzzed.
mod parser {
    use std::convert::Infallible;
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use parking_lot::Mutex;
    use proptest::collection::vec;
    use proptest::prelude::*;
    use proptest::sample::{select, Index};

    use crate::inline::parse::{parse_head, Parsed};

    const TCHAR: &[u8] =
        b"!#$%&'*+-.^_`|~0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

    fn token(max: usize) -> impl Strategy<Value = Vec<u8>> {
        vec(select(TCHAR), 1..=max)
    }

    fn method() -> impl Strategy<Value = Vec<u8>> {
        prop_oneof![
            8 => select(&[
                "GET", "HEAD", "POST", "PUT", "DELETE", "OPTIONS", "PATCH", "CONNECT", "TRACE",
            ][..])
            .prop_map(|m| m.as_bytes().to_vec()),
            1 => token(8),
        ]
    }

    fn target() -> impl Strategy<Value = Vec<u8>> {
        // Unreserved, sub-delims, ':' '@' '%' '/' '?'; now and then one
        // that needs escaping, or a byte past ASCII.
        let path_byte = prop_oneof![
            20 => select(&b"abcxyzABC0189-._~!$&'()*+,;=:@%/?"[..]),
            1 => select(&b"[]{}|\\^`\"<>\x7f\xc3\xa9#"[..]),
        ];
        prop_oneof![
            12 => vec(path_byte, 0..24).prop_map(|rest| [&b"/"[..], &rest].concat()),
            1 => Just(b"*".to_vec()),
            1 => Just(b"http://x.test/a?b".to_vec()),
            1 => Just(b"x.test:443".to_vec()),
        ]
    }

    fn value() -> impl Strategy<Value = Vec<u8>> {
        let byte = prop_oneof![
            8 => 0x21u8..0x7f,
            2 => Just(b' '),
            1 => Just(b'\t'),
            1 => 0x80u8..=0xff,
        ];
        vec(byte, 0..16)
    }

    fn header() -> impl Strategy<Value = (Vec<u8>, Vec<u8>)> {
        let named = |names: &'static [&'static str]| {
            select(names).prop_map(|name| name.as_bytes().to_vec())
        };
        let length = prop_oneof![
            3 => (0u64..100_000).prop_map(|n| n.to_string().into_bytes()),
            1 => select(
                &[
                    "05", "+5", " 5 ", "5, 5", "", "5 5", "18446744073709551614",
                    "18446744073709551615", "18446744073709551616", "-1", "0x5",
                ][..],
            )
            .prop_map(|v| v.as_bytes().to_vec()),
        ];
        let connection = vec(
            select(
                &[
                    "close",
                    "Close",
                    "keep-alive",
                    "upgrade",
                    "te",
                    "caf\u{e9}",
                    "",
                    " close ",
                ][..],
            ),
            1..4,
        )
        .prop_map(|tokens| tokens.join(",").into_bytes());
        prop_oneof![
            12 => (
                prop_oneof![named(&["host", "accept", "cookie", "x-a", "user-agent"]), token(12)],
                value(),
            ),
            3 => (named(&["content-length", "Content-Length", "CONTENT-LENGTH"]), length),
            4 => (named(&["connection", "Connection"]), connection),
            1 => (
                named(&[
                    "transfer-encoding", "expect", "upgrade", "http2-settings", "te", "trailer",
                ]),
                value(),
            ),
        ]
    }

    /// What a mutation does to a head.
    #[derive(Clone, Debug)]
    enum Mutation {
        /// One of CR, LF, NUL, space or colon, at a point.
        Insert(Index, u8),
        Duplicate(Index),
        Version(&'static str),
        Truncate(Index),
    }

    fn mutation() -> impl Strategy<Value = Mutation> {
        const VERSIONS: [&str; 6] = [
            "HTTP/1.0",
            "HTTP/2.0",
            "HTTP/1.2",
            "HTTP/0.9",
            "HTTP/1.10",
            "http/1.1",
        ];
        prop_oneof![
            (any::<Index>(), select(&b"\r\n\0 :"[..])).prop_map(|(at, b)| Mutation::Insert(at, b)),
            any::<Index>().prop_map(Mutation::Duplicate),
            select(&VERSIONS[..]).prop_map(Mutation::Version),
            any::<Index>().prop_map(Mutation::Truncate),
        ]
    }

    /// A valid head from method, target and header generators, then up
    /// to three mutations.
    fn head_strategy() -> impl Strategy<Value = Vec<u8>> {
        let line_end = prop_oneof![9 => Just(&b"\r\n"[..]), 1 => Just(&b"\n"[..])];
        let space = prop_oneof![4 => Just(&b" "[..]), 1 => Just(&b""[..]), 1 => Just(&b"\t"[..])];
        (
            method(),
            target(),
            vec((header(), space), 0..8),
            line_end,
            prop_oneof![4 => Just(0), 2 => Just(1), 1 => Just(2), 1 => Just(3)]
                .prop_flat_map(|n| vec(mutation(), n)),
        )
            .prop_map(|(method, target, mut headers, eol, mutations)| {
                let mut version = "HTTP/1.1";
                for mutation in &mutations {
                    match mutation {
                        Mutation::Duplicate(at) if !headers.is_empty() => {
                            let line = headers[at.index(headers.len())].clone();
                            headers.insert(at.index(headers.len()), line);
                        }
                        Mutation::Version(other) => version = other,
                        _ => {}
                    }
                }
                let mut head = [&method[..], b" ", &target, b" ", version.as_bytes(), eol].concat();
                for ((name, value), space) in &headers {
                    head.extend([&name[..], b":", space, value, eol].concat());
                }
                head.extend_from_slice(eol);
                for mutation in &mutations {
                    match mutation {
                        Mutation::Insert(at, byte) => head.insert(at.index(head.len() + 1), *byte),
                        Mutation::Truncate(at) => head.truncate(at.index(head.len() + 1)),
                        _ => {}
                    }
                }
                head
            })
    }

    /// A head_strategy head with up to seven bytes flipped.
    fn flipped() -> impl Strategy<Value = Vec<u8>> {
        let flips = vec((any::<Index>(), any::<u8>()), 1..8);
        (head_strategy(), flips).prop_map(|(mut head, flips)| {
            for (at, byte) in flips {
                if !head.is_empty() {
                    let at = at.index(head.len());
                    head[at] = byte;
                }
            }
            head
        })
    }

    /// What hyper makes of a request head: its parts, its body's length,
    /// and whether its answer closes the connection.
    #[derive(Debug, PartialEq)]
    struct Read {
        method: http::Method,
        uri: http::Uri,
        headers: http::HeaderMap,
        length: Option<u64>,
        close: bool,
    }

    /// `head`, read by hyper's HTTP/1 server; None where hyper refuses it.
    async fn hyper_reads(head: &[u8]) -> Option<Read> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let seen = Arc::new(Mutex::new(None));
        let service = {
            let seen = seen.clone();
            hyper::service::service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
                let length = hyper::body::Body::size_hint(req.body()).exact();
                // Its body kept, unread: hyper closes a connection whose
                // request body was let go of.
                let (parts, body) = req.into_parts();
                *seen.lock() = Some((parts.method, parts.uri, parts.headers, length, body));
                async {
                    Ok::<_, Infallible>(hyper::Response::new(http_body_util::Empty::<Bytes>::new()))
                }
            })
        };
        let served = tokio::spawn(
            hyper::server::conn::http1::Builder::new()
                .serve_connection(hyper_util::rt::TokioIo::new(server), service),
        );
        client.write_all(head).await.ok()?;
        let mut answer = Vec::new();
        let mut chunk = [0; 4096];
        while !answer.windows(4).any(|w| w == b"\r\n\r\n") {
            match tokio::time::timeout(Duration::from_secs(2), client.read(&mut chunk)).await {
                Ok(Ok(0) | Err(_)) => break,
                Ok(Ok(n)) => answer.extend_from_slice(&chunk[..n]),
                Err(_) => panic!("hyper neither answered nor ended {:?}", head.escape_ascii()),
            }
        }
        served.abort();
        let (method, uri, headers, length, _body) = seen.lock().take()?;
        let answer = String::from_utf8_lossy(&answer).to_ascii_lowercase();
        Some(Read {
            method,
            uri,
            headers,
            length,
            close: answer.contains("\r\nconnection: close\r\n"),
        })
    }

    /// One runtime for the cases' hyper.
    fn runtime() -> &'static tokio::runtime::Runtime {
        static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
        RUNTIME.get_or_init(|| {
            let mut builder = tokio::runtime::Builder::new_current_thread();
            builder.enable_all().build().expect("a runtime")
        })
    }

    proptest! {
        // Nothing written into the tree: a failure prints its case.
        #![proptest_config(ProptestConfig { cases: 2_000, failure_persistence: None, ..ProptestConfig::default() })]

        /// Accepted: the subset parser takes the head, and its parts
        /// convert (InlineConn::request hands back a head whose parts do
        /// not). A head, a head with bytes flipped, or random bytes: it
        /// never panics.
        #[test]
        fn the_subset_parser_never_panics_nor_accepts_what_httparse_and_http_refuse(
            head in prop_oneof![3 => head_strategy(), 1 => flipped(), 1 => vec(any::<u8>(), 0..20_000)]
        ) {
            let Parsed::Head(parsed) = parse_head(&head) else {
                return Ok(());
            };
            let bytes = Bytes::copy_from_slice(&head[..parsed.len]);
            let Some((uri, headers)) = parsed.parts(&bytes) else {
                return Ok(());
            };
            let shown = head.escape_ascii().to_string();

            let mut slots = [httparse::EMPTY_HEADER; 100];
            let mut req = httparse::Request::new(&mut slots);
            prop_assert_eq!(
                req.parse(&head),
                Ok(httparse::Status::Complete(parsed.len)),
                "httparse: {}", shown
            );
            let method = req.method.unwrap_or("").as_bytes();
            prop_assert!(http::Method::from_bytes(method).is_ok(), "method: {}", shown);
            prop_assert!(http::Uri::try_from(req.path.unwrap_or("")).is_ok(), "target: {}", shown);
            for header in req.headers.iter() {
                let name = http::HeaderName::from_bytes(header.name.as_bytes());
                prop_assert!(name.is_ok(), "name: {}", shown);
                let value = http::HeaderValue::from_bytes(header.value);
                prop_assert!(value.is_ok(), "value: {}", shown);
            }

            let hyper = runtime().block_on(hyper_reads(&head[..parsed.len]));
            let inline = Read {
                method: parsed.method.clone(),
                uri,
                headers,
                length: Some(parsed.content_length),
                close: parsed.close,
            };
            prop_assert_eq!(Some(inline), hyper, "hyper: {}", shown);
        }
    }
}
