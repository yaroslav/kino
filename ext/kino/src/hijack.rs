//! Full `rack.hijack` (Rack 3) on HTTP/1: the app gets one end of a Unix
//! socket pair and writes its own response there. The bridge on the other
//! end replays the head through hyper (so TLS works), then joins the two
//! connections (101) or streams the Content-Length body behind it. An
//! answer that has no body (a 204, a 304, the answer to a HEAD) needs no
//! Content-Length; any other framing gets a 502. Only a request that asked
//! to upgrade is offered one: its bridge runs on the task holding the
//! upgrade (upgrade::settle).

use std::fmt;
use std::future::Future;
use std::io;
use std::os::unix::net::UnixStream;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::{Buf, Bytes, BytesMut};
use http::header::{CONTENT_LENGTH, TRANSFER_ENCODING};
use http::StatusCode;
use hyper::upgrade::OnUpgrade;
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, ReadBuf};
use tokio::sync::oneshot;

use crate::counters::Counted;
use crate::framing;
use crate::registry::ServerInner;
use crate::request::RequestCtx;
use crate::response::{
    close_connection, full_body, plain_response, stream_body_holding, FrameResult, HyperResponse,
};
use crate::slot::WorkerSlot;
use crate::upgrade::Claim;

/// Longest response head the app may write; past it, the client gets 502.
const MAX_HEAD: usize = 16 * 1024;
/// hyper's own default.
const MAX_HEADERS: usize = 100;
const READ_CHUNK: usize = 16 * 1024;
/// Body frames that may wait for a slow client, as for streamed responses.
const BODY_FRAMES: usize = 8;
/// Most read at once after the workers stopped; an answer not whole by then gets 503.
const AT_DRAIN_MAX: usize = 1024 * 1024;

/// A hijacked connection's Rust side: its end of the pair, and the answer the app's head becomes.
pub(crate) struct Bridge {
    io: UnixStream,
    answer: oneshot::Sender<HyperResponse>,
    /// The 101 the app answered with, when it did not write one.
    head: Option<HyperResponse>,
    /// Counts against the hijacked worker's slot until the bridge ends, so the pool keeps it.
    open: Option<Counted<WorkerSlot>>,
    head_request: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Refused {
    NotOffered,
    AlreadyHijacked,
    /// Answered meanwhile (a request timeout's 504).
    Answered,
    Busy,
    Gone,
    Socket(io::ErrorKind),
}

impl fmt::Display for Refused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Refused::NotOffered => f.write_str(
                "offered on HTTP/1.1 requests with an Upgrade header, and not once a native \
                 WebSocket took the connection",
            ),
            Refused::AlreadyHijacked => f.write_str("the connection is already hijacked"),
            Refused::Answered => f.write_str("the request was already answered"),
            Refused::Busy => f.write_str(crate::request::BUSY),
            Refused::Gone => f.write_str("the server is shutting down"),
            Refused::Socket(kind) => write!(f, "no socket pair: {kind}"),
        }
    }
}

/// An HTTP/1.1 request that asked to upgrade, until something claims its connection.
pub fn offered(ctx: &RequestCtx) -> bool {
    ctx.upgrade.is_some()
}

/// rack.hijack: the app's end of a fresh socket pair; the bridge answers with what it writes
/// there. With `head`, the 101 an app answered with a callable, the bridge joins at once.
pub fn take(ctx: &mut RequestCtx, head: Option<HyperResponse>) -> Result<UnixStream, Refused> {
    if ctx.hijacked {
        return Err(Refused::AlreadyHijacked);
    }
    // A native WebSocket took the upgrade with it. After a 500 the claim below
    // finds the request answered, and after a 504 the bridge lets go at once.
    if !offered(ctx) {
        return Err(Refused::NotOffered);
    }
    // First, so running out of file descriptors leaves the hijack offered.
    let (app, io) = UnixStream::pair().map_err(|e| Refused::Socket(e.kind()))?;
    let answer = ctx.responder.hijack().ok_or(Refused::Answered)?;
    // The request is the app's now, so a drain does not wait for it.
    if let Some(server) = crate::registry::try_get(ctx.server_id) {
        if ctx.responder.leave_in_flight() {
            server.counters.in_flight.fetch_sub(1, Ordering::Relaxed);
        }
    }
    let bridge = Bridge {
        io,
        answer,
        head,
        open: ctx
            .slot
            .as_ref()
            .map(|slot| Counted::on(slot, |slot| &slot.hijacked)),
        head_request: ctx.method == http::Method::HEAD,
    };
    let upgrade = ctx.upgrade.take().ok_or(Refused::NotOffered)?;
    if !upgrade.claim(Claim::Hijack(bridge)) {
        return Err(Refused::Gone);
    }
    ctx.hijacked = true;
    Ok(app)
}

/// How far a drain has closed the bridges; it only rises.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Closed {
    Nothing,
    /// Requests have drained: a switched connection is the app's alone and
    /// closes now; an app's end that closes silently gets All's 503 from here.
    Switched,
    /// The workers have stopped: every bridge closes; an unanswered request gets 503.
    All,
}

/// Never reopens a bridge.
pub(crate) fn close(server: &ServerInner, closed: Closed) {
    server.bridges_closed.send_if_modified(|now| {
        let rises = *now < closed;
        if rises {
            *now = closed;
        }
        rises
    });
}

async fn closing(server: &ServerInner, closed: Closed) {
    let mut bridges = server.bridges_closed.subscribe();
    let _ = bridges.wait_for(|now| *now >= closed).await;
}

/// Kino's own answer: it closes the connection, which is the app's now.
fn own_answer(status: u16, message: &'static str) -> HyperResponse {
    let mut response = plain_response(status, message);
    close_connection(&mut response);
    response
}

fn unanswered() -> HyperResponse {
    own_answer(503, "Service Unavailable\n")
}

/// Why the bridge has no head from the app.
enum NoHead {
    /// The app closed its end without writing a byte.
    Silent,
    /// Malformed, past MAX_HEAD or cut short; or the read failed.
    Bad,
}

impl Bridge {
    /// Answer with the head the app writes, then carry the rest: both ways
    /// once switched (101), else as the body.
    pub(crate) async fn run(self, on_upgrade: OnUpgrade, server: &ServerInner) {
        // Held by the bridge and its answer until hyper takes it; close_bridges waits.
        let running = BridgeHold(Arc::new(Counted::hold(&server.bridges_running, 1)));
        let Bridge {
            io,
            mut answer,
            head,
            open: _open,
            head_request,
        } = self;
        // The answer that leaves nothing to carry: Kino's own, or one the app wrote whole in time.
        let last = 'last: {
            let Ok(mut io) = into_tokio(io) else {
                break 'last own_answer(502, "Bad Gateway\n");
            };
            let mut buf = BytesMut::new();
            // Whether the head was read after the workers stopped.
            let mut at_drain = false;
            let head = match head {
                Some(head) => Ok(head.map(drop)),
                // A head the app wrote goes out even when the drain is over.
                None => tokio::select! {
                    biased;
                    head = read_head(&mut io, &mut buf) => head,
                    // Answered meanwhile (a request timeout's 504), or the client left.
                    () = answer.closed() => return,
                    // The runtime may not have seen what the app wrote before its worker
                    // stopped: read the socket; no whole head by then gets a 503.
                    () = closing(server, Closed::All) => {
                        read_written(&io, &mut buf);
                        let Ok(Some(head)) = parse_head(&mut buf) else {
                            break 'last unanswered();
                        };
                        at_drain = true;
                        Ok(head)
                    }
                },
            };
            let mut head = match head {
                Ok(head) => head,
                // Past Switched, a worker Ractor's objects (a long poll's IO) go with it:
                // a silent close then is an unanswered request, not a failed app.
                Err(NoHead::Silent) if *server.bridges_closed.borrow() >= Closed::Switched => {
                    break 'last unanswered();
                }
                Err(_) => break 'last own_answer(502, "Bad Gateway\n"),
            };

            if head.status() == StatusCode::SWITCHING_PROTOCOLS {
                let response = head.map(|()| full_body(Bytes::new()));
                if answer.send(held(response, &running)).is_err() {
                    return; // the client is gone
                }
                // The connection is the app's now: it closes once requests drain, not at All.
                tokio::select! {
                    () = switch(on_upgrade, buf, io) => {}
                    () = closing(server, Closed::Switched) => {}
                }
                return;
            }

            let Some(length) = body_length(&head, head_request) else {
                break 'last own_answer(502, "Bad Gateway\n");
            };
            close_connection(&mut head);
            let (frames, body) = flume::bounded(BODY_FRAMES);
            // The body holds the bridge counted too, until hyper has its tail.
            let response = head.map(|()| stream_body_holding(body, running.0.clone()));
            if at_drain {
                // Nothing went out and nothing more comes: its body goes from memory
                // if it is all there; else 503.
                let whole = usize::try_from(length).ok();
                let Some(length) = whole.filter(|&length| buf.len() >= length) else {
                    break 'last unanswered();
                };
                if length > 0 {
                    buf.truncate(length);
                    let _ = frames.try_send(Ok(hyper::body::Frame::data(buf.freeze())));
                }
                break 'last response;
            }
            if answer.send(held(response, &running)).is_err() {
                return;
            }
            let mut app = UntilDrained {
                io: &mut io,
                drained: Some(Box::pin(closing(server, Closed::All))),
            };
            if let Err(error) = forward(&mut app, buf, length, &frames).await {
                // The error frame makes hyper abort, so a cut body cannot pass for whole.
                let _ = frames.send_async(Err(error)).await;
            }
            return;
        };
        let _ = answer.send(held(last, &running));
    }
}

/// What the app wrote behind its 101 goes first, then bytes go both ways.
async fn switch(on_upgrade: OnUpgrade, behind: BytesMut, app: tokio::net::UnixStream) {
    let Ok(upgraded) = on_upgrade.await else {
        return;
    };
    let mut client = TokioIo::new(upgraded);
    if client.write_all(&behind).await.is_err() || client.flush().await.is_err() {
        return;
    }
    join(client, app).await;
}

/// The client closing its side reaches the app as EOF; the app closing its
/// end closes the client outright, so a client cannot keep the bridge.
async fn join(client: TokioIo<hyper::upgrade::Upgraded>, mut app: tokio::net::UnixStream) {
    let (mut client_rd, mut client_wr) = tokio::io::split(client);
    let (mut app_rd, mut app_wr) = app.split();
    let to_client = async {
        let _ = tokio::io::copy(&mut app_rd, &mut client_wr).await;
        let _ = client_wr.shutdown().await;
    };
    let to_app = async {
        let _ = tokio::io::copy(&mut client_rd, &mut app_wr).await;
        let _ = app_wr.shutdown().await;
        // The app may still answer before it closes its end.
        std::future::pending::<()>().await
    };
    tokio::select! {
        () = to_client => {}
        () = to_app => {}
    }
}

/// A response extension of its own type, so no other count can replace it.
#[derive(Clone)]
struct BridgeHold(Arc<Counted>);

/// hyper drops a response's extensions as it encodes the head, ending the hold.
fn held(mut response: HyperResponse, running: &BridgeHold) -> HyperResponse {
    response.extensions_mut().insert(running.clone());
    response
}

fn into_tokio(io: UnixStream) -> io::Result<tokio::net::UnixStream> {
    io.set_nonblocking(true)?;
    tokio::net::UnixStream::from_std(io)
}

/// Straight from the socket: the runtime's readiness can trail a write by a
/// scheduling slice. WouldBlock when nothing is there.
fn read_now(io: &tokio::net::UnixStream, into: &mut [u8]) -> io::Result<usize> {
    use std::io::Read;
    let socket = socket2::SockRef::from(io);
    loop {
        match (&*socket).read(into) {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            read => return read,
        }
    }
}

/// What the app wrote, up to AT_DRAIN_MAX (past it by less than a read).
fn read_written(io: &tokio::net::UnixStream, buf: &mut BytesMut) {
    let mut chunk = [0; READ_CHUNK];
    while buf.len() < AT_DRAIN_MAX {
        match read_now(io, &mut chunk) {
            Ok(n) if n > 0 => buf.extend_from_slice(&chunk[..n]),
            _ => break,
        }
    }
}

/// Through the runtime until the workers stop, then from the socket; what is missing cuts the body.
struct UntilDrained<'a> {
    io: &'a mut tokio::net::UnixStream,
    drained: Option<Pin<Box<dyn Future<Output = ()> + Send + 'a>>>,
}

impl AsyncRead for UntilDrained<'_> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        into: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = &mut *self;
        if let Some(drained) = this.drained.as_mut() {
            if let Poll::Ready(read) = Pin::new(&mut *this.io).poll_read(cx, into) {
                return Poll::Ready(read);
            }
            if drained.as_mut().poll(cx).is_pending() {
                return Poll::Pending;
            }
            this.drained = None;
        }
        Poll::Ready(match read_now(this.io, into.initialize_unfilled()) {
            Ok(n) => {
                into.advance(n);
                Ok(())
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                Err(io::Error::other("the server shut down mid-body"))
            }
            Err(error) => Err(error),
        })
    }
}

/// The app's response head, leaving what follows it in `buf`.
async fn read_head(
    io: &mut tokio::net::UnixStream,
    buf: &mut BytesMut,
) -> Result<hyper::Response<()>, NoHead> {
    loop {
        if let Some(head) = parse_head(buf).map_err(|()| NoHead::Bad)? {
            return Ok(head);
        }
        if buf.len() >= MAX_HEAD {
            return Err(NoHead::Bad);
        }
        buf.reserve(READ_CHUNK);
        if io.read_buf(buf).await.map_err(|_| NoHead::Bad)? == 0 {
            return Err(if buf.is_empty() {
                NoHead::Silent
            } else {
                NoHead::Bad
            });
        }
    }
}

/// Takes a whole head off `buf`; Ok(None) while it is partial.
fn parse_head(buf: &mut BytesMut) -> Result<Option<hyper::Response<()>>, ()> {
    let mut slots = [httparse::EMPTY_HEADER; MAX_HEADERS];
    let mut parsed = httparse::Response::new(&mut slots);
    let length = match parsed.parse(buf) {
        Ok(httparse::Status::Complete(length)) if length <= MAX_HEAD => length,
        Ok(httparse::Status::Partial) => return Ok(None),
        _ => return Err(()),
    };
    let mut head = hyper::Response::builder().status(parsed.code.ok_or(())?);
    for header in parsed.headers.iter() {
        head = head.header(header.name, header.value);
    }
    let head = head.body(()).map_err(drop)?;
    // hyper sends one final head; of the 1xx, only a switch is final.
    let status = head.status();
    if status.is_informational() && status != StatusCode::SWITCHING_PROTOCOLS {
        return Err(());
    }
    buf.advance(length);
    Ok(Some(head))
}

/// The Content-Length of the body behind a head; None for a body framed any
/// other way (chunked, or by the close), which the bridge does not carry, and
/// for a Content-Length hyper would refuse to send. A bodiless answer has no
/// body whatever its head declares, which is what hyper sends of it.
fn body_length(head: &hyper::Response<()>, head_request: bool) -> Option<u64> {
    let declared = declared_length(head.headers()).ok()?;
    if framing::bodiless(head.status().as_u16(), head_request) {
        return Some(0);
    }
    // A transfer coding voids any length (RFC 9112 6.3).
    if head.headers().contains_key(TRANSFER_ENCODING) {
        return None;
    }
    declared
}

/// The length a head declares, Ok(None) when it declares none. Err for what
/// hyper's encoder refuses by dropping the connection unanswered: a value
/// that is not all ASCII digits (a sign, a list, a number past u64), or two
/// values that differ.
fn declared_length(headers: &http::HeaderMap) -> Result<Option<u64>, ()> {
    let mut declared = None;
    for value in headers.get_all(CONTENT_LENGTH) {
        let digits = value.as_bytes();
        if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
            return Err(());
        }
        let length = std::str::from_utf8(digits).map_err(drop)?;
        let length = length.parse::<u64>().map_err(drop)?;
        if declared.is_some_and(|first| first != length) {
            return Err(());
        }
        declared = Some(length);
    }
    Ok(declared)
}

/// The body's `length` bytes as frames, from `buf` first, then from the app.
async fn forward(
    app: &mut UntilDrained<'_>,
    mut buf: BytesMut,
    mut length: u64,
    frames: &flume::Sender<FrameResult>,
) -> io::Result<()> {
    while length > 0 {
        if buf.is_empty() {
            buf.reserve(READ_CHUNK);
            if app.read_buf(&mut buf).await? == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "the app closed its hijacked connection mid-body",
                ));
            }
        }
        let n = buf.len().min(usize::try_from(length).unwrap_or(usize::MAX));
        let data = hyper::body::Frame::data(buf.split_to(n).freeze());
        // Fails once the client is gone.
        let sent = frames.send_async(Ok(data)).await;
        sent.map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))?;
        length -= n as u64;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
