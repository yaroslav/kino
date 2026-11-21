//! The response half of a request's lifecycle. A `Responder` is shared
//! between the Ruby-held `Request` handle and (via `Weak`) the worker slot,
//! so exactly one of three parties can answer the client: the app (normal
//! path), the supervisor (`abort_inflight` after a ractor crash), or the
//! `RequestCtx` Drop backstop. The `responded` flag makes the race benign.
//!
//! Two response shapes: `send_response` (complete, one shot) and
//! `send_stream_head` + a bounded frame channel (streaming bodies). The
//! channel being bounded(8) is the client-side backpressure: a slow client
//! makes `write_chunk` block in Ruby, with the GVL released.

use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body_util::{Either, Full, StreamBody};
use parking_lot::Mutex;
use tokio::sync::oneshot;

use crate::counters::{tallied, Tallied, Tally};

pub type BodyFrame = hyper::body::Frame<Bytes>;
pub type FrameResult = Result<BodyFrame, io::Error>;
/// Both response shapes as one concrete type: no per-response Box or
/// vtable, unlike BoxBody. Either erases the two error types itself.
pub type ResponseBody = Either<Full<Bytes>, Streamed>;
pub type HyperResponse = hyper::Response<ResponseBody>;

const STREAM_BUFFER: usize = 8;

tallied! {
    /// Why a streamed response ended before the app finished it.
    pub enum Cut {
        ClientGone => "client_gone",
        /// The drain's deadline passed with the stream still going.
        Shutdown => "shutdown",
        /// Its worker crashed, the app raised mid-stream, or the request was dropped.
        Crash => "crash",
    }
}

static CUT: Tally<Cut, { Cut::ALL.len() }> = Tally::new();

pub fn responses_cut() -> [(&'static str, u64); Cut::ALL.len()] {
    Cut::named(CUT.read())
}

/// Why the body forwarder stopped pulling a request body: read_body turns
/// these into raised errors instead of a (truncated) clean EOF.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub enum BodyAbandon {
    Oversize = 1,
    TimedOut = 2,
}

impl TryFrom<u8> for BodyAbandon {
    type Error = u8;

    fn try_from(code: u8) -> Result<Self, u8> {
        match code {
            1 => Ok(BodyAbandon::Oversize),
            2 => Ok(BodyAbandon::TimedOut),
            none => Err(none),
        }
    }
}

/// hyper's side of the answer: the head's oneshot, then a streamed body's channel.
#[derive(Default)]
struct HyperSink {
    head: Mutex<Option<oneshot::Sender<HyperResponse>>>,
    body: Mutex<Option<flume::Sender<FrameResult>>>,
    /// An accepted handshake's 101, held until on_open ran, with what tells the socket's
    /// task it went out; dropped unsent, the task learns it never will.
    held_101: Mutex<Option<(oneshot::Sender<()>, Box<HyperResponse>)>>,
}

#[derive(Default)]
pub struct Responder {
    sink: HyperSink,
    responded: AtomicBool,
    /// The forwarder's abandonment flag rides in the one per-request shared
    /// allocation rather than its own Arc. 0 = none, 1/2 = BodyAbandon.
    body_abandoned: AtomicU8,
    /// Out of in_flight: once hijacked or once handle_request ended, whichever came first.
    left_in_flight: AtomicBool,
    /// Cut short (the client left, or it was given up): it ends in an error even when its
    /// error frame found no room; counted once, for the first cause.
    stream_cut: AtomicBool,
    /// No cut counts: a bodiless answer, or a head that never went out (answered_first).
    uncounted: AtomicBool,
}

impl Responder {
    pub fn new(head_tx: oneshot::Sender<HyperResponse>) -> Self {
        let head = Mutex::new(Some(head_tx));
        Responder {
            sink: HyperSink {
                head,
                ..HyperSink::default()
            },
            ..Responder::default()
        }
    }

    /// True for the first caller only, which owes in_flight its decrement.
    pub fn leave_in_flight(&self) -> bool {
        !self.left_in_flight.swap(true, Ordering::AcqRel)
    }

    /// Record why the body forwarder gave up; the store happens before the
    /// sender drops, so a reader that saw Disconnected sees the flag too.
    pub fn abandon_body(&self, why: BodyAbandon) {
        self.body_abandoned.store(why as u8, Ordering::Relaxed);
    }

    /// Whether (and why) the forwarder abandoned the request body.
    pub fn body_abandoned(&self) -> Option<BodyAbandon> {
        BodyAbandon::try_from(self.body_abandoned.load(Ordering::Relaxed)).ok()
    }

    /// Claim the right to respond. First caller wins; everyone else gets None.
    fn claim(&self) -> Option<oneshot::Sender<HyperResponse>> {
        if self.responded.swap(true, Ordering::SeqCst) {
            return None;
        }
        self.sink.head.lock().take()
    }

    /// Complete response in one shot.
    pub fn send_response(&self, response: HyperResponse) -> bool {
        self.claim().is_some_and(|head| head.send(response).is_ok())
    }

    /// Hold a handshake's 101 for `switch`; `switched` hears when it goes out, and is dropped
    /// when anything else answers. hyper keeps a connection open for an upgrade after any
    /// answer but a 101, so the socket must not wait on hyper.
    pub fn hold_for_switch(&self, switched: oneshot::Sender<()>, response: HyperResponse) {
        *self.sink.held_101.lock() = Some((switched, Box::new(response)));
    }

    /// False when nothing is held, or something else answered first (a 500, a 504).
    pub fn switch(&self) -> bool {
        let mut held = self.sink.held_101.lock();
        let Some((switched, response)) = held.take() else {
            return false;
        };
        let sent = self.send_response(*response);
        if sent {
            let _ = switched.send(());
        }
        sent
    }

    pub fn cancel_switch(&self) -> bool {
        self.sink.held_101.lock().take().is_some()
    }

    /// Start a streaming response from a prepared head: send it now, return
    /// chunks later, unless `bodiless`; false when Kino answered first.
    pub fn send_stream_head(
        self: &Arc<Self>,
        builder: hyper::http::response::Builder,
        bodiless: bool,
    ) -> Result<bool, http::Error> {
        let Some(head) = self.claim() else {
            // A request starts its stream once: an open one is its own.
            if self.sink.body.lock().is_none() {
                self.answered_first();
            }
            return Ok(false);
        };
        // Closed when a request_timeout 504 went out instead, or the request is gone.
        if !head.is_closed() {
            self.uncounted.store(bodiless, Ordering::Relaxed);
            let (body, frames) = flume::bounded(STREAM_BUFFER);
            let streamed = Streamed::new(frames, Some(self.clone()), None);
            let response = builder.body(Either::Right(streamed))?;
            *self.sink.body.lock() = Some(body);
            if head.send(response).is_ok() {
                return Ok(true);
            }
        }
        self.answered_first();
        Ok(false)
    }

    /// Kino answered before the stream started, or the request is gone: the stream reads
    /// as cut, so the next write ends it, but no cut counts.
    fn answered_first(&self) {
        self.uncounted.store(true, Ordering::Relaxed);
        self.sink.body.lock().take();
        self.stream_cut.store(true, Ordering::SeqCst);
    }

    /// Hand the answer to a hijack's bridge; everyone else then finds it answered.
    pub fn hijack(&self) -> Option<oneshot::Sender<HyperResponse>> {
        self.claim()
    }

    /// Clone of the live frame sender, if a stream is open. The clone lets
    /// `write_chunk` block on a full channel without holding the lock.
    pub fn body_sender(&self) -> Option<flume::Sender<FrameResult>> {
        self.sink.body.lock().clone()
    }

    pub fn stream_cut(&self) -> bool {
        self.stream_cut.load(Ordering::SeqCst)
    }

    /// Clean end of stream: dropping the sender ends the hyper body.
    pub fn finish_stream(&self) {
        self.sink.body.lock().take();
    }

    pub fn cut_stream(&self, why: Cut) {
        if !self.stream_cut.swap(true, Ordering::SeqCst) && !self.uncounted.load(Ordering::Relaxed)
        {
            CUT.add(why);
        }
    }

    /// The "the app will never answer" path. Before the head: canned 500.
    /// Mid-stream: error frame, which makes hyper abort the connection
    /// rather than fake a clean end. Never touches the Ruby API: callable
    /// from Drop, tokio threads, and the supervisor. Never waits either.
    pub fn give_up(&self, why: Cut) {
        if let Some(tx) = self.claim() {
            let mut response = plain_response(500, "Internal Server Error\n");
            if self.cancel_switch() {
                close_connection(&mut response);
            }
            let _ = tx.send(response);
            return;
        }
        if let Some(body_tx) = self.sink.body.lock().take() {
            // Flagged before the send: without room for the frame, hyper fails it once drained.
            self.cut_stream(why);
            let _ = body_tx.try_send(Err(abandoned()));
        }
    }
}

fn abandoned() -> io::Error {
    io::Error::other("Kino: response abandoned mid-stream")
}

pub fn full_body(bytes: Bytes) -> ResponseBody {
    Either::Left(Full::new(bytes))
}

/// Holds `held` until hyper drops the body, in the poll that writes its last frame.
pub fn stream_body_holding(
    frames: flume::Receiver<FrameResult>,
    held: Arc<dyn Send + Sync>,
) -> ResponseBody {
    Either::Right(Streamed::new(frames, None, Some(held)))
}

pub struct Streamed {
    frames: StreamBody<flume::r#async::RecvStream<'static, FrameResult>>,
    cut_by: Option<Arc<Responder>>,
    _held: Option<Arc<dyn Send + Sync>>,
    /// hyper took something (the head, then data) since the body last made it wait.
    taken_since_wait: bool,
    failed: Option<io::Error>,
}

impl Streamed {
    fn new(
        frames: flume::Receiver<FrameResult>,
        cut_by: Option<Arc<Responder>>,
        held: Option<Arc<dyn Send + Sync>>,
    ) -> Streamed {
        Streamed {
            frames: StreamBody::new(frames.into_stream()),
            cut_by,
            _held: held,
            taken_since_wait: true,
            failed: None,
        }
    }
}

impl hyper::body::Body for Streamed {
    type Data = Bytes;
    type Error = io::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Bytes>, io::Error>>> {
        let this = self.get_mut();
        if let Some(error) = this.failed.take() {
            return Poll::Ready(Some(Err(error)));
        }
        let mut polled = Pin::new(&mut this.frames).poll_frame(cx);
        if matches!(polled, Poll::Ready(None))
            && this.cut_by.as_ref().is_some_and(|r| r.stream_cut())
        {
            polled = Poll::Ready(Some(Err(abandoned())));
        }
        match polled {
            // Once a body fails, hyper drops what it took but has not written (h1: the
            // write buffer, head included; h2: the reset stream's frames): wait once first.
            // h2 writing before this task runs again rests on tokio's order, not on hyper.
            Poll::Ready(Some(Err(error))) if this.taken_since_wait => {
                this.taken_since_wait = false;
                this.failed = Some(error);
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Poll::Ready(Some(Ok(frame))) => {
                this.taken_since_wait = true;
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Pending => {
                this.taken_since_wait = false;
                Poll::Pending
            }
            polled => polled,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.frames.is_end_stream()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        self.frames.size_hint()
    }
}

/// A request that asked to switch and did not: hyper would keep it for an upgrade.
pub fn close_connection<B>(response: &mut hyper::Response<B>) {
    response.headers_mut().insert(
        http::header::CONNECTION,
        http::HeaderValue::from_static("close"),
    );
}

/// Canned plain-text response, built entirely on the Rust side (used for
/// the 500/503/504 paths that never reach Ruby).
pub fn plain_response(status: u16, message: &'static str) -> HyperResponse {
    hyper::Response::builder()
        .status(status)
        .header("content-type", "text/plain")
        .body(full_body(Bytes::from_static(message.as_bytes())))
        .expect("static response must build")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pair() -> (
        Arc<Responder>,
        tokio::sync::oneshot::Receiver<HyperResponse>,
    ) {
        let (head_tx, head_rx) = tokio::sync::oneshot::channel();
        (Arc::new(Responder::new(head_tx)), head_rx)
    }

    #[test]
    fn backstop_sends_500_when_unsent_and_is_idempotent() {
        let (responder, mut head_rx) = pair();

        responder.give_up(Cut::Crash);
        responder.give_up(Cut::Crash); // second call must be a no-op

        assert_eq!(head_rx.try_recv().expect("head sent").status(), 500);
        assert!(!responder.send_response(plain_response(200, "late")));
    }

    /// A response whose stream has started.
    fn streaming() -> (
        Arc<Responder>,
        tokio::sync::oneshot::Receiver<HyperResponse>,
    ) {
        let (responder, head_rx) = pair();
        assert!(responder
            .send_stream_head(hyper::Response::builder().status(200), false)
            .expect("valid head"));
        (responder, head_rx)
    }

    #[test]
    fn a_stream_cut_counts_once_for_what_cut_it_first() {
        // The counts are process-wide; the other tests that cut a stream
        // take the lock too.
        let _serial = crate::testing::SERIAL.blocking_lock();
        let before = responses_cut();

        let (gone, _gone) = streaming();
        gone.cut_stream(Cut::ClientGone);
        gone.give_up(Cut::Crash); // the app raised once its client left
        let (stopped, _stopped) = streaming();
        stopped.give_up(Cut::Shutdown);
        stopped.give_up(Cut::Shutdown);
        let (raised, _raised) = streaming();
        raised.give_up(Cut::Crash);
        let (finished, _finished) = streaming();
        finished.finish_stream();
        finished.give_up(Cut::Crash);
        let (unstarted, _unstarted) = pair();
        unstarted.give_up(Cut::Crash);

        let cut: Vec<_> = responses_cut()
            .into_iter()
            .zip(before)
            .map(|((cause, now), (_, then))| (cause, now - then))
            .collect();
        assert_eq!(
            cut,
            [("client_gone", 1), ("shutdown", 1), ("crash", 1)],
            "a finished stream and an unstarted response are not cut"
        );
    }

    #[test]
    fn concurrent_claimants_yield_exactly_one_winner() {
        let (responder, mut head_rx) = pair();

        let winners: usize = std::thread::scope(|scope| {
            (0..16)
                .map(|i| {
                    let responder = responder.clone();
                    scope.spawn(move || responder.send_response(plain_response(200 + i, "race")))
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(|handle| usize::from(handle.join().expect("no panic")))
                .sum()
        });

        assert_eq!(winners, 1);
        assert!(head_rx.try_recv().is_ok());
    }
}
