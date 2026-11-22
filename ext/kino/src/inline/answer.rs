//! The answers to an inline connection's requests, written on its socket under its lock: the
//! app's, whole or streamed, and Kino's own (a give-up's 500, a request_timeout's 504).

use std::io::Write;
use std::net::Shutdown;
use std::os::fd::{AsRawFd, RawFd};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use http::{HeaderMap, StatusCode};

use super::encode::{self, Body};
use super::io::{InlineIo, IoState};
use crate::access_log::{Logged, Timing};
use crate::response::{Cut, Responder};
use crate::server::{InFlight, HEADER_READ_TIMEOUT};
use crate::slot::WorkerSlot;

/// Kino's own answer in place of the app's, after which the connection closes.
struct Canned {
    status: u16,
    head: &'static [u8],
    body: &'static [u8],
}

/// A give-up's 500, as response::plain_response(500, ..).
const GIVE_UP: Canned = Canned {
    status: 500,
    head:
        b"HTTP/1.1 500 Internal Server Error\r\n\
        content-type: text/plain\r\nconnection: close\r\ncontent-length: 22\r\nserver: Kino\r\n\r\n",
    body: b"Internal Server Error\n",
};
const _: () = assert!(GIVE_UP.body.len() == 22);

/// hyper's 504 (server::handle), but closing the connection: its job still runs the app, so no
/// next request could be read.
const TIMED_OUT: Canned = Canned {
    status: 504,
    head:
        b"HTTP/1.1 504 Gateway Timeout\r\n\
        content-type: text/plain\r\nconnection: close\r\ncontent-length: 16\r\nserver: Kino\r\n\r\n",
    body: b"Gateway Timeout\n",
};
const _: () = assert!(TIMED_OUT.body.len() == 16);

/// hyper's answer to a 1xx other than 101: a bare 500, after which the connection ends.
const INTERIM: &[u8] = b"HTTP/1.1 500 Internal Server Error\r\ncontent-length: 0\r\n\r\n";
const INTERIM_HEAD: &[u8] = b"HTTP/1.1 500 Internal Server Error\r\n\r\n";

/// The request a connection serves, counted in ServerInner::in_flight until its answer is out.
pub(super) struct Current {
    pub(super) responder: Arc<Responder>,
    pub(super) head_only: bool,
    /// The connection ends after this answer: asked by the request, or as a streamed head said.
    pub(super) close: bool,
    /// Counts it in flight until it drops: with its answer, or its connection.
    pub(super) _in_flight: InFlight,
    /// How the app's writes go out, once its streamed head did.
    pub(super) stream: Option<Streamed>,
    /// Its arrival, admitted as it is read: where its total and its time in Ruby start. Its
    /// completion line follows its answer's head, as hyper's does.
    pub(super) logged: Option<Box<Logged>>,
}

#[derive(Clone, Copy)]
pub(super) enum Streamed {
    Chunked,
    /// The app's length, this many bytes still to go.
    Length(usize),
}

/// What a write of a streamed body did.
#[derive(Debug, PartialEq, Eq)]
pub enum Wrote {
    /// On its way: what the socket did not take goes out at the next write's look or the job's.
    Done,
    /// The socket still holds the last write: wait for it to take more, say so
    /// (InlineIo::waited), then write this again; the descriptor stays open until then.
    Wait(RawFd),
    /// Nothing reaches the client any more: it left, the stream was cut, or the answer is whole.
    Gone,
    /// No stream of this request is going out: never started, or ended.
    Closed,
}

impl InlineIo {
    /// The answer is whole: unless the connection ends, its next head is due HEADER_READ_TIMEOUT
    /// after the answer went out (now, or once the job's flush sends the rest). Until then the
    /// deadline is the answer's, which bounds it only on a worker that leaves (expire).
    fn answered(&self, state: &IoState) {
        if !(state.closing && state.out.sent()) {
            self.deadline
                .due_in(&self.origin.server, HEADER_READ_TIMEOUT);
        }
    }

    /// Whether the connection ends after `current`'s answer (the app's own Connection aside):
    /// asked by the request, the drain, or a quarantine. A retiring worker's goes on, with hyper.
    fn closes(&self, current: &Current) -> bool {
        current.close
            || self.origin.server.is_draining()
            || self.slot.life.quarantined.load(Ordering::Relaxed)
    }

    /// The socket took the last of the stream's length: the connection's end is decided now, as
    /// hyper decides once that is flushed.
    fn length_out(&self, state: &mut IoState) {
        if let Some(current) = state.current.as_mut() {
            current.close = self.closes(current);
        }
    }

    /// With request_timeout set, the request served times out until its head goes out.
    pub(super) fn times_out(&self, state: &IoState) -> bool {
        self.origin.server.config.request_timeout_ms > 0
            && state
                .current
                .as_ref()
                .is_some_and(|current| !current.responder.claimed())
    }

    /// `logged`'s completion line: `total` after its arrival, with the breakdown for the app's
    /// own answer (`apps`), which has no queue wait (none inline) and no GC figures (a
    /// worker_loop worker's requests interleave); Kino's 504 and 500 carry none.
    #[inline]
    fn log(&self, logged: Option<Box<Logged>>, total: Option<Duration>, status: u16, apps: bool) {
        let (Some(logged), Some(log)) = (logged, &self.origin.server.access_log) else {
            return;
        };
        let total = total.unwrap_or_else(|| logged.elapsed());
        let timing = apps.then_some(Timing {
            wait: Duration::ZERO,
            ruby: total,
            gc: None,
        });
        logged.completion(log, status, total, timing);
    }

    /// The app's answer to an inline request, written now; what the socket does not take waits
    /// for the job's next look. False when something answered the request first.
    pub fn respond(
        &self,
        responder: &Responder,
        status: StatusCode,
        headers: &HeaderMap,
        body: &[u8],
    ) -> bool {
        let mut state = self.state.lock();
        if !state.serves(responder) || !responder.claim_inline() {
            return false;
        }
        let Some(mut current) = state.current.take() else {
            return false;
        };
        // Its total ends where its answer begins, before the write, as a stream's and hyper's do.
        let total = current.logged.as_ref().map(|logged| logged.elapsed());
        state.answered_on(&self.slot);
        let begun = self.begin(
            &mut state,
            &current,
            status,
            headers,
            Body::Length(body.len()),
        );
        if let Some((head, framing, close)) = begun {
            let body = if framing == Body::Length(0) {
                &[][..]
            } else {
                body
            };
            if state.write(&[&head, body]).is_err() || close {
                state.closing = true;
            }
            self.answered(&state);
        }
        drop(state);
        self.log(current.logged.take(), total, status.as_u16(), true);
        true
    }

    /// The head of the app's streamed answer, written now; its body follows through write_chunk
    /// to finish_stream, chunked unless the app gave its length. Where the head ends the answer,
    /// body writes go nowhere; where a body could have followed, the first write counts as its
    /// client gone, as hyper's dropped body counts. False when something answered the request
    /// first.
    pub fn start_stream(
        &self,
        responder: &Responder,
        status: StatusCode,
        headers: &HeaderMap,
    ) -> bool {
        let mut state = self.state.lock();
        if !state.serves(responder) || !responder.claim_inline() {
            // A request starts its stream once: one going out is its own.
            if state.streamed(responder).is_none() {
                responder.answered_first();
            }
            return false;
        }
        let Some(mut current) = state.current.take() else {
            return false;
        };
        self.log(current.logged.take(), None, status.as_u16(), true);
        let Some((head, framing, close)) =
            self.begin(&mut state, &current, status, headers, Body::Chunked)
        else {
            if crate::framing::bodiless(status.as_u16(), current.head_only) {
                responder.answered_first();
            } else {
                state.dropped = address(responder);
            }
            state.answered_on(&self.slot);
            return true;
        };
        if state.write(&[&head]).is_err() {
            state.current = Some(current);
            state.cut(responder, Cut::ClientGone);
            return true;
        }
        // As hyper drops the body of a response that may carry none.
        if crate::framing::bodiless(status.as_u16(), current.head_only) {
            responder.answered_first();
            state.closing |= close;
            state.answered_on(&self.slot);
            self.answered(&state);
            return true;
        }
        current.close = close;
        current.stream = Some(match framing {
            Body::Chunked => Streamed::Chunked,
            Body::Length(length) => Streamed::Length(length),
        });
        responder.set_streaming(true);
        state.current = Some(current);
        true
    }

    /// One chunk, framed as its head said, once the last one's rest is out: as much as the socket
    /// takes now, the rest kept for the next look. While the socket still holds some: Wait, the
    /// chunk not taken, so the buffer holds at most one chunk's rest; the descriptor stays open
    /// until the caller's wait is back (InlineIo::waited). A write error cuts the stream
    /// (Cut::ClientGone): Gone, and the connection ends after the request. Bytes past the app's
    /// length never go out, as hyper's encoder takes no more: the answer is whole, and a later
    /// write is Gone, counted as its client gone.
    pub fn write_chunk(&self, responder: &Responder, data: &[u8]) -> Wrote {
        let mut state = self.state.lock();
        let Some(&mut framing) = state.streamed(responder) else {
            return if responder.stream_cut() || state.dropped == address(responder) {
                Wrote::Gone
            } else {
                Wrote::Closed
            };
        };
        let length_unsent = matches!(framing, Streamed::Length(0)) && !state.out.sent();
        match state.flush() {
            Ok(true) => {}
            Ok(false) => {
                let Some(fd) = state.stream.as_ref().map(AsRawFd::as_raw_fd) else {
                    return Wrote::Gone;
                };
                state.lent += 1;
                return Wrote::Wait(fd);
            }
            Err(_) => return state.cut(responder, Cut::ClientGone),
        }
        // The last of its length went out just now.
        if length_unsent {
            self.length_out(&mut state);
        }
        // An empty chunk would end the body.
        if data.is_empty() {
            return Wrote::Done;
        }
        let written = match framing {
            Streamed::Chunked => {
                let mut size = [0; 18];
                let size = chunk_size(data.len(), &mut size);
                state.write(&[size, data, b"\r\n"])
            }
            Streamed::Length(left) => {
                let fits = &data[..data.len().min(left)];
                if let Some(Streamed::Length(left)) = state.streamed(responder) {
                    *left -= fits.len();
                }
                let written = if fits.is_empty() {
                    Ok(())
                } else {
                    state.write(&[fits])
                };
                if !fits.is_empty() && fits.len() == left && state.out.sent() {
                    self.length_out(&mut state);
                }
                if data.len() > left {
                    if let Some(current) = state.current.take() {
                        self.ended(&mut state, &current, true);
                    }
                    state.dropped = address(responder);
                    if fits.is_empty() {
                        return Wrote::Gone;
                    }
                }
                written
            }
        };
        match written {
            Ok(()) => Wrote::Done,
            Err(_) => state.cut(responder, Cut::ClientGone),
        }
    }

    /// The end of the app's stream: a chunked body's last chunk, after the rest; a body shorter
    /// than its length ends the connection where it stops, as hyper does. Nothing once ended or
    /// cut.
    pub fn finish_stream(&self, responder: &Responder) {
        let mut state = self.state.lock();
        let Some(&mut framing) = state.streamed(responder) else {
            return;
        };
        let Some(current) = state.current.take() else {
            return;
        };
        let whole = match framing {
            Streamed::Chunked => state.write(&[b"0\r\n\r\n"]).is_ok(),
            Streamed::Length(left) => left == 0,
        };
        self.ended(&mut state, &current, whole);
    }

    /// `current`'s stream ended, `whole` or not: its rest goes out at the job's next look, then
    /// the connection ends unless the answer is whole and may keep it. One still owed when the
    /// drain began ends too, as hyper's does once the drain turns keep-alive off mid-answer.
    fn ended(&self, state: &mut IoState, current: &Current, whole: bool) {
        state.answered_on(&self.slot);
        current.responder.set_streaming(false);
        let closes = match current.stream {
            Some(Streamed::Length(_)) => current.close,
            _ => self.closes(current),
        };
        if !whole || closes {
            state.closing = true;
        }
        self.answered(state);
    }

    /// Responder::give_up from any thread: the 500 if nothing answered yet, written now or never
    /// (the socket is shut down rather than wait), or a stream going out cut short.
    pub(crate) fn give_up(&self, responder: &Responder, why: Cut) {
        let mut state = self.state.lock();
        let ours = state.serves(responder);
        let claimed = responder.claim_inline();
        if !ours {
            return;
        }
        if !claimed {
            // A stream going out is cut, never ending as if whole; one whose length is all
            // written is whole already. What the socket takes of the rest goes now, as a job that
            // unwound looks no more, and it ends as finish_stream ends it; a worker given up on
            // shuts it down after (InlineIo::abandon).
            responder.cut_stream(why);
            match state.current.take() {
                Some(mut current) if matches!(current.stream, Some(Streamed::Length(0))) => {
                    let unsent = !state.out.sent();
                    if state.flush().unwrap_or(false) && unsent {
                        current.close = self.closes(&current);
                    }
                    self.ended(&mut state, &current, true);
                }
                _ => state.shutdown(),
            }
            return;
        }
        self.answer_and_end(&mut state, &GIVE_UP);
    }

    /// Past request_timeout with no answer begun: the 504 instead, counted with hyper's, written
    /// now or never, and the connection ends after it. The app runs on, as through hyper; its
    /// answer goes nowhere, and a stream it starts ends at its first write.
    pub(super) fn time_out(&self, state: &mut IoState) {
        let claimed = |current: &Current| current.responder.claim_inline();
        if state.current.as_ref().is_some_and(claimed) {
            let timeouts = &self.origin.server.counters.timeouts;
            timeouts.fetch_add(1, Ordering::Relaxed);
            self.answer_and_end(state, &TIMED_OUT);
        }
    }

    /// Kino's own answer in place of the app's to the request served, just claimed: written
    /// whole if the socket takes it now, else cut where it stopped, or not at all behind an
    /// earlier answer still buffered (shut down rather than wait: no job may be left to write the
    /// rest). The connection ends either way, and the request's line is logged with no breakdown.
    fn answer_and_end(&self, state: &mut IoState, canned: &Canned) {
        let Some(current) = state.current.take() else {
            return;
        };
        let body = if current.head_only { &[] } else { canned.body };
        let written = state.out.sent()
            && state.write(&[canned.head, body]).is_ok()
            && matches!(state.flush(), Ok(true));
        match &state.stream {
            Some(stream) if written => {
                let _ = stream.socket.shutdown(Shutdown::Write);
            }
            _ => state.shutdown(),
        }
        state.closing = true;
        self.log(current.logged, None, canned.status, false);
    }

    /// How `current`'s answer begins: its head, its body's framing and whether the connection
    /// ends after. None where it ends at once, as hyper ends it: after a 1xx other than 101, or
    /// for a head hyper refuses.
    fn begin(
        &self,
        state: &mut IoState,
        current: &Current,
        status: StatusCode,
        headers: &HeaderMap,
        body: Body,
    ) -> Option<(Vec<u8>, Body, bool)> {
        // As hyper: no 1xx but a 101 is final, and a 101 ends the connection, as no switch is
        // served here.
        let switching = status == StatusCode::SWITCHING_PROTOCOLS;
        if status.is_informational() && !switching {
            let answer = if current.head_only {
                INTERIM_HEAD
            } else {
                INTERIM
            };
            let _ = state.write(&[answer]);
            state.closing = true;
            return None;
        }
        let closes = self.closes(current);
        let begun = encode::head(status, headers, body, current.head_only, closes);
        let Some((head, framing, close)) = begun else {
            state.closing = true;
            return None;
        };
        Some((head, framing, close || switching))
    }
}

impl IoState {
    fn serves(&self, responder: &Responder) -> bool {
        self.current
            .as_ref()
            .is_some_and(|current| std::ptr::eq(Arc::as_ptr(&current.responder), responder))
    }

    fn streamed(&mut self, responder: &Responder) -> Option<&mut Streamed> {
        self.current
            .as_mut()
            .filter(|current| std::ptr::eq(Arc::as_ptr(&current.responder), responder))
            .and_then(|current| current.stream.as_mut())
    }

    /// The stream served is cut short (`why`, unless it was already): nothing more goes out, and
    /// the connection ends at its job's next look.
    fn cut(&mut self, responder: &Responder, why: Cut) -> Wrote {
        responder.cut_stream(why);
        self.current = None;
        self.out.clear();
        self.closing = true;
        Wrote::Gone
    }

    /// The answer is whole: until its job looks again, the connection counts as answered on
    /// `slot`.
    fn answered_on(&mut self, slot: &WorkerSlot) {
        if !self.answered {
            self.answered = true;
            slot.inline.answered.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// `responder`'s address, which tells its request apart.
fn address(responder: &Responder) -> usize {
    std::ptr::from_ref(responder) as usize
}

/// A chunk's size line, as hyper writes it, in `out`.
fn chunk_size(len: usize, out: &mut [u8; 18]) -> &[u8] {
    let mut line = std::io::Cursor::new(&mut out[..]);
    let _ = write!(line, "{len:X}\r\n");
    let end = line.position() as usize;
    &out[..end]
}
