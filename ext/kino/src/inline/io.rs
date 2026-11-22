//! What an inline connection shares with the threads that answer it (InlineIo, its state under
//! one lock), and how it ends: shut down, abandoned with its worker, or handed back to hyper.

use std::io::{ErrorKind, IoSlice, Read, Write};
use std::net::Shutdown;
use std::os::fd::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use bytes::Bytes;
use parking_lot::Mutex;

use super::answer::Current;
use crate::counters::Counted;
use crate::registry::ServerInner;
use crate::response::Cut;
use crate::server::{Held, Peer, StdConn, HEADER_READ_TIMEOUT};
use crate::slot::WorkerSlot;

/// The socket and what its requests share with the threads that may answer them.
/// Its last Arc may go in a thread-safe free, so what it holds keeps typed_data.rs's promise.
pub struct InlineIo {
    pub(super) state: Mutex<IoState>,
    pub(super) deadline: Deadline,
    pub(super) origin: Origin,
    /// The worker it was placed on, which counts it while it holds its seat.
    pub(super) slot: Arc<WorkerSlot>,
}

/// When the connection is cut off if it still waits on its client (InlineIo::expire), on the
/// inline clock: HEADER_READ_TIMEOUT after its accept or its last answer, BODY_READ_TIMEOUT after
/// a body's last bytes; with request_timeout set, an answer must begin by it or the request gets
/// a 504. Set under the state's lock, but for a body's bytes, which only its job reads; the
/// ticker reads it off the worker's list (Listed) with no lock.
#[derive(Clone)]
pub(super) struct Deadline(Arc<AtomicU64>);

impl Deadline {
    pub(super) fn new(at: u64) -> Deadline {
        Deadline(Arc::new(AtomicU64::new(at)))
    }

    pub(super) fn due(&self, now: u64) -> bool {
        self.0.load(Ordering::Relaxed) <= now
    }

    pub(super) fn set(&self, at: u64) {
        self.0.store(at, Ordering::Relaxed);
    }

    pub(super) fn due_in(&self, server: &ServerInner, timeout: Duration) {
        self.set(super::now(server) + micros(timeout));
    }

    /// Of a connection waiting for its next head: what is left of HEADER_READ_TIMEOUT.
    fn left(&self, server: &ServerInner) -> Duration {
        let at = self.0.load(Ordering::Relaxed);
        Duration::from_micros(at.saturating_sub(super::now(server)))
    }
}

/// What the socket did not take yet: `buf` from `pos` on.
#[derive(Default)]
pub(super) struct WriteBuf {
    pub(super) buf: Vec<u8>,
    pub(super) pos: usize,
}

/// A larger write buffer is let go once empty, so one big response does not keep its memory.
const WBUF_KEEP: usize = 64 * 1024;

impl WriteBuf {
    pub(super) fn sent(&self) -> bool {
        self.pos == self.buf.len()
    }

    pub(super) fn clear(&mut self) {
        self.buf.clear();
        self.pos = 0;
    }

    /// `parts` after what is buffered: as much as `stream` takes now, the rest kept for `flush`.
    fn write(&mut self, stream: &StdConn, parts: &[&[u8]]) -> std::io::Result<()> {
        let mut written = 0;
        if self.sent() {
            self.clear();
            let slices: smallvec::SmallVec<[IoSlice; 3]> =
                parts.iter().map(|part| IoSlice::new(part)).collect();
            written = loop {
                match (&stream.socket).write_vectored(&slices) {
                    Ok(n) => break n,
                    Err(e) if e.kind() == ErrorKind::Interrupted => {}
                    Err(e) if e.kind() == ErrorKind::WouldBlock => break 0,
                    Err(e) => return Err(e),
                }
            };
        }
        for part in parts {
            let skip = written.min(part.len());
            written -= skip;
            self.buf.extend_from_slice(&part[skip..]);
        }
        Ok(())
    }

    /// True once nothing is buffered.
    fn flush(&mut self, stream: &StdConn) -> std::io::Result<bool> {
        while self.pos < self.buf.len() {
            match stream.send(&self.buf[self.pos..]) {
                Ok(0) => return Err(ErrorKind::WriteZero.into()),
                Ok(n) => self.pos += n,
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) if e.kind() == ErrorKind::WouldBlock => return Ok(false),
                Err(e) => return Err(e),
            }
        }
        if self.buf.capacity() > WBUF_KEEP {
            self.buf = Vec::new();
        }
        self.clear();
        Ok(true)
    }
}

/// On its worker's list, with its deadline, which the ticker reads without the connection's lock.
pub struct Listed {
    pub(super) deadline: Deadline,
    pub(super) io: Weak<InlineIo>,
}

impl Listed {
    pub(crate) fn io(&self) -> Option<Arc<InlineIo>> {
        self.io.upgrade()
    }

    pub(crate) fn is(&self, io: *const InlineIo) -> bool {
        std::ptr::eq(self.io.as_ptr(), io)
    }
}

/// What hyper needs to serve the connection from where it is: handed back by its job, or taken
/// over idle after its job ended or its worker crashed.
pub(crate) struct Origin {
    pub(crate) server: Arc<ServerInner>,
    /// Hand-backs go to the I/O runtime that accepted the connection.
    pub(crate) home: tokio::runtime::Handle,
    pub(crate) peer: Peer,
}

#[derive(Default)]
pub(super) struct IoState {
    /// None once closed or handed back; kept past its job's let-go while a write waits (`lent`).
    pub(super) stream: Option<StdConn>,
    /// Its count on its worker and its max_connections permit, given back once by whichever comes
    /// first: its job letting it go (the permit to hyper with the socket) or abandon.
    pub(super) seat: Option<Seat>,
    pub(super) held: Option<Held>,
    pub(super) out: WriteBuf,
    /// The connection ends once what it wrote is out.
    pub(super) closing: bool,
    /// What its job waits on the socket for, as a drain or a quarantine sees it.
    pub(super) waits: Waits,
    /// The last answer is whole and its job has not looked since (nor stepped aside): counted in
    /// its worker's InlineSeats::answered meanwhile.
    pub(super) answered: bool,
    /// Bytes read past the returned request wait unparsed: a next request may sit whole in the
    /// buffer, which no deadline cuts off (hyper parses a head it holds).
    pub(super) unparsed: bool,
    /// Writes waiting on the socket (Wrote::Wait) not back yet: the descriptor stays open
    /// meanwhile, so no wait meets a closed or reused one.
    pub(super) lent: usize,
    /// Its job let it go while a write still waited: the last write back closes it.
    pub(super) released: bool,
    /// Its job looked at it: until then it may go to hyper untouched (inline::unplace), and no
    /// crash counts it as cut off.
    pub(super) taken: bool,
    /// The request whose streamed body hyper would have dropped (a refused head, a length past
    /// its end), by its Responder's address (0: none): its next write finds the client gone,
    /// counted as hyper counts it.
    pub(super) dropped: usize,
    /// The request being served, until its answer is written.
    pub(super) current: Option<Current>,
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub(super) enum Waits {
    /// Nothing below: its job looks, or waits on a body or a write.
    #[default]
    Other,
    /// The next request, nothing of it read (or the step aside just before: yield_turn).
    Idle,
    /// The rest of a head, on a connection that served a request.
    MidHead,
}

/// A connection's count on its worker (InlineSeats::conns), given back on drop.
pub(crate) type Seat = Counted<WorkerSlot>;

pub(crate) fn take_seat(slot: &Arc<WorkerSlot>) -> Seat {
    Counted::on(slot, |slot| &slot.inline.conns)
}

#[derive(Clone, Copy)]
pub(crate) enum Shut {
    /// Counts no close.
    Drain,
    /// Counted as cut off.
    Quarantine,
}

impl InlineIo {
    /// The ticker's look once the deadline passed (inline::tick): a connection still waiting on
    /// its client is shut down, which wakes its job to close it; its client reads the end. A
    /// request whose answer has not begun past request_timeout gets a 504 and the connection ends
    /// after it (time_out). One serving a request, or writing out on a worker that stays, waits
    /// on no client: it is looked at again a header timeout on. On a worker that leaves, a client
    /// that took nothing more of an answer for a header timeout is cut off, so the worker can go.
    /// While its worker lives, one whose job holds unparsed bytes or whose client's bytes wait
    /// unread is spared: its thread was only held up.
    pub(super) fn expire(&self, now: u64) {
        let slot = &*self.slot;
        let mut state = self.state.lock();
        if !self.deadline.due(now) {
            return;
        }
        if self.times_out(&state) {
            self.time_out(&mut state);
            return;
        }
        let alive = !slot.life.quarantined.load(Ordering::Relaxed);
        if state.current.is_some() || !state.out.sent() {
            let leaving = !alive || slot.life.retired.load(Ordering::Relaxed);
            if leaving && state.current.is_none() && !(alive && state.writable()) {
                state.ends(slot);
                state.shutdown();
            } else {
                self.deadline.set(now + micros(HEADER_READ_TIMEOUT));
            }
            return;
        }
        if alive && (state.unparsed || state.unread()) {
            return;
        }
        state.ends(slot);
        state.shutdown();
    }

    /// A drain's or a quarantine's look: the socket is shut down, which wakes the job to close
    /// it, if its job waits for a request of which nothing was read (or steps aside before it),
    /// or, for a drain, for the rest of a kept-alive head, as hyper closes such a one. A drain
    /// spares one whose client's bytes wait unread, as hyper reads what came before it heeds the
    /// drain. A job about to wait sees either itself (InlineConn::wait).
    pub(super) fn shut_if_idle(&self, shut: Shut) {
        let mut state = self.state.lock();
        match (shut, state.waits) {
            (_, Waits::Other) | (Shut::Quarantine, Waits::MidHead) => return,
            (Shut::Drain, _) if state.unread() => return,
            (Shut::Drain, _) => {}
            (Shut::Quarantine, _) => state.ends(&self.slot),
        }
        state.shutdown();
    }

    /// Past the shutdown deadline: the socket is shut down unless a request is being served,
    /// cutting one still reading a request or writing to a slow reader.
    pub(super) fn shut_unless_serving(&self) {
        let mut state = self.state.lock();
        if state.current.is_none() {
            state.ends(&self.slot);
            state.shutdown();
        }
    }

    /// Its worker is given up on for `why` (a crash, or the shutdown past its deadline): counted
    /// as cut off, unless its job never looked at it or, on a crash, it waits idle; abandon
    /// counts those once they do not go to hyper.
    pub(super) fn giving_up(&self, why: Cut) {
        let mut state = self.state.lock();
        if state.taken && !(why == Cut::Crash && state.idle()) {
            state.ends(&self.slot);
        }
    }

    /// A write's wait (Wrote::Wait) is back; with none out, a connection let go meanwhile closes.
    pub(crate) fn waited(&self) {
        let mut state = self.state.lock();
        state.lent = state.lent.saturating_sub(1);
        if state.lent == 0 && state.released {
            if let Some(stream) = state.stream.take() {
                close(stream);
            }
        }
    }

    /// Its worker is given up on (it crashed, or the shutdown passed its deadline): no job looks
    /// at it again. On a crash an idle one goes to hyper. Else an unanswered request gets its 500
    /// (or its stream is cut, counted for `why`), what the socket takes now of an answer goes
    /// out, the socket is shut down so its client reads the end, and its worker counts it no
    /// more, once. Only its job, or the last write waiting at its end, closes the descriptor. Out
    /// come its seat and permit.
    fn abandon(&self, why: Cut) -> (Option<Seat>, Option<Held>) {
        if why == Cut::Crash {
            if let Some(seat) = self.to_hyper_if_idle() {
                return (Some(seat), None);
            }
        }
        let owed = {
            let mut state = self.state.lock();
            state.ends(&self.slot);
            state
                .current
                .as_ref()
                .map(|current| current.responder.clone())
        };
        if let Some(responder) = owed {
            self.give_up(&responder, why);
        }
        let mut state = self.state.lock();
        let _ = state.flush();
        state.shutdown();
        state.looked(&self.slot);
        (state.seat.take(), state.held.take())
    }

    /// Its worker crashed or its job ended with it idle: hyper takes it over as it is, on its
    /// accepting runtime, handed back, so a request already sent is served and a
    /// keep-alive client keeps it, with what is left of its header time. A job that looks again
    /// finds it over. Out comes its seat; None, changing nothing, unless idle.
    pub(super) fn to_hyper_if_idle(&self) -> Option<Seat> {
        let (stream, held, seat) = {
            let mut state = self.state.lock();
            if !state.idle() {
                return None;
            }
            state.looked(&self.slot);
            state.waits = Waits::Other;
            state.closing = true;
            (state.stream.take()?, state.held.take()?, state.seat.take())
        };
        self.to_hyper(stream, held, Bytes::new(), true);
        seat
    }

    /// Hyper serves the connection from here on, on its accepting runtime, reading `prefix` (what
    /// was read here) first; counted as handed back, or else as declined at accept. With too little read to
    /// sniff (nothing, or part of the h2 preface), its client has what is left of its header
    /// time.
    pub(super) fn to_hyper(&self, stream: StdConn, held: Held, prefix: Bytes, handed_back: bool) {
        if let Some(inline) = &self.origin.server.inline {
            let count = match handed_back {
                true => &inline.handbacks,
                false => &inline.declined,
            };
            count.fetch_add(1, Ordering::Relaxed);
        }
        crate::server::serve_handed_back(
            &self.origin,
            stream,
            prefix,
            held,
            self.deadline.left(&self.origin.server),
        );
    }
}

impl IoState {
    /// Kino ends the connection (a deadline, a crash, the shutdown, a quarantine, a write nobody
    /// waits on), counted once: not if it was ending already (an answer that closes it, a 504, an
    /// earlier cut) or is its worker's no more.
    pub(super) fn ends(&mut self, slot: &WorkerSlot) {
        if !self.closing && self.seat.is_some() {
            slot.inline.closed.fetch_add(1, Ordering::Relaxed);
            self.closing = true;
        }
    }

    /// Hyper could take it over as it is: open and its worker's, its job waiting for a next
    /// request with nothing read, nothing owed, nothing left to write and no write waiting.
    fn idle(&self) -> bool {
        self.waits == Waits::Idle
            && !self.closing
            && self.seat.is_some()
            && self.stream.is_some()
            && self.held.is_some()
            && self.current.is_none()
            && !self.unparsed
            && self.out.sent()
            && self.lent == 0
    }

    pub(super) fn write(&mut self, parts: &[&[u8]]) -> std::io::Result<()> {
        let stream = self.stream.as_ref().ok_or(ErrorKind::NotConnected)?;
        self.out.write(stream, parts)
    }

    pub(super) fn flush(&mut self) -> std::io::Result<bool> {
        let stream = self.stream.as_ref().ok_or(ErrorKind::NotConnected)?;
        self.out.flush(stream)
    }

    /// Its job looks for the next request, steps aside before it, or lets go: no longer answered.
    pub(super) fn looked(&mut self, slot: &WorkerSlot) {
        if self.answered {
            self.answered = false;
            slot.inline.answered.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Cut the connection short, from any thread: its job wakes and ends.
    pub(super) fn shutdown(&mut self) {
        if let Some(stream) = &self.stream {
            let _ = stream.socket.shutdown(Shutdown::Both);
        }
        self.out.clear();
        self.closing = true;
    }

    /// The socket takes more now: its client read some since a write found it full.
    fn writable(&self) -> bool {
        self.stream.as_ref().is_some_and(|stream| {
            poll_for(stream.as_raw_fd(), libc::POLLOUT, 0)
                .is_some_and(|revents| revents & (libc::POLLERR | libc::POLLHUP) == 0)
        })
    }

    /// Its client's bytes wait unread in the socket.
    pub(super) fn unread(&self) -> bool {
        let mut byte = [0u8];
        self.stream
            .as_ref()
            .is_some_and(|stream| matches!(stream.peek(&mut byte), Ok(n) if n > 0))
    }
}

/// What a closing connection reads off its socket at most (close): a request or two its client
/// sent behind the last answer. Past it, the close resets the connection.
const DISCARD_MAX: usize = 64 * 1024;

/// The close of a connection's socket by its job or by the last write waiting on it: the FIN goes
/// out behind what the socket still holds of an answer, and what the client sent that nothing will
/// read is read off first, up to DISCARD_MAX. A close with such bytes unread resets the connection
/// instead (RFC 1122, 4.2.2.13), dropping what the socket held of the answer; a client may not
/// even take that reset, and waits on an end that never comes.
pub(super) fn close(stream: StdConn) {
    let _ = stream.socket.shutdown(Shutdown::Write);
    let mut sink = [0u8; 8 * 1024];
    let mut left = DISCARD_MAX;
    while left > 0 {
        match (&stream.socket).read(&mut sink) {
            Ok(0) => break,
            Ok(n) => left = left.saturating_sub(n),
            Err(e) if e.kind() == ErrorKind::Interrupted => {}
            Err(_) => break,
        }
    }
}

pub(super) fn forget(slot: &WorkerSlot, io: &Arc<InlineIo>) {
    let io = Arc::as_ptr(io);
    slot.inline.ios.lock().retain(|listed| !listed.is(io));
}

/// `slot`'s worker is given up on: each connection is answered or shut down and counted there no
/// more, whether or not its job looks again.
pub(crate) fn abandon(slot: &WorkerSlot, why: Cut) {
    for io in super::ios(slot) {
        let (seat, held) = io.abandon(why);
        if seat.is_some() {
            forget(slot, &io);
        }
        drop((seat, held));
    }
}

pub(super) fn micros(span: Duration) -> u64 {
    u64::try_from(span.as_micros()).unwrap_or(u64::MAX)
}

/// A blocking write's wait (write_chunk's Wait): up to `timeout` for `fd` to take more or fail (a
/// closed descriptor reads ready).
pub fn await_writable(fd: RawFd, timeout: Duration) {
    let ms = libc::c_int::try_from(timeout.as_millis()).unwrap_or(libc::c_int::MAX);
    poll_for(fd, libc::POLLOUT, ms);
}

/// poll(2) for `events` on `fd` up to `ms` (0: no wait): the events seen, None on timeout or
/// failure.
pub(super) fn poll_for(fd: RawFd, events: libc::c_short, ms: libc::c_int) -> Option<libc::c_short> {
    let mut ready = libc::pollfd {
        fd,
        events,
        revents: 0,
    };
    // SAFETY: `ready` is one pollfd borrowed for the call alone, and the count says one; poll(2)
    // reports a bad `fd` in revents rather than touch it.
    let polled = unsafe { libc::poll(&mut ready, 1, ms) };
    (polled == 1).then_some(ready.revents)
}
