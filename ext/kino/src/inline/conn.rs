//! An inline connection's reader: its socket, its read buffer and its looks for the next request.

use std::io::{ErrorKind, Read};
use std::os::fd::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use parking_lot::Mutex;

use super::answer::Current;
use super::io::{close, forget, micros, Deadline, InlineIo, IoState, Listed, Origin, Seat, Waits};
use super::parse::{parse_head, Head, Parsed, HEAD_MAX};
use super::spin::{Backoff, Release, Run, Spin};
use super::INLINE_BODY_MAX;
use crate::access_log::Logged;
use crate::registry::{BoxedCtx, ServerInner};
use crate::request::RequestCtx;
use crate::response::{Cut, Responder};
use crate::server::{Held, InFlight, Peer, StdConn, BODY_READ_TIMEOUT, HEADER_READ_TIMEOUT};
use crate::slot::WorkerSlot;

/// A plain TCP or unix connection served inline, owned by its worker's job.
pub struct InlineConn {
    pub(super) io: Arc<InlineIo>,
    /// Read and not consumed yet; HEAD_MAX long except while a longer body is read (read_body).
    buf: Box<[u8]>,
    len: usize,
    pending: Option<Head>,
    /// IoState::unparsed as its job last set it: a look locks to clear it only if set.
    unparsed: bool,
    /// It ran a request, so its next head is not its first (the drain ends it mid-head).
    served: bool,
    /// A request went to the app since its job last looked: that look waits its turn (wait_turn).
    after_request: bool,
    /// Likewise: that look decides whether the job steps aside first (yield_turn).
    yield_due: bool,
    /// It may spin for its next request (spin_for_next): as it opens and after each request.
    spin_due: bool,
    /// The spin's budget (InlineServer::spin_budget), read once at accept.
    budget: Duration,
    backoff: Backoff,
    /// Its last look ended in a wait on the app's scheduler, or it has not looked yet: the next
    /// look starts a new run (spin::Run).
    waited: bool,
    run: Run,
    server: Arc<ServerInner>,
    /// Counts it (InlineSeats::conns) while it holds its seat, until it closes or goes to hyper.
    slot: Arc<WorkerSlot>,
    /// Tests: run once, as a request is read whole and before it is
    /// admitted or handed back, as another thread may cut the connection
    /// short then.
    #[cfg(test)]
    pub(crate) on_whole: Option<Box<dyn FnOnce() + Send>>,
    /// Tests: run once, as a look that read all there was is about to
    /// wait, as its client's bytes and the drain may come then.
    #[cfg(test)]
    pub(crate) on_wait: Option<Box<dyn FnOnce() + Send>>,
}

enum Got {
    /// Bytes, or an interrupted read: look again.
    More,
    Nothing,
    /// Its client closed its side, or the socket failed or is gone.
    End,
}

pub enum Next {
    Request(BoxedCtx),
    Readable,
    Writable,
    /// Closed, or handed back to hyper.
    Closed,
}

impl InlineConn {
    /// `stream` (nonblocking), accepted on `home`; it holds `held` while open, here or on hyper.
    pub(crate) fn new(
        server: &Arc<ServerInner>,
        seat: Seat,
        stream: StdConn,
        held: Held,
        peer: Peer,
        home: tokio::runtime::Handle,
    ) -> Box<InlineConn> {
        let slot = seat.owner().clone();
        let io = Arc::new(InlineIo {
            deadline: Deadline::new(super::now(server) + micros(HEADER_READ_TIMEOUT)),
            origin: Origin {
                server: server.clone(),
                home,
                peer,
            },
            slot: slot.clone(),
            state: Mutex::new(IoState {
                stream: Some(stream),
                seat: Some(seat),
                held: Some(held),
                ..IoState::default()
            }),
        });
        Box::new(InlineConn {
            io,
            buf: vec![0; HEAD_MAX].into_boxed_slice(),
            len: 0,
            pending: None,
            unparsed: false,
            served: false,
            after_request: false,
            yield_due: false,
            spin_due: true,
            budget: server
                .inline
                .as_ref()
                .map_or(super::spin::BUDGET, |inline| inline.spin_budget),
            backoff: Backoff::default(),
            waited: true,
            run: Run::default(),
            server: server.clone(),
            slot,
            #[cfg(test)]
            on_whole: None,
            #[cfg(test)]
            on_wait: None,
        })
    }

    /// Listed under the lane lock as send_placed accepts it, so the drain, the deadlines and a
    /// crash find it before its worker takes it; one a crash missed goes to hyper whole.
    pub(crate) fn list(&self) {
        self.slot.inline.ios.lock().push(Listed {
            deadline: self.io.deadline.clone(),
            io: Arc::downgrade(&self.io),
        });
    }

    pub(crate) fn server(&self) -> &Arc<ServerInner> {
        &self.server
    }

    pub(crate) fn slot(&self) -> &Arc<WorkerSlot> {
        &self.slot
    }

    pub fn fd(&self) -> Option<RawFd> {
        self.io.state.lock().stream.as_ref().map(AsRawFd::as_raw_fd)
    }

    /// Its worker counts it no more, and its socket closes (io::close), if still here.
    pub(crate) fn let_go(&mut self) {
        self.release(false);
    }

    /// let_go for hyper: out come its socket and what it holds, if still here. None, the socket
    /// closed, if it was cut short (past its deadline or the shutdown's), as hyper must not serve
    /// what was cut.
    pub(crate) fn let_go_to_hyper(&mut self) -> Option<(StdConn, Held)> {
        self.release(true)
    }

    fn release(&mut self, to_hyper: bool) -> Option<(StdConn, Held)> {
        let (stream, seat, held, cut) = {
            let mut state = self.io.state.lock();
            state.looked(&self.slot);
            // Its job ends a connection as it looks (next): a close here is a shutdown's.
            let cut = to_hyper && state.closing;
            let stream = if state.lent > 0 {
                // A write still waits on it: shut down to end that wait; the last back closes it.
                if to_hyper {
                    state.ends(&self.slot);
                }
                state.shutdown();
                state.released = true;
                None
            } else {
                state.stream.take()
            };
            (stream, state.seat.take(), state.held.take(), cut)
        };
        if seat.is_some() {
            forget(&self.slot, &self.io);
        }
        drop(seat);
        match (stream, held) {
            (Some(stream), Some(held)) if to_hyper && !cut => Some((stream, held)),
            (stream, _) => {
                if let Some(stream) = stream {
                    close(stream);
                }
                None
            }
        }
    }

    /// One its worker never took, to hyper on its accepting runtime with nothing read, counted
    /// as handed back (or else as declined): its client keeps what is left of its header time.
    pub(crate) fn send_to_hyper(&mut self, handed_back: bool) {
        if let Some((stream, held)) = self.let_go_to_hyper() {
            self.io.to_hyper(stream, held, Bytes::new(), handed_back);
        }
    }

    /// At its job's first look after a request: whether the job steps aside behind the fibers
    /// already ready on the app's scheduler, as async-http's server yields after each response.
    /// Only when the answer went out whole, nothing of the next request was read and another
    /// inline connection shares the worker. The step counts as the look, for placement
    /// (IoState::looked) and as an idle wait (Waits::Idle), so a crash, a quarantine or the drain
    /// treats the connection as idle meanwhile.
    pub fn yield_turn(&mut self) -> bool {
        if !std::mem::take(&mut self.yield_due) || self.len > 0 {
            return false;
        }
        if self.slot.inline.conns.load(Ordering::Relaxed) <= 1 {
            return false;
        }
        let mut state = self.io.state.lock();
        let due = state.out.sent() && state.current.is_none() && !state.closing;
        if due {
            state.looked(&self.slot);
            state.waits = Waits::Idle;
            self.waited = true;
        }
        due
    }

    /// The next request once the last answer is out, its body whole. One outside the subset, or
    /// with no `room` on its worker, goes to hyper with the connection, as does the connection
    /// once its worker retired. Alone on its worker, the job may spin for it first.
    pub fn next(&mut self, room: bool) -> Next {
        self.next_with(room, &mut super::spin::released)
    }

    /// next, with the spin's release passed in: tests spin in place.
    pub(crate) fn next_with(&mut self, room: bool, release: &mut Release) -> Next {
        if std::mem::take(&mut self.waited) {
            self.run.waited(&self.server, &self.slot);
        }
        let next = self.look_for(room, release);
        self.waited = matches!(next, Next::Readable | Next::Writable);
        next
    }

    fn look_for(&mut self, room: bool, release: &mut Release) -> Next {
        self.give_up_owed();
        {
            let mut state = self.io.state.lock();
            state.waits = Waits::Other;
            state.taken = true;
            state.looked(&self.slot);
            let unsent = !state.out.sent();
            let from = state.out.pos;
            match state.flush() {
                Ok(true) => {}
                Ok(false) => {
                    // Its client took some: the answer's deadline moves on.
                    if state.out.pos > from {
                        self.io.deadline.due_in(&self.server, HEADER_READ_TIMEOUT);
                    }
                    return Next::Writable;
                }
                Err(_) => {
                    drop(state);
                    return self.end();
                }
            }
            if state.closing {
                drop(state);
                return self.end();
            }
            // The last answer just went out whole: as hyper decides once it flushes, the
            // connection ends now if the drain began or its worker was quarantined.
            if unsent {
                let draining = self.server.is_draining();
                if draining || self.slot.life.quarantined() {
                    if !draining {
                        state.ends(&self.slot);
                    }
                    drop(state);
                    return self.end();
                }
                self.io.deadline.due_in(&self.server, HEADER_READ_TIMEOUT);
            }
        }
        // Once the drain began, a request that came is still served, then the connection closes;
        // a kept-alive one mid-head ends, as hyper's does; a fresh one reads its first head on.
        let draining = self.server.is_draining();
        if !draining && self.slot.life.seems_retired() {
            return self.hand_back();
        }
        if let Some(head) = self.pending.take() {
            return self.read_body(head, room, false);
        }
        if std::mem::take(&mut self.after_request) && self.len == 0 {
            if let Some(next) = self.wait_turn(release) {
                return next;
            }
        }
        loop {
            match parse_head(&self.buf[..self.len]) {
                Parsed::Head(head) if head.content_length > self.body_max() => {
                    return self.hand_back()
                }
                // Ready to run: no room is why it goes.
                Parsed::Head(head) if head.content_length == 0 && !room => return self.hand_back(),
                Parsed::Head(head) => return self.read_body(head, room, true),
                Parsed::Refused => return self.hand_back(),
                Parsed::Partial if self.len == self.buf.len() && draining && self.served => {
                    return self.end()
                }
                Parsed::Partial if self.len == self.buf.len() => return self.hand_back(),
                Parsed::Partial => match self.read_more() {
                    Got::More => self.backoff.observed(self.budget),
                    Got::Nothing => {
                        if let Some(next) = self.wait(release) {
                            return next;
                        }
                    }
                    Got::End => return self.end(),
                },
            }
        }
    }

    /// INLINE_BODY_MAX, or max_body_size (0: none) if lower, past which hyper answers 413.
    fn body_max(&self) -> u64 {
        match self.server.config.max_body_size as u64 {
            0 => INLINE_BODY_MAX,
            cap => cap.min(INLINE_BODY_MAX),
        }
    }

    /// `head`'s request once its body is whole, waiting on the socket up to BODY_READ_TIMEOUT
    /// from the head (`fresh`) or the last bytes. A body past the buffer grows it, doubling, for
    /// this request alone; `room` counts only once the app is to run.
    fn read_body(&mut self, head: Head, room: bool, fresh: bool) -> Next {
        let end = head.len + head.content_length as usize;
        let mut came = fresh;
        while self.len < end {
            if self.len == self.buf.len() {
                self.grow((self.len * 2).min(end));
            }
            match self.read_more() {
                Got::More => came = true,
                Got::Nothing => {
                    if came {
                        self.io.deadline.due_in(&self.server, BODY_READ_TIMEOUT);
                    }
                    self.pending = Some(head);
                    self.parsed();
                    return Next::Readable;
                }
                Got::End => return self.end(),
            }
        }
        if !room {
            return self.hand_back();
        }
        self.request(head, end)
    }

    /// Nothing to parse until the socket reads ready. Idle, a drain or a quarantine may shut the
    /// connection; mid-head on a kept-alive one, a drain may. Should the drain have begun since
    /// the look, it ends now, unless its client's bytes came meanwhile (or the spin saw them):
    /// the look reads those first (None), as hyper reads what came before it heeds the drain.
    fn wait(&mut self, release: &mut Release) -> Option<Next> {
        #[cfg(test)]
        if let Some(meanwhile) = self.on_wait.take() {
            meanwhile();
        }
        if self.len > 0 && !self.served {
            self.parsed();
            return Some(Next::Readable);
        }
        if self.len == 0 && self.spin_for_next(release) {
            return self.retired_meanwhile();
        }
        let mut state = self.io.state.lock();
        if self.server.is_draining() {
            if state.unread() {
                return None;
            }
            drop(state);
            return Some(self.end());
        }
        if self.len == 0 {
            if self.slot.life.quarantined() {
                state.ends(&self.slot);
                drop(state);
                return Some(self.end());
            }
            state.waits = Waits::Idle;
            // Nothing is left unparsed: a request() consumed what it left.
            return Some(Next::Readable);
        }
        state.waits = Waits::MidHead;
        state.unparsed = false;
        self.unparsed = false;
        Some(Next::Readable)
    }

    /// The first look after a request, nothing of the next read: the job waits on the socket
    /// before it reads, so a request that came meanwhile runs behind the worker's connections
    /// whose requests came first; a pipelined one runs at once. None, to read first, once the
    /// drain began or the worker was quarantined (read under the lock their passes take), or once
    /// the spin saw the next request, as then no other connection shares the worker.
    fn wait_turn(&mut self, release: &mut Release) -> Option<Next> {
        if self.spin_for_next(release) {
            return self.retired_meanwhile();
        }
        let mut state = self.io.state.lock();
        if self.server.is_draining() || self.slot.life.quarantined() {
            return None;
        }
        state.waits = Waits::Idle;
        Some(Next::Readable)
    }

    /// Whether its job reads the next request now, having spun for it up to the budget instead of
    /// parking on the socket: once per request read and as it opens, nothing owed, within the
    /// server-wide gate and the back-off. A stop by the VM's unblock function does not count, and
    /// a catch past the job's run (spin::Run) is left to the job's wait, so the app's scheduler
    /// gets its pass. The descriptor stays the job's: nothing takes it to hyper while the job
    /// waits on nothing.
    fn spin_for_next(&mut self, release: &mut Release) -> bool {
        if !std::mem::take(&mut self.spin_due) || !super::spin::may_spin(&self.server, &self.slot) {
            return false;
        }
        let Some(inline) = self.server.inline.as_deref() else {
            return false;
        };
        let fd = {
            let state = self.io.state.lock();
            match &state.stream {
                Some(stream) if state.current.is_none() && state.out.sent() && !state.closing => {
                    stream.as_raw_fd()
                }
                _ => return false,
            }
        };
        // No room in the server-wide count leaves the back-off as it was: a due probe waits for a
        // chance that can spin.
        let Some(spinning) = inline.spinners.join(inline.spin_in_flight) else {
            return false;
        };
        if !self.backoff.chance() {
            return false;
        }
        let budget = self.budget;
        let (server, slot) = (&self.server, &self.slot);
        let woken = AtomicBool::new(false);
        let spun = release(&woken, &mut || {
            super::spin::poll(fd, budget, &woken, server, slot)
        });
        drop(spinning);
        let Some(spun) = spun else {
            return false;
        };
        let spun = if woken.load(Ordering::Relaxed) {
            Spin::Stopped
        } else {
            spun
        };
        slot.inline.spins.fetch_add(1, Ordering::Relaxed);
        self.backoff.record(spun) && self.run.reads(inline.spin_run)
    }

    /// After a spin's catch: None for the look to read it, unless the worker retired meanwhile,
    /// when the connection goes to hyper as at the look's start.
    fn retired_meanwhile(&mut self) -> Option<Next> {
        let draining = self.server.is_draining();
        (!draining && self.slot.life.seems_retired()).then(|| self.hand_back())
    }

    /// Parsed short of a request: no whole one waits in the buffer (IoState::unparsed).
    fn parsed(&mut self) {
        if self.unparsed {
            self.io.state.lock().unparsed = false;
            self.unparsed = false;
        }
    }

    /// The request being served got no answer by the job's next look: a 500, then it ends.
    fn give_up_owed(&self) {
        let owed = self
            .io
            .state
            .lock()
            .current
            .as_ref()
            .map(|c| c.responder.clone());
        if let Some(responder) = owed {
            self.io.give_up(&responder, Cut::Crash);
        }
    }

    fn grow(&mut self, to: usize) {
        let mut buf = std::mem::take(&mut self.buf).into_vec();
        buf.reserve_exact(to - buf.len());
        buf.resize(to, 0);
        self.buf = buf.into_boxed_slice();
    }

    /// The first `end` bytes were a request's: the rest moves to a usual-size buffer's front.
    fn consume(&mut self, end: usize) {
        self.buf.copy_within(end..self.len, 0);
        self.len -= end;
        if self.buf.len() > HEAD_MAX {
            let mut buf = std::mem::take(&mut self.buf).into_vec();
            buf.truncate(HEAD_MAX.max(self.len));
            self.buf = buf.into_boxed_slice();
        }
    }

    fn read_more(&mut self) -> Got {
        let state = self.io.state.lock();
        let Some(stream) = state.stream.as_ref() else {
            return Got::End;
        };
        match (&stream.socket).read(&mut self.buf[self.len..]) {
            Ok(0) => Got::End,
            Ok(n) => {
                self.len += n;
                Got::More
            }
            Err(e) if e.kind() == ErrorKind::WouldBlock => Got::Nothing,
            Err(e) if e.kind() == ErrorKind::Interrupted => Got::More,
            Err(_) => Got::End,
        }
    }

    fn end(&mut self) -> Next {
        self.let_go();
        Next::Closed
    }

    /// Hyper serves the connection from here on, reading what was read here first.
    fn hand_back(&mut self) -> Next {
        #[cfg(test)]
        if let Some(cut) = self.on_whole.take() {
            cut();
        }
        let Some((stream, held)) = self.let_go_to_hyper() else {
            return Next::Closed;
        };
        let prefix = Bytes::copy_from_slice(&self.buf[..self.len]);
        self.len = 0;
        self.pending = None;
        self.io.to_hyper(stream, held, prefix, true);
        Next::Closed
    }

    /// The request read whole, the first `end` bytes of the buffer.
    fn request(&mut self, head: Head, end: usize) -> Next {
        #[cfg(test)]
        if let Some(cut) = self.on_whole.take() {
            cut();
        }
        // The header values and the body share one copy.
        let bytes = Bytes::copy_from_slice(&self.buf[..end]);
        let (uri, headers) = match head.parts(&bytes) {
            Some(parts) => parts,
            None => return self.hand_back(),
        };
        self.consume(end);
        // All of it, then the end: the app's reads never wait.
        let body_rx = (end > head.len).then(|| {
            let (body_tx, body_rx) = flume::bounded(1);
            let _ = body_tx.send(bytes.slice(head.len..));
            body_rx
        });

        let mut state = self.io.state.lock();
        // Cut short since its job looked: it ends unanswered and the app never runs it.
        if state.closing {
            drop(state);
            return self.end();
        }
        self.served = true;
        self.after_request = true;
        self.yield_due = true;
        self.spin_due = true;
        let server = &self.server;
        // Under the lock, so its arrival line comes before any thread's completion line.
        let logged = server.access_log.as_ref().map(|log| {
            let from = self.io.origin.peer.remote.ip();
            Box::new(Logged::arrival(log, &head.method, &uri, from))
        });
        state.unparsed = self.len > 0;
        self.unparsed = state.unparsed;
        // A 504 unless its answer begins by then, on the clock read now: hyper's never comes
        // early.
        if server.config.request_timeout_ms > 0 {
            let timeout = server.config.request_timeout_ms.saturating_mul(1000);
            self.io
                .deadline
                .set(super::real_now(server).saturating_add(timeout));
        }
        let responder = Arc::new(Responder::inline(Arc::downgrade(&self.io)));
        // It offers no hijack: nothing takes it out of the count early.
        let in_flight = InFlight::enter(server.clone(), None);
        self.slot.inline.requests.fetch_add(1, Ordering::Relaxed);
        state.dropped = 0;
        state.current = Some(Current {
            responder: responder.clone(),
            head_only: head.method == http::Method::HEAD,
            close: head.close,
            _in_flight: in_flight,
            stream: None,
            logged,
        });
        drop(state);
        // Never https inline; unix connections get hyper's addresses
        // (server::UNIX_PEER).
        let mut ctx = RequestCtx::queued(
            server,
            head.method,
            uri,
            http::Version::HTTP_11,
            headers,
            self.io.origin.peer,
            responder,
        );
        ctx.body_rx = body_rx;
        ctx.slot = Some(self.slot.clone());
        ctx.inline = Some(self.io.clone());
        Next::Request(Box::new(ctx))
    }
}

impl Drop for InlineConn {
    /// No Ruby API, so it may run in a free. Let go while still its worker's, the connection was
    /// cut off by whatever ended the job; one waiting idle goes to hyper instead
    /// (InlineIo::abandon).
    fn drop(&mut self) {
        if let Some(seat) = self.io.to_hyper_if_idle() {
            forget(&self.slot, &self.io);
            drop(seat);
            return;
        }
        self.io.state.lock().ends(&self.slot);
        self.give_up_owed();
        self.let_go();
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use super::*;
    use crate::inline::testing::{
        advance, answer, answer_dispatched, await_bytes, body_of, closes, configured_server,
        cut_offs, handbacks, inline_loop_server, look, loop_server_as, loopback, loopback_holding,
        narrowed, next_request, once_written, read_to_end, reads_within, send_get, server,
        with_body, Event, Wire,
    };
    use crate::inline::Wrote;
    use crate::testing::{eventually_blocking, slot};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_body_past_max_body_size_goes_to_hyper_for_its_413() {
        // As long as max_body_size, read inline; a byte past it (though
        // short enough to read inline), hyper's 413, as without inline
        // serving.
        let server = configured_server(|c| c.max_body_size = 1024);
        let (client, mut conn) = loopback(&server);
        (&client)
            .write_all(&with_body("/form", 1024, 1024))
            .unwrap();
        let ctx = next_request(&mut conn);
        assert_eq!(body_of(&ctx).len(), 1024);
        answer(&ctx, b"stored");
        drop(ctx);
        let mut wire = Wire::of(&client);
        assert_eq!(wire.response().0, "HTTP/1.1 200 OK");
        (&client).write_all(&with_body("/form", 1025, 10)).unwrap();
        await_bytes(&conn);
        assert!(matches!(look(&mut conn, true), Next::Closed), "read inline");
        assert_eq!(wire.response().0, "HTTP/1.1 413 Payload Too Large");
        assert_eq!(handbacks(&server), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_drain_closes_idle_and_half_read_connections_and_serves_with_a_close_what_was_sent() {
        let server = server();
        let (idle_client, mut idle) = loopback(&server);
        assert!(matches!(idle.next(true), Next::Readable));
        // Placed, and not looked at yet by its job: one silent, one whose
        // client's request is in.
        let (fresh_client, mut fresh) = loopback(&server);
        let (sent_client, mut sent) = loopback(&server);
        send_get(&sent_client, "/sent", false);
        await_bytes(&sent);
        let (busy_client, mut busy) = loopback(&server);
        send_get(&busy_client, "/busy", false);
        let ctx = next_request(&mut busy);
        // Kept alive, and part of its next head read, the rest still to
        // come.
        let (part_client, mut part) = loopback(&server);
        send_get(&part_client, "/first", false);
        answer(&next_request(&mut part), b"first");
        assert_eq!(Wire::of(&part_client).response().2, b"first");
        (&part_client)
            .write_all(b"GET /part HTTP/1.1\r\nho")
            .unwrap();
        await_bytes(&part);
        while part.readable() {
            assert!(matches!(part.next(true), Next::Readable));
        }
        server.stop_accepting();

        // The idle one's client reads the end before its job looks again.
        assert_eq!(read_to_end(&idle_client), b"");
        for conn in [&mut idle, &mut fresh] {
            assert!(matches!(conn.next(true), Next::Closed));
        }
        assert_eq!(read_to_end(&fresh_client), b"");
        assert_eq!(
            read_to_end(&part_client),
            b"",
            "a half-read head was answered"
        );
        assert!(matches!(part.next(true), Next::Closed));

        let sent_ctx = next_request(&mut sent);
        for (ctx, client, conn) in [
            (ctx, &busy_client, &mut busy),
            (sent_ctx, &sent_client, &mut sent),
        ] {
            answer(&ctx, b"done");
            drop(ctx);
            let mut wire = Wire::of(client);
            let (status, headers, body) = wire.response();
            assert!(
                status == "HTTP/1.1 200 OK" && body == b"done" && closes(&headers),
                "{headers:?}"
            );
            assert!(matches!(conn.next(true), Next::Closed));
            assert_eq!(wire.answer(false), Event::Closed);
        }
        assert_eq!(slot(&server, 0).inline.conns.load(Ordering::Relaxed), 0);
        assert_eq!(cut_offs(&server), 0, "a drain's closes count as cut off");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_request_that_comes_as_the_drain_begins_is_answered_and_its_connection_ends() {
        // The job had read none of it, or part of its head, and waited
        // for the rest, which came before its next look, or during it,
        // after it read all there was and before it waited.
        let second = b"GET /2 HTTP/1.1\r\nhost: x\r\n\r\n";
        for (before, during) in [(0, false), (19, false), (0, true), (19, true)] {
            let shown = format!("{before} bytes read, the rest during the look {during}");
            let server = server();
            let _accepting = server.shutdown_tx.subscribe();
            let (client, mut conn) = loopback(&server);
            send_get(&client, "/1", false);
            answer(&next_request(&mut conn), b"one");
            let mut wire = Wire::of(&client);
            assert_eq!(wire.response().2, b"one");
            let fd = conn.fd().expect("an open connection");
            if before > 0 {
                (&client).write_all(&second[..before]).unwrap();
                assert!(reads_within(fd, 5_000), "the bytes never came");
            }
            assert!(matches!(look(&mut conn, true), Next::Readable), "{shown}");
            let rest = second[before..].to_vec();
            if during {
                let (sender, draining) = (client.try_clone().expect("a clone"), server.clone());
                conn.on_wait = Some(Box::new(move || {
                    (&sender).write_all(&rest).unwrap();
                    assert!(reads_within(fd, 5_000), "the rest never came");
                    draining.stop_accepting();
                }));
            } else {
                (&client).write_all(&rest).unwrap();
                await_bytes(&conn);
                server.stop_accepting();
            }

            let ctx = next_request(&mut conn);
            assert_eq!(ctx.uri.path(), "/2", "{shown}");
            answer(&ctx, b"two");
            drop(ctx);
            let (_, headers, body) = wire.response();
            assert!(body == b"two" && closes(&headers), "{shown}: {headers:?}");
            assert!(matches!(conn.next(true), Next::Closed), "{shown}");
            assert_eq!(wire.answer(false), Event::Closed, "{shown}");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_request_handed_to_hyper_as_the_drain_begins_is_answered() {
        /// The drain begins as `conn`'s next look reads a request whole,
        /// before that request goes anywhere.
        fn drain_on_whole(server: &Arc<ServerInner>, conn: &mut InlineConn) {
            let draining = server.clone();
            conn.on_whole = Some(Box::new(move || draining.stop_accepting()));
        }
        type Hand = fn(&Arc<ServerInner>, &std::net::TcpStream, &mut InlineConn) -> &'static str;
        // Outside the subset; read whole on a worker with no room; read a
        // byte at a time, a start of the h2 preface, then whole, as its
        // worker retires: it goes to hyper with too little for its sniff.
        let causes: [Hand; 3] = [
            |server, client, conn| {
                let chunked = "POST /chunked HTTP/1.1\r\nhost: x\r\ntransfer-encoding: chunked\r\n\r\n2\r\nhi\r\n0\r\n\r\n";
                (&*client).write_all(chunked.as_bytes()).unwrap();
                await_bytes(conn);
                drain_on_whole(server, conn);
                assert!(matches!(conn.next(true), Next::Closed));
                "/chunked"
            },
            |server, client, conn| {
                send_get(client, "/full", false);
                await_bytes(conn);
                drain_on_whole(server, conn);
                assert!(matches!(conn.next(false), Next::Closed));
                "/full"
            },
            |server, client, conn| {
                send_get(client, "/1", false);
                answer(&next_request(conn), b"one");
                assert_eq!(Wire::of(client).response().2, b"one");
                let next = b"POST /retired HTTP/1.1\r\nhost: x\r\ncontent-length: 2\r\n\r\nhi";
                (&*client).write_all(&next[..1]).unwrap();
                await_bytes(conn);
                assert!(matches!(look(conn, true), Next::Readable));
                (&*client).write_all(&next[1..]).unwrap();
                await_bytes(conn);
                let slot = slot(server, 0);
                slot.retire();
                drain_on_whole(server, conn);
                assert!(matches!(conn.next(true), Next::Closed));
                "/retired"
            },
        ];
        // Eight times over: which of the two hyper looked at first, the
        // request or the drain, is a coin's toss (fewer rounds let a hand-back
        // that drops what the worker read pass now and then). With either
        // builder: the HTTP/1 one closes a connection it read nothing of at
        // the drain, as the auto one cancels its sniff.
        for _ in 0..8 {
            for http2 in [true, false] {
                for (i, hand) in causes.iter().enumerate() {
                    let server = loop_server_as(2, 8, |t| t.with(|c| c.http2 = http2));
                    let _accepting = server.shutdown_tx.subscribe();
                    let (client, mut conn) = loopback(&server);
                    let path = hand(&server, &client, &mut conn);
                    let shown = format!("cause {i} ({path}), http2 {http2}");
                    let paths = [path];
                    let answered = answer_dispatched(&server, &paths);
                    let answered = tokio::time::timeout(Duration::from_secs(3), answered);
                    assert!(answered.await.is_ok(), "{shown}: never dispatched");
                    let (status, headers, _) = Wire::of(&client).response();
                    assert!(
                        status == "HTTP/1.1 200 OK" && closes(&headers),
                        "{shown}: {status} {headers:?}"
                    );
                    assert_eq!(read_to_end(&client), b"", "{shown}");
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_streamed_chunk_waits_while_the_socket_still_holds_the_last() {
        let server = server();
        let (client, mut conn) = narrowed(&server);
        send_get(&client, "/s", true);
        let ctx = next_request(&mut conn);
        let io = ctx.inline.clone().expect("an inline request");
        let responder = &ctx.responder;
        assert!(io.start_stream(responder, http::StatusCode::OK, &http::HeaderMap::new()));
        let chunk = vec![b'x'; 1 << 20];
        // The chunk and its frame, and no more, wait for the client.
        let bound = chunk.len() + b"100000\r\n\r\n".len();
        let buffered = |conn: &InlineConn| {
            assert!(
                conn.write_buffered() <= bound,
                "{} bytes buffered",
                conn.write_buffered()
            );
        };
        assert_eq!(io.write_chunk(responder, &chunk), Wrote::Done);
        buffered(&conn);
        match io.write_chunk(responder, &chunk) {
            Wrote::Wait(fd) => assert_eq!(Some(fd), conn.fd(), "a wait on another descriptor"),
            wrote => {
                panic!("the stream took another chunk while its client read nothing: {wrote:?}")
            }
        }
        buffered(&conn);

        // The socket takes the first chunk 4 KiB at a time, as its
        // client reads: each wait is a blocking write's, for the socket
        // to take more, however long its client takes over a read.
        let reader = std::thread::spawn(move || read_to_end(&client));
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        while let Wrote::Wait(fd) = io.write_chunk(responder, &chunk) {
            buffered(&conn);
            assert!(
                std::time::Instant::now() < deadline,
                "the client's reads never made room"
            );
            crate::inline::await_writable(fd, Duration::from_secs(1));
        }
        io.finish_stream(responder);
        assert!(matches!(once_written(&mut conn), Next::Closed));
        let read = reader.join().expect("the client");
        assert_eq!(read.iter().filter(|&&b| b == b'x').count(), 2 * chunk.len());
        assert!(read.ends_with(b"\r\n0\r\n\r\n"), "the body never ended");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_connection_handed_to_hyper_with_too_little_read_keeps_its_header_deadline() {
        let server = server();
        let ends_soon = |client: &std::net::TcpStream, within: Duration, what: &str| {
            let handed = std::time::Instant::now();
            assert_eq!(read_to_end(client), b"", "{what}");
            assert!(
                handed.elapsed() < within,
                "{what}: closed {:?} after",
                handed.elapsed()
            );
        };
        // Its client silent: it closes once its header time is up, 100 ms
        // on, as it would have inline.
        let (silent_client, mut silent) = loopback(&server);
        advance(&server, HEADER_READ_TIMEOUT - Duration::from_millis(100));
        silent.send_to_hyper(true);
        ends_soon(&silent_client, Duration::from_secs(2), "silent");

        // One whose client stopped inside the h2 preface, on which hyper's
        // sniff would wait, closes at its header time too.
        let (stalled_client, mut stalled) = loopback(&server);
        (&stalled_client).write_all(b"P").unwrap();
        await_bytes(&stalled);
        assert!(matches!(stalled.next(true), Next::Readable));
        advance(&server, HEADER_READ_TIMEOUT - Duration::from_millis(100));
        assert!(matches!(stalled.hand_back(), Next::Closed));
        ends_soon(
            &stalled_client,
            Duration::from_secs(2),
            "inside the preface",
        );

        // Once the drain began, a silent one closes at once.
        let _accepting = server.shutdown_tx.subscribe();
        server.stop_accepting();
        let (silent_client, mut silent) = loopback(&server);
        silent.send_to_hyper(true);
        ends_soon(
            &silent_client,
            Duration::from_millis(500),
            "silent, the drain begun",
        );
    }

    #[test]
    fn a_connection_handed_to_hyper_inside_the_h2_preface_that_then_ends_closes_at_once() {
        // Its job read a start of the preface or nothing, then its client
        // sent more of it and ended its side, or reset: what hyper reads
        // first comes from the worker, the socket, or both.
        for (read, then) in [(&b"P"[..], &b"R"[..]), (b"", b"P")] {
            for reset in [false, true] {
                let end = if reset { "RST" } else { "FIN" };
                let what = format!(
                    "{:?} read, {:?} then a {end}",
                    read.escape_ascii().to_string(),
                    then.escape_ascii().to_string()
                );
                let shown = what.clone();
                crate::testing::on_own_runtime(what, Duration::from_secs(3), async move {
                    let server = server();
                    let permits = Arc::new(tokio::sync::Semaphore::new(1));
                    let permit = permits.clone().try_acquire_owned().expect("a permit");
                    let (client, mut conn) = loopback_holding(&server, permit);
                    if !read.is_empty() {
                        (&client).write_all(read).unwrap();
                        await_bytes(&conn);
                    }
                    assert!(matches!(conn.next(true), Next::Readable));
                    (&client).write_all(then).unwrap();
                    let client = if reset {
                        crate::inline::testing::abortive(&client);
                        drop(client);
                        None
                    } else {
                        client.shutdown(std::net::Shutdown::Write).unwrap();
                        client.set_nonblocking(true).unwrap();
                        Some(tokio::net::TcpStream::from_std(client).expect("a client"))
                    };
                    assert!(matches!(conn.hand_back(), Next::Closed));
                    // Hyper reads all its client sent, and the end, and
                    // closes: a close with some of it unread would reset.
                    if let Some(mut client) = client {
                        let mut rest = Vec::new();
                        let read = tokio::io::AsyncReadExt::read_to_end(&mut client, &mut rest);
                        tokio::time::timeout(Duration::from_secs(1), read)
                            .await
                            .unwrap_or_else(|_| panic!("{shown}: still open a second on"))
                            .unwrap_or_else(|e| panic!("{shown}: the end: {e}"));
                    }
                    // Its task ends, which gives its permit back.
                    let ended = tokio::time::timeout(Duration::from_secs(1), permits.acquire());
                    assert!(
                        ended.await.is_ok(),
                        "{shown}: still served a second after its hand-back"
                    );
                });
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_request_whose_client_then_ends_its_side_is_answered_inline_where_hyper_closes_it() {
        let server = inline_loop_server(1, 8);
        let get = &b"GET /hello HTTP/1.1\r\nhost: x\r\n\r\n"[..];
        // The job reads a request whole and looks at the socket again only
        // for the next: it answers, then reads the end.
        let (client, mut conn) = loopback(&server);
        (&client).write_all(get).unwrap();
        client.shutdown(std::net::Shutdown::Write).unwrap();
        answer(&next_request(&mut conn), b"inline");
        assert!(
            reads_within(conn.fd().expect("open"), 5_000),
            "the end never came"
        );
        assert!(
            matches!(look(&mut conn, true), Next::Closed),
            "kept open past its client's end"
        );
        let read = read_to_end(&client);
        assert!(read.starts_with(b"HTTP/1.1 200 OK\r\n") && read.ends_with(b"\r\n\r\ninline"));
        // Part of a head, or nothing: it closes, and no app sees a request.
        for bytes in [&b"GET /hello HT"[..], b""] {
            let (client, mut conn) = loopback(&server);
            (&client).write_all(bytes).unwrap();
            client.shutdown(std::net::Shutdown::Write).unwrap();
            eventually_blocking("the close", || match conn.next(true) {
                Next::Closed => Some(()),
                Next::Readable => None,
                _ => panic!(
                    "{:?} then the end: a request",
                    bytes.escape_ascii().to_string()
                ),
            });
            assert_eq!(read_to_end(&client), b"");
        }
        // Handed back whole, as its worker has no room: hyper reads the end
        // before any answer and closes, as on its own connections.
        let (client, mut conn) = loopback(&server);
        (&client).write_all(get).unwrap();
        client.shutdown(std::net::Shutdown::Write).unwrap();
        await_bytes(&conn);
        assert!(matches!(conn.next(false), Next::Closed));
        assert_eq!(read_to_end(&client), b"", "handed back: answered");
        assert_eq!(handbacks(&server), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_connection_a_write_still_waits_on_is_cut_rather_than_handed_to_hyper() {
        let server = inline_loop_server(2, 8);
        let slot = slot(&server, 0);
        let (client, mut conn) = narrowed(&server);
        send_get(&client, "/s", false);
        let ctx = next_request(&mut conn);
        let io = ctx.inline.clone().expect("an inline request");
        assert!(io.start_stream(
            &ctx.responder,
            http::StatusCode::OK,
            &http::HeaderMap::new()
        ));
        let chunk = vec![b'x'; 1 << 20];
        // A helper's write that waits on the socket, while the stream
        // ends; then its worker retires.
        loop {
            match io.write_chunk(&ctx.responder, &chunk) {
                Wrote::Done => {}
                Wrote::Wait(_) => break,
                wrote => panic!("the stream ended: {wrote:?}"),
            }
        }
        io.finish_stream(&ctx.responder);
        slot.retire();
        let reader = std::thread::spawn(move || read_to_end(&client));

        // Its answer out, its job would hand it to hyper: the descriptor
        // is the wait's, so the connection ends instead.
        assert!(matches!(once_written(&mut conn), Next::Closed));
        let read = reader.join().expect("the client");
        assert!(
            read.ends_with(b"0\r\n\r\n"),
            "the answer did not go out whole"
        );
        let counts = (handbacks(&server), cut_offs(&server));
        assert_eq!(counts, (0, 1));
        io.waited();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_connection_whose_job_ends_while_it_waits_idle_goes_to_hyper_with_its_clients_request(
    ) {
        let server = inline_loop_server(2, 8);
        let (client, mut conn) = loopback(&server);
        send_get(&client, "/first", false);
        answer(&next_request(&mut conn), b"inline");
        let mut wire = Wire::of(&client);
        assert_eq!(wire.response().2, b"inline");
        assert!(matches!(conn.next(true), Next::Readable));
        send_get(&client, "/sent", false);
        await_bytes(&conn);

        // Its job ends (its task stopped, its thread killed) as it waits.
        drop(conn);

        answer_dispatched(&server, &["/sent"]).await;
        assert_eq!(wire.response().2, b"hyper:/sent");
        assert_eq!((handbacks(&server), cut_offs(&server)), (1, 0));
    }
}
