//! A socket as its writers see it: the outgoing queue and its budgets, inbound credit, and closing.

use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::{mpsc, Notify};

use super::codec::close_frame;
use super::drive::Liveness;
use super::stats::{Cause, WsCount, COUNTS};
use super::CLOSE_TIMEOUT;
use crate::slot::WorkerSlot;

/// Move `counter` to what `next` makes of it: Ok with the old value, Err with the refused
/// one. Not std's fetch_update: it is deprecated, and its successor needs Rust 1.95.
pub(super) fn update(
    counter: &AtomicUsize,
    next: impl Fn(usize) -> Option<usize>,
) -> Result<usize, usize> {
    let mut seen = counter.load(Ordering::SeqCst);
    loop {
        let to = next(seen).ok_or(seen)?;
        match counter.compare_exchange_weak(seen, to, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => return Ok(seen),
            Err(now) => seen = now,
        }
    }
}

/// Add `len` to `queued` unless that passes `cap`; anything fits when nothing is queued.
pub(super) fn charge(queued: &AtomicUsize, cap: usize, len: usize) -> bool {
    update(queued, |held| {
        (held == 0 || held.saturating_add(len) <= cap).then_some(held + len)
    })
    .is_ok()
}

pub(super) enum Out {
    /// Counted against the byte budget until written.
    Frame(Bytes),
    /// Kino's own (a pong), outside the byte budget.
    Control(Bytes),
    /// Kino's ping: the client's time to answer starts once it is written.
    Ping(Bytes),
    Close(Bytes),
}

#[derive(Clone)]
pub struct Conn {
    pub(super) tx: mpsc::Sender<Out>,
    pub(super) abort: Arc<Abort>,
    pub(super) owner: Arc<WorkerSlot>,
}

/// What a socket's writers share with its connection task.
#[derive(Default)]
pub(super) struct Abort {
    /// Drops the connection even when its queue is full (a slow client).
    pub(super) notify: Notify,
    /// Set at unregister, before its subscriptions go; see Conn::subscribing.
    pub(super) gone: AtomicBool,
    pub(super) subscribed: AtomicBool,
    /// Set by the first close queued; `close_queued` starts the client's CLOSE_TIMEOUT.
    pub(super) closing: AtomicBool,
    pub(super) close_queued: Notify,
    pub(super) queued_bytes: AtomicUsize,
    pub(super) queue_bytes: usize,
    /// Data frames not yet written, with REFUSED set by a refused write: whoever sees the
    /// count reach zero with it set clears it and tells the worker.
    pub(super) queued_frames: AtomicUsize,
    /// Messages with the worker, counting one being read; `credit` wakes a reader waiting for room.
    pub(super) unhandled: AtomicUsize,
    pub(super) inbound: usize,
    pub(super) credit: Notify,
    /// The drain's goodbye: `leave` after `leave_after_us`, after on_shutdown (`goodbye`) if any.
    pub(super) leave: Notify,
    pub(super) leave_after_us: AtomicU64,
    pub(super) on_shutdown: AtomicBool,
    pub(super) goodbye: Notify,
    pub(super) cause: AtomicU8,
    pub(super) switched: AtomicBool,
}

pub(super) const REFUSED: usize = 1 << (usize::BITS - 1);

impl Abort {
    pub(super) fn end_as(&self, cause: Cause) {
        let _ = self
            .cause
            .compare_exchange(0, cause as u8, Ordering::SeqCst, Ordering::SeqCst);
    }

    pub(super) fn cause(&self) -> Option<Cause> {
        Cause::of(self.cause.load(Ordering::SeqCst))
    }

    pub(super) async fn close_expired(&self) {
        self.close_queued.notified().await;
        tokio::time::sleep(CLOSE_TIMEOUT).await;
    }

    /// Waits while `inbound` are unhandled; a slot whose events are dropped has room.
    pub(super) async fn take_credit(&self, owner: &WorkerSlot, liveness: &Liveness) {
        let mut paused = false;
        loop {
            let taken = update(&self.unhandled, |unhandled| {
                (unhandled < self.inbound).then_some(unhandled + 1)
            })
            .is_ok();
            if taken || owner.sockets.abandoned() {
                return;
            }
            if !paused {
                paused = true;
                COUNTS.add(WsCount::InboundPauses);
            }
            liveness.on_worker(self.credit.notified()).await;
        }
    }

    pub(super) fn return_credit(&self) {
        let returned = update(&self.unhandled, |unhandled| unhandled.checked_sub(1));
        if returned.is_ok_and(|before| before >= self.inbound) {
            self.credit.notify_one();
        }
    }

    pub(super) fn pending(&self) -> usize {
        self.queued_frames.load(Ordering::SeqCst) & !REFUSED
    }

    /// The queue emptied since a refused write; true once, for the caller that tells the worker.
    pub(super) fn drained(&self) -> bool {
        self.queued_frames.load(Ordering::SeqCst) == REFUSED
            && self
                .queued_frames
                .compare_exchange(REFUSED, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
    }

    /// After what is queued, ending for `cause`; the first close stands. False on a full queue.
    pub(super) fn close(&self, tx: &mpsc::Sender<Out>, code: u16, cause: Cause) -> bool {
        if self.closing.swap(true, Ordering::SeqCst) {
            return true;
        }
        if tx.try_send(Out::Close(close_frame(code))).is_err() {
            self.closing.store(false, Ordering::SeqCst);
            return false;
        }
        self.end_as(cause);
        self.close_queued.notify_one();
        true
    }

    pub(super) fn drop_as(&self, cause: Cause) {
        self.end_as(cause);
        self.notify.notify_one();
    }
}

#[derive(Default)]
pub struct SlotSockets {
    /// Not closed yet as the worker knows: their close event has not reached its inbox.
    pub(super) open: AtomicUsize,
    /// The code its sockets were closed with (0: none); handshakes are refused from then.
    pub(super) close_code: AtomicU16,
    /// No worker takes the slot's events (it crashed, or is wedged): they are dropped.
    pub(super) abandoned: AtomicBool,
}

impl SlotSockets {
    pub fn open(&self) -> usize {
        self.open.load(Ordering::SeqCst)
    }

    pub fn accepting(&self) -> bool {
        self.close_code.load(Ordering::Relaxed) == 0 && !self.abandoned()
    }

    pub fn reset(&self) {
        self.close_code.store(0, Ordering::SeqCst);
        self.abandoned.store(false, Ordering::SeqCst);
    }

    pub(super) fn abandoned(&self) -> bool {
        self.abandoned.load(Ordering::SeqCst)
    }
}

impl Conn {
    /// Mark the socket as subscribing, under the topic lock; false if it was unregistered
    /// meanwhile. unregister marks it gone, then forgets subscriptions only if it sees this
    /// mark: one sees the other's, so either the subscribe gives up or the forget waits for it.
    pub(super) fn subscribing(&self) -> bool {
        self.abort.subscribed.store(true, Ordering::SeqCst);
        !self.abort.gone.load(Ordering::SeqCst)
    }

    /// Without waiting: full at `Limits::queue` frames or past `Limits::queue_bytes`.
    fn push(&self, frame: Bytes) -> Push {
        let len = frame.len();
        if !charge(&self.abort.queued_bytes, self.abort.queue_bytes, len) {
            // A writer that ended leaves the bytes of what it never wrote.
            return if self.tx.is_closed() {
                Push::Gone
            } else {
                Push::Full
            };
        }
        // Counted first: the writer may write it before try_send returns.
        self.abort.queued_frames.fetch_add(1, Ordering::SeqCst);
        match self.tx.try_send(Out::Frame(frame)) {
            Ok(()) => Push::Queued,
            Err(refused) => {
                self.abort.queued_bytes.fetch_sub(len, Ordering::SeqCst);
                self.abort.queued_frames.fetch_sub(1, Ordering::SeqCst);
                match refused {
                    mpsc::error::TrySendError::Full(_) => Push::Full,
                    mpsc::error::TrySendError::Closed(_) => Push::Gone,
                }
            }
        }
    }

    /// A publish; false only when the queue is full (a client too slow to keep up).
    pub(super) fn offer(&self, frame: Bytes) -> bool {
        !matches!(self.push(frame), Push::Full)
    }

    pub(super) fn drop_slow(&self) {
        self.abort.drop_as(Cause::Slow);
    }

    /// An app write; a refusal by the full queue is marked, so on_drained follows.
    pub(super) fn write(&self, frame: Bytes) -> bool {
        match self.push(frame) {
            Push::Queued => true,
            Push::Full => {
                self.refused_write();
                false
            }
            Push::Gone => false,
        }
    }

    /// The app's close; a full queue refuses it as it does a write.
    pub(super) fn close(&self, code: u16) -> bool {
        let queued = self.abort.close(&self.tx, code, Cause::Normal);
        if !queued && !self.tx.is_closed() {
            self.refused_write();
        }
        queued
    }

    pub(super) fn owned_by(&self, slot: &WorkerSlot) -> bool {
        std::ptr::eq(&*self.owner, slot)
    }

    fn refused_write(&self) {
        self.refused();
        COUNTS.add(WsCount::WritesRefused);
    }

    /// The worker hears once the queue empties; one that emptied meanwhile is sent an
    /// empty frame, so its writer sees the mark.
    pub(super) fn refused(&self) {
        let before = self.abort.queued_frames.fetch_or(REFUSED, Ordering::SeqCst);
        if before & !REFUSED == 0 {
            let _ = self.tx.try_send(Out::Control(Bytes::new()));
        }
    }
}

enum Push {
    Queued,
    Full,
    Gone,
}
