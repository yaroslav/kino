//! What a slot's worker is serving.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::response::Responder;

/// Load::holding for an item that is not an admitted request: no responder lives there.
pub const HOLDING_ITEM: usize = 1;

/// What the supervisor 500s if the worker dies, or its next take if it let go of one unanswered.
type Current = smallvec::SmallVec<[Arc<Responder>; 8]>;

/// Relaxed, advisory. in_flight counts a socket callback or wake hook too (a worker_loop
/// worker's: its open requests); last_started_us drives wedge reporting and steers dispatch
/// off a slow item.
#[derive(Default)]
#[repr(align(128))]
pub struct Load {
    /// A batch may hold several.
    pub current: Mutex<Current>,
    pub in_flight: AtomicUsize,
    pub served: AtomicU64,
    pub last_started_us: AtomicU64,
    /// worker_loop: what its thread holds, its request's responder address until answered, else
    /// HOLDING_ITEM; 0 back in its loop. Lane dispatch prefers an idle sibling to a holder.
    pub holding: AtomicUsize,
    /// worker_loop mode: the most requests its worker runs at once (0 = no limit).
    pub capacity: AtomicUsize,
}

impl Load {
    #[inline]
    pub fn started(&self) -> u64 {
        let now = crate::mono::mono_us();
        self.last_started_us.store(now, Ordering::Relaxed);
        now
    }

    /// A request joins what the worker serves at `now`, a clock read the caller made, counted
    /// in under the list's lock, as `answered` reads it.
    #[inline]
    pub fn admit(&self, responder: &Arc<Responder>, now: std::time::Instant) {
        self.served.fetch_add(1, Ordering::Relaxed);
        let now = crate::mono::us_at(now);
        self.last_started_us.store(now, Ordering::Relaxed);
        let mut current = self.current.lock();
        current.push(responder.clone());
        self.in_flight.fetch_add(1, Ordering::Relaxed);
    }

    /// A classic worker comes back for its next take, or a worker leaves its slot: what is listed
    /// unanswered is answered now (Responder::let_go), as nothing it started still runs.
    pub fn let_go(&self) {
        for responder in self.current.lock().drain(..) {
            responder.let_go();
        }
    }

    /// The worker came back for its next take: the previous batch is fully answered, what it
    /// left unanswered first (let_go).
    #[inline]
    pub fn idle(&self) {
        self.let_go();
        self.in_flight.store(0, Ordering::Relaxed);
    }

    /// A socket callback or a wake hook counts as one item, busy until the next take, so one
    /// that never returns reads as a wedge.
    #[inline]
    pub fn runs_item(&self) {
        self.started();
        self.in_flight.store(1, Ordering::Relaxed);
    }

    /// When its last item started (mono us), busy or not: a clock read a take need not repeat.
    #[inline]
    pub fn last_started(&self) -> u64 {
        self.last_started_us.load(Ordering::Relaxed)
    }

    /// A dead worker holds nothing: no phantom busy worker in the stats.
    pub fn clear(&self) {
        self.in_flight.store(0, Ordering::Relaxed);
        self.last_started_us.store(0, Ordering::Relaxed);
    }

    /// When its current work started (mono us); None while nothing is in flight.
    #[inline]
    pub fn busy_since(&self) -> Option<u64> {
        if self.in_flight.load(Ordering::Relaxed) == 0 {
            return None;
        }
        Some(self.last_started_us.load(Ordering::Relaxed))
    }

    /// worker_loop: when its thread last left its loop (mono us), or the end of the tick a
    /// doorbell wait gets; None before its first take and while it takes with no job slot.
    #[inline]
    pub fn away_since(&self) -> Option<u64> {
        match self.last_started_us.load(Ordering::Relaxed) {
            0 => None,
            started => Some(started),
        }
    }

    /// worker_loop: its thread holds `what`, a request's responder address or HOLDING_ITEM.
    #[inline]
    pub fn hold(&self, what: usize) {
        self.holding.store(what, Ordering::Relaxed);
    }

    /// worker_loop: its thread holds a request or an item it has not come back from.
    #[inline]
    pub fn is_holding(&self) -> bool {
        self.holding.load(Ordering::Relaxed) != 0
    }

    /// worker_loop: how long its thread stays away from here is its busy age. Returns now.
    #[inline]
    pub fn back_in_loop(&self) -> u64 {
        self.hold(0);
        self.started()
    }

    /// worker_loop: a doorbell wait is time in the loop, back within a TICK (a send or the
    /// ticker rings it), so its busy age starts only once the wait is overdue.
    #[inline]
    pub fn waits_on_doorbell(&self, now: u64) {
        let tick = crate::queue::TICK.as_micros() as u64;
        self.last_started_us.store(now + tick, Ordering::Relaxed);
    }

    /// worker_loop: its loop ended, so what still waits on the worker no longer makes it busy.
    pub fn loop_ended(&self) {
        self.last_started_us.store(0, Ordering::Relaxed);
    }

    /// worker_loop: the most requests its worker runs at once, set by its serve.
    pub fn set_capacity(&self, capacity: usize) {
        self.capacity.store(capacity, Ordering::Relaxed);
    }

    /// worker_loop: the job slots its worker has free.
    #[inline]
    pub fn room(&self) -> usize {
        let capacity = self.capacity.load(Ordering::Relaxed);
        capacity.saturating_sub(self.in_flight.load(Ordering::Relaxed))
    }

    pub fn at_capacity(&self) -> bool {
        let capacity = self.capacity.load(Ordering::Relaxed);
        capacity > 0 && self.in_flight.load(Ordering::Relaxed) >= capacity
    }

    /// worker_loop: prunes the in-flight list; whether that took it below its capacity.
    pub fn keep_open(&self) -> bool {
        let mut current = self.current.lock();
        let full = self.at_capacity();
        self.count_open(&mut current);
        full && !self.at_capacity()
    }

    /// worker_loop: its thread no longer holds the request, and its open ones are counted now,
    /// not at its next take, which may wait for the job's hooks. At capacity only a take with
    /// room lowers the count, telling a request waiting for lane room; the list's lock keeps
    /// this out of its way.
    pub fn answered(&self, responder: &Arc<Responder>) {
        let request = Arc::as_ptr(responder) as usize;
        let relaxed = Ordering::Relaxed;
        let _ = self.holding.compare_exchange(request, 0, relaxed, relaxed);
        let mut current = self.current.lock();
        if !self.at_capacity() {
            self.count_open(&mut current);
        }
    }

    /// Under the list's lock, which admit counts a request in under too.
    fn count_open(&self, current: &mut Current) {
        current.retain(|responder| responder.open());
        self.in_flight.store(current.len(), Ordering::Relaxed);
    }

    pub fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Relaxed)
    }

    pub fn served(&self) -> u64 {
        self.served.load(Ordering::Relaxed)
    }
}
