//! A worker slot: one per worker thread, known to Ruby by its index. Dispatch,
//! the take loop, the pool and the drain meet here. Each part owns its state: the lane
//! (lane.rs), the park flag (park.rs), what is in flight (load.rs), and who is on the slot
//! (life.rs).

use std::sync::atomic::{AtomicUsize, Ordering};

use crate::registry::LaneItem;
use crate::response::Cut;

mod lane;
mod life;
mod load;
mod park;

pub use lane::Lane;
pub use life::Life;
pub use load::Load;
pub use park::Park;

/// Small, so a slow handler delays at most this many neighbors (stealing rescues them).
pub const LANE_DEPTH: usize = 4;

/// A full inbox stops the sockets' readers; each socket's share is lower (ws::INBOUND_CREDIT).
pub const WS_INBOX: usize = 1024;

/// A held lane lock: retire, quarantine and recycle check the generation under it.
type LaneGuard<'a> = parking_lot::MutexGuard<'a, Option<flume::Sender<LaneItem>>>;

#[derive(Default)]
pub struct WorkerSlot {
    pub lane: Lane,
    pub park: Park,
    pub load: Load,
    pub life: Life,
    /// Lane mode only: the WebSockets this worker accepted.
    pub sockets: crate::ws::SlotSockets,
    /// Open hijack bridges of this worker's requests. In :ractor mode the app serves them
    /// from the worker's Ractor, so a worker holding any is not idle.
    pub hijacked: AtomicUsize,
}

impl WorkerSlot {
    pub fn new(lanes: bool, inbox: usize) -> Self {
        WorkerSlot {
            lane: Lane::new(lanes, inbox),
            ..WorkerSlot::default()
        }
    }

    pub fn generation(&self) -> u64 {
        self.life.generation()
    }

    /// Every lane send comes here. Full for a request once the worker retired.
    #[inline]
    pub fn send_lane(&self, item: LaneItem) -> Result<(), flume::TrySendError<LaneItem>> {
        let lane = self.lane.tx.lock();
        let Some(tx) = lane.as_ref() else {
            return Err(flume::TrySendError::Disconnected(item));
        };
        let request = matches!(item, LaneItem::Request(_));
        // Under the lane lock, as WorkerSlot::retire_at orders it.
        if request && self.life.retired() {
            return Err(flume::TrySendError::Full(item));
        }
        tx.try_send(item)
    }

    pub fn close_lane(&self) {
        self.lane.tx.lock().take();
    }

    /// One bell in the lane at a time, for what reached the inbox or a due wake; a full lane's
    /// worker is busy and checks its inbox on every take.
    pub fn notify(&self) {
        if self.lane.bell_pending.swap(true, Ordering::SeqCst) {
            return;
        }
        if self.send_lane(LaneItem::Bell).is_err() {
            self.lane.took_bell();
        }
    }

    /// The next take hands `:wake` before any request; wakes until then coalesce. False
    /// without lanes.
    pub fn wake(&self) -> bool {
        if !self.lane.exists() {
            return false;
        }
        if !self.lane.wake_pending.swap(true, Ordering::SeqCst) {
            self.notify();
        }
        true
    }

    /// Send the worker home once idle, if still at `generation`. Raised under the lane lock
    /// the dispatcher holds while it checks and sends, so every dispatch that saw "not
    /// retired" is in the lane before the worker drains it. The sockets close with 1001
    /// first, so a later worker on the slot cannot inherit the close.
    pub fn retire_at(&self, generation: u64) -> bool {
        let lane = self.lane.tx.lock();
        let current = self.generation() == generation;
        if current {
            self.retire_under(&lane);
        }
        current
    }

    fn retire_under(&self, _lane: &LaneGuard<'_>) {
        crate::ws::close_slot(self, crate::ws::GOING_AWAY, crate::ws::Cause::GoingAway);
        self.life.retired.store(true, Ordering::SeqCst);
    }

    /// Give the slot up as wedged if still at `generation` (the sample may predate a crash):
    /// its sockets' events are dropped and they close with 1011.
    pub fn quarantine_at(&self, generation: u64) -> bool {
        {
            let _lane = self.lane.tx.lock();
            if self.generation() != generation {
                return false;
            }
            self.life.quarantined.store(true, Ordering::SeqCst);
        }
        crate::ws::give_up_slot(self);
        true
    }

    /// Hand the slot to a new worker once quiet: no socket of the last worker counts on it,
    /// no hijacked connection is open, and it is not quarantined (its worker may come back).
    /// Slots are never removed. Returns the new generation.
    pub fn recycle(&self) -> Option<u64> {
        let lane = self.lane.tx.lock();
        let quiet = self.sockets.open() == 0
            && self.hijacked.load(Ordering::SeqCst) == 0
            && !self.life.quarantined.load(Ordering::SeqCst);
        if !quiet {
            return None;
        }
        crate::ws::hand_over(self);
        self.reset_under(&lane);
        Some(self.generation())
    }

    /// 500 or cut (for `why`) what the worker was serving, and retire the slot.
    pub fn abort(&self, why: Cut) {
        for weak in self.load.current.lock().drain(..) {
            if let Some(responder) = weak.upgrade() {
                responder.give_up(why);
            }
        }
        // A dead worker holds nothing: no phantom busy worker in the stats.
        self.load.in_flight.store(0, Ordering::Relaxed);
        // Retired under the lane lock as in retire_at, so dispatch stops; dropping what is
        // queued fires the Drop-500 backstop. The lane stays for the slot's next worker.
        {
            let _lane = self.lane.tx.lock();
            self.life.retired.store(true, Ordering::SeqCst);
        }
        self.park.parked.store(true, Ordering::Relaxed);
        if let Some(rx) = self.lane.rx.as_ref() {
            while rx.try_recv().is_ok() {}
        }
    }

    /// Abort, and close its sockets with 1011 (their handlers died with it); how many.
    pub fn crash(&self) -> usize {
        self.abort(Cut::Crash);
        crate::ws::give_up_slot(self)
    }

    pub fn interrupt(&self) {
        self.life.interrupted.store(true, Ordering::SeqCst);
    }

    /// Its worker leaves: the wakers it handed out stop waking.
    pub fn release(&self) {
        self.life.generation.fetch_add(1, Ordering::SeqCst);
    }

    /// Fresh for a new worker; the lane channel and the quarantine mark stay.
    fn reset_under(&self, _lane: &LaneGuard<'_>) {
        self.load.current.lock().clear();
        self.life.retired.store(false, Ordering::SeqCst);
        self.park.parked.store(false, Ordering::SeqCst);
        self.park.parked_at.store(0, Ordering::Relaxed);
        self.life.interrupted.store(false, Ordering::SeqCst);
        self.lane.wake_pending.store(false, Ordering::SeqCst);
        // A crashed worker's lane was drained without taking its doorbell.
        self.lane.bell_pending.store(false, Ordering::SeqCst);
        self.life.generation.fetch_add(1, Ordering::SeqCst);
        self.sockets.reset();
        self.park.own_run.store(0, Ordering::Relaxed);
        self.park.woke.store(false, Ordering::Relaxed);
        self.load.served.store(0, Ordering::Relaxed);
        self.load.in_flight.store(0, Ordering::Relaxed);
        self.load.last_started_us.store(0, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering::{Relaxed, SeqCst};
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use crate::testing::{slot, TestServer};

    impl WorkerSlot {
        /// `retire_at` the slot's current generation.
        pub(crate) fn retire(&self) {
            let lane = self.lane.tx.lock();
            self.retire_under(&lane);
        }

        /// Return the slot to its fresh state, quiet or not (recycle checks).
        pub(crate) fn reset(&self) {
            let lane = self.lane.tx.lock();
            self.reset_under(&lane);
        }
    }

    fn lanes_slot() -> Arc<WorkerSlot> {
        slot(&TestServer::lanes().workers(1).build(), 0)
    }

    #[test]
    fn a_reset_slot_is_fresh_for_its_next_worker_but_for_its_lane_and_a_quarantine_mark() {
        let slot = lanes_slot();
        slot.park.parked.store(true, Relaxed);
        slot.life.interrupted.store(true, Relaxed);
        slot.life.quarantined.store(true, Relaxed);
        slot.lane.wake_pending.store(true, Relaxed);
        slot.load.served.store(5, Relaxed);
        slot.load.in_flight.store(1, Relaxed);
        slot.load.last_started_us.store(9, Relaxed);
        slot.load.current.lock().push(std::sync::Weak::new());

        slot.retire();
        slot.reset();
        let flags = [
            slot.life.retired.load(Relaxed),
            slot.park.parked.load(Relaxed),
            slot.life.interrupted.load(Relaxed),
            slot.lane.wake_pending.load(Relaxed),
        ];
        assert_eq!(
            flags, [false; 4],
            "the last occupant's wake is not the next one's"
        );
        let load = (
            slot.load.served.load(Relaxed),
            slot.load.in_flight.load(Relaxed),
            slot.load.last_started_us.load(Relaxed),
            slot.load.current.lock().len(),
        );
        assert_eq!(load, (0, 0, 0, 0));
        // A wedged slot stays flagged, should it ever come back around, and
        // the next occupant takes from the same lane.
        assert!(slot.life.quarantined.load(Relaxed) && slot.lane.tx.lock().is_some());
    }

    #[test]
    fn a_slot_is_recycled_once_its_last_worker_left_nothing_open_unless_it_was_quarantined() {
        // Its socket counts in the process-wide counts socket tests read.
        let _serial = crate::testing::serial_blocking();
        let slot = lanes_slot();
        slot.retire();
        slot.hijacked.store(1, SeqCst);
        let socket =
            crate::ws::testing::QueuedSocket::with_limits(slot.clone(), Default::default());

        assert!(slot.recycle().is_none(), "a hijacked connection is open");
        let why = "a refused recycle changes nothing";
        assert!(slot.life.retired.load(SeqCst), "{why}");
        slot.hijacked.store(0, SeqCst);
        assert!(slot.recycle().is_none(), "a socket is open");
        socket.close();
        assert!(slot.recycle().is_some());
        assert!(!slot.life.retired.load(SeqCst));

        slot.life.quarantined.store(true, SeqCst);
        slot.retire();
        assert!(
            slot.recycle().is_none(),
            "a quarantined slot is never recycled"
        );
    }

    #[test]
    fn retire_waits_for_a_dispatcher_holding_the_lane_lock() {
        let slot = lanes_slot();

        // A dispatcher that has already read "not retired" and is sending.
        let sending = slot.lane.tx.lock();
        let retiring = std::thread::spawn({
            let slot = slot.clone();
            move || slot.retire()
        });
        std::thread::sleep(Duration::from_millis(30));
        assert!(
            !slot.life.retired.load(SeqCst),
            "the flag must not go up under a dispatcher's lock"
        );

        drop(sending);
        retiring.join().expect("retire thread");
        assert!(slot.life.retired.load(SeqCst));
    }
}
