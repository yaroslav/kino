//! Which worker is on a slot, and how it is to leave.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use parking_lot::Mutex;

/// Which worker is on the slot, and how it is to leave.
#[derive(Default)]
pub struct Life {
    /// Moves on whenever a worker leaves the slot or the slot goes to a new one, so a Waker
    /// or a retirement meant for one worker cannot reach a later one.
    pub generation: AtomicU64,
    /// Set by the scaler, or by abort: dispatch skips the slot and the take loop ends at its
    /// next idle tick. Cleared by reset_under.
    pub retired: AtomicBool,
    /// Abandoned as wedged: out of wedge detection, busy_ms reported as 0.
    pub quarantined: AtomicBool,
    /// Past the shutdown deadline (see WorkerSlot::halt): the slot's waits, polling it between
    /// bounded ticks, end as a shutdown. Kino's alone: never the target of a VM unblock
    /// function, and lowered only for the slot's next worker. Private, so that nothing but
    /// halt and reset_halt writes it.
    halted: AtomicBool,
    /// worker_loop: when serve returned (mono us) and its label, for lingering_loops' warning.
    pub drained: Mutex<Option<(u64, String)>>,
}

impl Life {
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }

    /// Exact under the lane lock (see WorkerSlot::retire_at), and for the worker's own exit.
    #[inline]
    pub fn retired(&self) -> bool {
        self.retired.load(Ordering::SeqCst)
    }

    /// Advisory: a look that a later exact one, or the next tick, corrects.
    #[inline]
    pub fn seems_retired(&self) -> bool {
        self.retired.load(Ordering::Relaxed)
    }

    /// The stop flag a park or a blocked body read polls.
    #[inline]
    pub fn halted(&self) -> &AtomicBool {
        &self.halted
    }

    #[inline]
    pub fn is_halted(&self) -> bool {
        self.halted.load(Ordering::SeqCst)
    }

    /// Past the shutdown deadline: the slot's waits end.
    pub(super) fn halt(&self) {
        self.halted.store(true, Ordering::SeqCst);
    }

    /// For the slot's next worker.
    pub(super) fn reset_halt(&self) {
        self.halted.store(false, Ordering::SeqCst);
    }

    /// worker_loop: its serve returned now, under `label`.
    pub fn drained(&self, label: String) {
        *self.drained.lock() = Some((crate::mono::mono_us(), label));
    }

    /// worker_loop: the label of a loop whose serve returned by `due` (mono us), once.
    pub fn take_drained_by(&self, due: u64) -> Option<String> {
        let mut drained = self.drained.lock();
        match drained.as_ref() {
            Some((at, _)) if *at <= due => drained.take().map(|(_, label)| label),
            _ => None,
        }
    }

    /// Advisory, for the stats.
    pub fn quarantined(&self) -> bool {
        self.quarantined.load(Ordering::Relaxed)
    }
}
