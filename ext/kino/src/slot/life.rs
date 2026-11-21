//! Which worker is on a slot, and how it is to leave.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

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
    /// The UBF target and the shutdown kick, polled between bounded waits.
    pub interrupted: AtomicBool,
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

    /// Advisory, for the stats.
    pub fn quarantined(&self) -> bool {
        self.quarantined.load(Ordering::Relaxed)
    }
}
