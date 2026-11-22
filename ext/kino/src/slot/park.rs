//! Whether a slot's worker is parked, and since when.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use crate::queue::Item;
use crate::registry::ServerInner;

/// The dispatcher prefers awake lanes, so a hot worker never pays a futex wake.
#[derive(Default)]
pub struct Park {
    pub parked: AtomicBool,
    /// ServerInner::park_seq at its last park (0 = never); among idle workers the last is warmest.
    pub parked_at: AtomicU64,
    /// Only wakes were taken since parked_at was stamped, so the next park keeps it. Owner only.
    pub woke: AtomicBool,
    /// Socket events since a take last looked at the lane, a wake counting as a whole run
    /// (queue::OWN_RUN), so neither a flooding socket nor a self-waking hook starves requests.
    pub own_run: AtomicU32,
}

impl Park {
    /// A worker that has taken only wakes since its parked_at was stamped
    /// keeps it: periodic wakes must not reorder the hot-lane dispatch.
    pub fn park(&self, server: &ServerInner) {
        if !self.woke.load(Ordering::Relaxed) {
            let seq = server.park_seq.fetch_add(1, Ordering::Relaxed) + 1;
            self.parked_at.store(seq, Ordering::Relaxed);
        }
        self.woke.store(true, Ordering::Relaxed);
        self.parked.store(true, Ordering::SeqCst);
    }

    pub fn unpark(&self) {
        self.parked.store(false, Ordering::SeqCst);
    }

    #[inline]
    pub fn took(&self, item: Option<&Item>) {
        let only_wakes = self.woke.load(Ordering::Relaxed) && matches!(item, Some(Item::Wake));
        self.woke.store(only_wakes, Ordering::Relaxed);
    }

    /// Advisory: the dispatcher's look.
    #[inline]
    pub fn is_parked(&self) -> bool {
        self.parked.load(Ordering::Relaxed)
    }

    /// Its place in the hot-lane order: the later it parked, the warmer.
    #[inline]
    pub fn parked_at(&self) -> u64 {
        self.parked_at.load(Ordering::Relaxed)
    }
}
