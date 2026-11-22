//! What a slot's worker is serving.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

use parking_lot::Mutex;

use crate::response::Responder;

/// Relaxed, advisory. in_flight counts a socket callback or wake hook too; last_started_us
/// drives wedge reporting and steers dispatch off a slow item.
#[derive(Default)]
pub struct Load {
    /// What the supervisor 500s if the worker dies: a batch may hold several.
    pub current: Mutex<smallvec::SmallVec<[Weak<Responder>; 8]>>,
    pub in_flight: AtomicUsize,
    pub served: AtomicU64,
    pub last_started_us: AtomicU64,
}

impl Load {
    #[inline]
    pub fn started(&self) -> u64 {
        let now = crate::mono::mono_us();
        self.last_started_us.store(now, Ordering::Relaxed);
        now
    }

    /// A request joins what the worker serves.
    #[inline]
    pub fn admit(&self, responder: &Arc<Responder>) {
        self.served.fetch_add(1, Ordering::Relaxed);
        self.started();
        self.in_flight.fetch_add(1, Ordering::Relaxed);
        self.current.lock().push(Arc::downgrade(responder));
    }

    /// The worker came back for its next take: the previous batch is fully answered.
    #[inline]
    pub fn idle(&self) {
        self.current.lock().clear();
        self.in_flight.store(0, Ordering::Relaxed);
    }

    /// A socket callback or a wake hook counts as one item, busy until the next take, so one
    /// that never returns reads as a wedge.
    #[inline]
    pub fn runs_item(&self) {
        self.started();
        self.in_flight.store(1, Ordering::Relaxed);
    }

    /// When its current work started (mono us); None while nothing is in flight.
    #[inline]
    pub fn busy_since(&self) -> Option<u64> {
        if self.in_flight.load(Ordering::Relaxed) == 0 {
            return None;
        }
        Some(self.last_started_us.load(Ordering::Relaxed))
    }

    pub fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Relaxed)
    }

    pub fn served(&self) -> u64 {
        self.served.load(Ordering::Relaxed)
    }
}
