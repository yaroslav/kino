//! Zero-copy response bodies: a large body's bytes ride to hyper in
//! place instead of being copied at the FFI boundary, with the backing
//! Ruby string kept alive until hyper drops the buffer.
//!
//! Buffer stability is the same mechanism io.c uses to hold a string
//! across a GVL-released write: `rb_str_tmp_frozen_acquire` returns a
//! frozen string sharing the original's byte buffer (a frozen original
//! is returned as-is). Mutating the original afterwards copy-on-writes
//! the ORIGINAL side, so the shared buffer stays byte-stable for as
//! long as the acquired string lives.
//!
//! Liveness is a fixed slab of atomic VALUE slots per server, marked
//! from a `PinKeeper` TypedData object that the Ruby `Kino::Server`
//! holds for the server's lifetime (pin.rs never registers global GC
//! roots: `rb_gc_register_address` is not ractor-safe in Ruby 4.0).
//! The mark hook uses the pinning `rb_gc_mark`, so compaction never
//! moves a pinned string either. Because the keeper lives on the main
//! ractor, pinned buffers survive worker-ractor crashes while hyper is
//! still flushing them.
//!
//! Concurrency: inserts happen on worker threads (any ractor, its own
//! GVL held), release is a single atomic store from whatever tokio
//! thread drops the buffer, and the mark hook only loads atomics, so
//! no path takes a lock. A full slab degrades to the copy path.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use magnus::rb_sys::AsRawValue;
use magnus::RString;

/// Floor for the zero-copy path. Below it the pin machinery costs more
/// than the memcpy it saves. It is also a SOUNDNESS bound: it must
/// exceed the largest embeddable string (a 640-byte GC slot in Ruby
/// 4.0), so the acquired string's bytes are guaranteed to live in a
/// stable malloc'd buffer rather than inside a movable RVALUE, and the
/// string can never be re-embedded by compaction.
pub const ZERO_COPY_MIN: usize = 4096;

/// In-flight pins per server. Bounds slab memory (8 bytes per slot) and
/// GC mark work; overflow falls back to copying, so the cap is a
/// throughput heuristic, not a limit on concurrency.
const SLAB_CAPACITY: usize = 4096;

extern "C" {
    // Exported by libruby but absent from the public headers: io.c's
    // buffer-stabilizing primitive (see module docs). Signature per
    // string.c: VALUE rb_str_tmp_frozen_acquire(VALUE orig).
    fn rb_str_tmp_frozen_acquire(orig: rb_sys::VALUE) -> rb_sys::VALUE;
}

/// GC roots for in-flight response buffers. Slot value 0 = empty (a
/// VALUE of 0 is Qfalse, which can never be a pinned string).
pub struct PinSlab {
    slots: Box<[AtomicU64]>,
    /// Rotating claim cursor: keeps the free-slot scan O(1) amortized.
    cursor: AtomicUsize,
}

impl PinSlab {
    pub fn new() -> Self {
        PinSlab {
            slots: (0..SLAB_CAPACITY).map(|_| AtomicU64::new(0)).collect(),
            cursor: AtomicUsize::new(0),
        }
    }

    /// Root `value`; None when the slab is full (caller copies instead).
    fn insert(&self, value: rb_sys::VALUE) -> Option<usize> {
        let start = self.cursor.fetch_add(1, Ordering::Relaxed);
        for offset in 0..self.slots.len() {
            let index = (start + offset) % self.slots.len();
            if self.slots[index]
                .compare_exchange(0, value, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
            {
                return Some(index);
            }
        }
        None
    }

    /// Drop the root. Called from tokio threads: a plain atomic store,
    /// no Ruby API. The string stays alive until the next GC sweep.
    fn release(&self, index: usize) {
        self.slots[index].store(0, Ordering::SeqCst);
    }

    /// GC mark hook (PinKeeper). Runs during stop-the-world, so it must
    /// not allocate, lock, or panic: it only loads atomics. A slot
    /// concurrently released by a tokio thread is seen as either the
    /// live value (marked one cycle longer, harmless) or 0 (skipped).
    pub fn mark_all(&self) {
        for slot in self.slots.iter() {
            let value = slot.load(Ordering::SeqCst);
            if value != 0 {
                // SAFETY: non-zero slots hold VALUEs inserted by
                // pinned_bytes and not yet released, i.e. live objects;
                // rb_gc_mark is the pinning mark, callable from a mark
                // hook by design.
                unsafe { rb_sys::rb_gc_mark(value) };
            }
        }
    }

    #[cfg(test)]
    fn occupied(&self) -> usize {
        self.slots
            .iter()
            .filter(|s| s.load(Ordering::SeqCst) != 0)
            .count()
    }
}

/// The GC-visible face of a server's PinSlab: `Kino::Server` holds one
/// for its lifetime (surviving worker-ractor crashes), and Ruby's GC
/// marks every pinned string through it.
#[derive(magnus::TypedData)]
#[magnus(class = "Kino::Native::PinKeeper", free_immediately, mark)]
pub struct PinKeeper(pub Arc<PinSlab>);

impl magnus::DataTypeFunctions for PinKeeper {
    fn mark(&self, _marker: &magnus::gc::Marker) {
        self.0.mark_all();
    }
}

/// Owns one in-flight buffer: the raw view plus the slab slot rooting
/// the acquired string.
struct PinnedBuf {
    slab: Arc<PinSlab>,
    index: usize,
    ptr: *const u8,
    len: usize,
}

// SAFETY: the only operations performed off the Ruby thread are reading
// the byte buffer (the acquired string is frozen and slab-rooted: no
// writer exists and the buffer neither moves nor frees while the slot
// holds it) and Drop's atomic store. No Ruby API is called off-thread.
unsafe impl Send for PinnedBuf {}

impl AsRef<[u8]> for PinnedBuf {
    fn as_ref(&self) -> &[u8] {
        // SAFETY: ptr/len were captured under the GVL from the acquired
        // frozen string, which stays alive and byte-stable until this
        // owner releases its slot (see module docs).
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl Drop for PinnedBuf {
    fn drop(&mut self) {
        self.slab.release(self.index);
    }
}

/// Bytes borrowing `body`'s buffer, with the string rooted until hyper
/// drops it; None when the body is below ZERO_COPY_MIN or the slab is
/// full (caller copies). Requires the calling worker's GVL; safe from
/// any ractor.
pub fn pinned_bytes(slab: &Arc<PinSlab>, body: RString) -> Option<Bytes> {
    if body.len() < ZERO_COPY_MIN {
        return None;
    }
    // SAFETY: this worker's GVL is held; body is a live RString rooted
    // by the caller. The acquired tmp is rooted by this thread's machine
    // stack (conservatively scanned) until the slab insert publishes it.
    let tmp = unsafe { rb_str_tmp_frozen_acquire(body.as_raw()) };
    let index = slab.insert(tmp)?;
    // SAFETY: tmp is frozen, alive, and slab-rooted; len >= ZERO_COPY_MIN
    // rules out an embedded buffer, so ptr is a stable heap allocation.
    let (ptr, len) = unsafe {
        (
            rb_sys::macros::RSTRING_PTR(tmp) as *const u8,
            rb_sys::macros::RSTRING_LEN(tmp) as usize,
        )
    };
    Some(Bytes::from_owner(PinnedBuf {
        slab: slab.clone(),
        index,
        ptr,
        len,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_release_reuse_cycle() {
        let slab = PinSlab::new();
        let a = slab.insert(0x1000).expect("slot free");
        let b = slab.insert(0x2000).expect("slot free");
        assert_ne!(a, b);
        assert_eq!(slab.occupied(), 2);

        slab.release(a);
        assert_eq!(slab.occupied(), 1);

        // The freed slot is claimable again.
        let c = slab.insert(0x3000).expect("slot free");
        assert_eq!(slab.occupied(), 2);
        slab.release(b);
        slab.release(c);
        assert_eq!(slab.occupied(), 0);
    }

    #[test]
    fn full_slab_refuses_instead_of_evicting() {
        let slab = PinSlab::new();
        let indexes: Vec<usize> = (0..SLAB_CAPACITY)
            .map(|i| slab.insert(0x1000 + i as rb_sys::VALUE).expect("capacity"))
            .collect();
        assert_eq!(slab.occupied(), SLAB_CAPACITY);
        assert!(slab.insert(0x9999).is_none());

        slab.release(indexes[7]);
        assert_eq!(slab.insert(0x9999), Some(indexes[7]));
    }
}
