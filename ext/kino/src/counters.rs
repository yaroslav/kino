//! Counts a guard holds while what they count lives, and process-wide counts by cause.

use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

/// A count held on an atomic counter while this lives, and given back as
/// it drops, however its holder ends: closed, cancelled at teardown, or
/// unwound. The counter is `owner` itself, or one of its fields.
pub(crate) struct Counted<O = AtomicUsize> {
    owner: Arc<O>,
    counter: fn(&O) -> &AtomicUsize,
    n: usize,
}

impl Counted {
    pub(crate) fn hold(counter: &Arc<AtomicUsize>, n: usize) -> Self {
        counter.fetch_add(n, Ordering::SeqCst);
        Self::adopt(counter.clone(), n)
    }

    /// Hold the `n` the caller already counted on `counter`.
    pub(crate) fn adopt(counter: Arc<AtomicUsize>, n: usize) -> Self {
        Counted {
            owner: counter,
            counter: |counter| counter,
            n,
        }
    }
}

impl<O> Counted<O> {
    pub(crate) fn on(owner: &Arc<O>, counter: fn(&O) -> &AtomicUsize) -> Self {
        counter(owner).fetch_add(1, Ordering::SeqCst);
        Counted {
            owner: owner.clone(),
            counter,
            n: 1,
        }
    }
    pub(crate) fn owner(&self) -> &Arc<O> {
        &self.owner
    }
}

impl<O> Drop for Counted<O> {
    fn drop(&mut self) {
        (self.counter)(&self.owner).fetch_sub(self.n, Ordering::SeqCst);
    }
}

/// An enum whose ALL lists each variant once, named for /stats, Server#stats and the
/// metrics' `cause` label; declared with `tallied!`.
pub(crate) trait Tallied<const N: usize>: Copy {
    const ALL: [Self; N];

    fn name(self) -> &'static str;

    fn index(self) -> usize;

    /// `counts`, indexed as ALL, by name.
    fn named(counts: [u64; N]) -> [(&'static str, u64); N] {
        Self::ALL.map(|cause| (cause.name(), counts[cause.index()]))
    }
}

/// An enum and its `Tallied` impl: each variant, in ALL's order, with
/// its name. The first may set where the discriminants start.
macro_rules! tallied {
    (
        $(#[$meta:meta])*
        $vis:vis enum $enum:ident {
            $(#[$first_doc:meta])* $first:ident $(= $start:literal)? => $first_name:literal,
            $($(#[$doc:meta])* $variant:ident => $name:literal,)*
        }
    ) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        $vis enum $enum {
            $(#[$first_doc])* $first $(= $start)?,
            $($(#[$doc])* $variant,)*
        }

        impl $crate::counters::Tallied<{ [$first_name $(, $name)*].len() }> for $enum {
            const ALL: [$enum; [$first_name $(, $name)*].len()] =
                [$enum::$first $(, $enum::$variant)*];

            fn name(self) -> &'static str {
                match self {
                    $enum::$first => $first_name,
                    $($enum::$variant => $name,)*
                }
            }

            fn index(self) -> usize {
                self as usize - Self::ALL[0] as usize
            }
        }
    };
}
pub(crate) use tallied;

/// Process-wide counts by cause (Relaxed, advisory).
pub(crate) struct Tally<K, const N: usize> {
    counts: [AtomicU64; N],
    by: PhantomData<fn(K)>,
}

impl<K: Tallied<N>, const N: usize> Tally<K, N> {
    pub(crate) const fn new() -> Self {
        Tally {
            counts: [const { AtomicU64::new(0) }; N],
            by: PhantomData,
        }
    }

    pub(crate) fn add(&self, cause: K) {
        self.add_n(cause, 1);
    }

    #[inline]
    pub(crate) fn add_n(&self, cause: K, n: u64) {
        self.counts[cause.index()].fetch_add(n, Ordering::Relaxed);
    }

    /// The counts now, indexed as K::ALL.
    pub(crate) fn read(&self) -> [u64; N] {
        self.counts
            .each_ref()
            .map(|count| count.load(Ordering::Relaxed))
    }
}
