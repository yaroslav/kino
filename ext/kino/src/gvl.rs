//! Releasing the GVL (per-ractor VM lock) around blocking Rust calls.
//!
//! magnus does not wrap `rb_thread_call_without_gvl`, so this is the one
//! module that talks to rb-sys directly. Everything blocking (queue pops,
//! body reads/writes) must release the GVL through here, so other Ruby threads
//! in the same ractor keep running and the VM can interrupt us via the UBF.
//! Every release is rb_thread_call_without_gvl2, which checks no interrupts: Kino runs them in
//! protected calls (released, park), so a raise comes back as Err, never jumping over Rust frames.
//! The one other GVL hand-over, `pass`, is rb_thread_schedule in a protected call too.

use std::any::Any;
use std::ffi::c_void;
use std::marker::PhantomData;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};

use magnus::{Error, Ruby};

/// An unblock function: called by the Ruby VM from another thread when it
/// needs to interrupt the blocking region (Thread#kill, VM shutdown, ...).
/// Implementations must be async-signal-safe in spirit: no locks, no Ruby
/// API. Its flag stays borrowed for as long as the release it is handed to runs.
#[derive(Clone, Copy)]
pub struct Ubf<'a> {
    func: unsafe extern "C" fn(*mut c_void),
    data: *mut c_void,
    flag: PhantomData<&'a AtomicBool>,
}

impl<'a> Ubf<'a> {
    /// Raises `flag` (a slot's, or a wait's own), which the blocked region polls between waits.
    pub fn interrupt(flag: &'a AtomicBool) -> Self {
        Ubf {
            func: ubf_interrupt,
            data: flag as *const AtomicBool as *mut c_void,
            flag: PhantomData,
        }
    }
}

unsafe extern "C" fn ubf_interrupt(data: *mut c_void) {
    // SAFETY: `data` is the flag of a Ubf::interrupt, whose borrow
    // outlives the release the VM calls this from.
    let flag = unsafe { &*(data as *const AtomicBool) };
    flag.store(true, Ordering::SeqCst);
}

/// What a release runs, then what it returned or the panic it raised. A release that did not
/// run it leaves it there.
type Slot<F, R> = (Option<F>, Option<std::thread::Result<R>>);

unsafe extern "C" fn trampoline<F, R>(arg: *mut c_void) -> *mut c_void
where
    F: FnOnce() -> R,
{
    // SAFETY: `arg` is the Slot<F, R> that `call` hands its release,
    // borrowed by nothing else until the release returns.
    let slot = unsafe { &mut *(arg as *mut Slot<F, R>) };
    let f = slot.0.take().expect("a release ran its work twice");
    // A panic must not unwind into the VM's C frame beneath us (that
    // aborts the process); catch it here and let the caller rethrow it
    // as a Ruby exception once the GVL is held again.
    slot.1 = Some(catch_unwind(AssertUnwindSafe(f)));
    std::ptr::null_mut()
}

/// Run what `slot` holds with the GVL released, unless an interrupt is pending (without_gvl2).
/// Always inlined, so each release stays a direct call, its slot on the caller's own frame.
#[inline(always)]
// The crate's one GVL release (clippy.toml bars the others).
#[allow(clippy::disallowed_methods)]
fn call<F, R>(slot: &mut Slot<F, R>, ubf: Option<Ubf<'_>>)
where
    F: FnOnce() -> R,
{
    let (ubf_func, ubf_data) = match ubf {
        Some(u) => (Some(u.func), u.data),
        None => (None, std::ptr::null_mut()),
    };
    // SAFETY: `slot` is borrowed for as long as the release runs, which runs `trampoline::<F, R>`
    // on it at most once. The VM calls the unblock function only while the release runs, which
    // the Ubf's borrow of its data outlives.
    unsafe {
        rb_sys::rb_thread_call_without_gvl2(
            Some(trampoline::<F, R>),
            slot as *mut Slot<F, R> as *mut c_void,
            ubf_func,
            ubf_data,
        );
    }
}

/// The caught panic payload as a plain RuntimeError, built with the GVL
/// held. Deliberately not magnus's own panic conversion: that raises
/// `fatal`, which still ends the process; a RuntimeError flows through
/// the worker's normal rescue path (500, on_error, error log, respawn).
fn panic_error(payload: Box<dyn Any + Send>) -> Error {
    let msg = if let Some(m) = payload.downcast_ref::<&'static str>() {
        (*m).to_string()
    } else if let Some(m) = payload.downcast_ref::<String>() {
        m.clone()
    } else {
        "opaque panic payload".to_string()
    };
    let ruby = Ruby::get().expect("a release runs on a Ruby thread");
    Error::new(
        ruby.exception_runtime_error(),
        format!("Kino: panic in native blocking call: {msg}"),
    )
}

/// Run `f` with the GVL released. Blocks the current Ruby thread but lets
/// every other Ruby thread (in this ractor) run in parallel.
/// Pending interrupts run before and after `f` in protected calls: a raise comes back as Err.
///
/// `f` MUST NOT touch any Ruby API. The VM's unblock function raises `woken`, if any: `f` is
/// responsible for noticing and returning promptly.
///
/// A panic in `f` comes back as `Err` (RuntimeError with the panic
/// message); the default panic hook still reports it to stderr first.
pub fn released<R>(f: impl FnOnce() -> R, woken: Option<&AtomicBool>) -> Result<R, Error> {
    let unread = AtomicBool::new(false);
    released_by(&mut Released, woken.unwrap_or(&unread), f)
}

/// released, where `parker` waits: tests stand in for the VM.
pub(crate) fn released_by<P: Parker, R>(
    parker: &mut P,
    woken: &AtomicBool,
    f: impl FnOnce() -> R,
) -> Result<R, P::Error> {
    let mut f = Some(f);
    loop {
        let ran = parker.release(woken, || f.take().map(|f| f()));
        parker.check_ints()?;
        if let Some(ran) = ran?.flatten() {
            return Ok(ran);
        }
    }
}

/// Run `f` with the GVL released, or held while an interrupt is pending: `f` always runs, and
/// no interrupt runs here (for paths that cannot pass an error on); a panic comes back as Err.
pub fn released_or_held<R>(f: impl FnOnce() -> R) -> Result<R, Error> {
    let (unread, mut f) = (AtomicBool::new(false), Some(f));
    let ran = Released
        .release(&unread, || f.take().map(|f| f()))?
        .flatten();
    match f {
        Some(f) => catch_unwind(AssertUnwindSafe(f)).map_err(panic_error),
        None => Ok(ran.expect("a release that took its work ran it")),
    }
}

/// Thread.pass: the threads waiting for this GVL run first, and this one returns once it holds it
/// again, at once when none waits. Its interrupt check runs in a protected call: a raise comes
/// back as Err.
// rb_thread_schedule is barred for its unprotected interrupt check, which this protects.
#[allow(clippy::disallowed_methods)]
pub fn pass() -> Result<(), Error> {
    magnus::rb_sys::protect(|| {
        // SAFETY: called on a Ruby thread holding its GVL, as every caller of this crate's
        // natives is.
        unsafe { rb_sys::rb_thread_schedule() };
        rb_sys::Qnil as rb_sys::VALUE
    })?;
    Ok(())
}

/// How a worker's thread waits: Released, or in place in tests (no VM).
pub(crate) trait Parker {
    type Error;

    /// Runs `f` (no Ruby inside), unblocked by raising `woken`; None while an interrupt is pending.
    fn release<R>(
        &mut self,
        woken: &AtomicBool,
        f: impl FnOnce() -> R,
    ) -> Result<Option<R>, Self::Error>;

    /// Runs the thread's pending VM interrupts; Err when one raised.
    fn check_ints(&mut self) -> Result<(), Self::Error>;
}

/// The GVL released with no interrupt check of the release's own (nothing raises past the
/// caller's frames; a panic in `f` comes back as Err); check_ints runs them in a protected call.
pub(crate) struct Released;

impl Parker for Released {
    type Error = Error;

    fn release<R>(
        &mut self,
        woken: &AtomicBool,
        f: impl FnOnce() -> R,
    ) -> Result<Option<R>, Error> {
        let mut slot = (Some(f), None);
        let ubf = Some(Ubf::interrupt(woken));
        call(&mut slot, ubf);
        slot.1.map(|ran| ran.map_err(panic_error)).transpose()
    }

    fn check_ints(&mut self) -> Result<(), Error> {
        Ruby::get()
            .expect("a release runs on a Ruby thread")
            .thread_check_ints()
    }
}

/// The crate's blocking idiom (a worker's next item, a request body, room for a chunk):
/// `attempt` runs where `parker` waits until Some, bounding each wait and never touching Ruby.
/// Ok(None) once Kino stopped the wait (`stop`: WorkerSlot::halt). The VM's unblock
/// function raises a flag of the wait's own, so a wake for one thread never ends another's wait;
/// the wait runs the pending interrupts and waits again, or returns Err if one raised. What a
/// wait found as it fired goes to `put_back`, lest a pending Thread#raise beat it to Ruby.
pub(crate) fn park<T, P: Parker>(
    parker: &mut P,
    stop: &AtomicBool,
    mut attempt: impl FnMut() -> Option<T>,
    mut put_back: impl FnMut(T) -> Back<T>,
) -> Result<Option<T>, P::Error> {
    let woken = AtomicBool::new(false);
    loop {
        // `woken` is read only once an attempt waited a tick: one that finds something at once
        // pays no load for it.
        let mut wait = || loop {
            if stop.load(Ordering::SeqCst) {
                return None;
            }
            if let Some(found) = attempt() {
                return Some(found);
            }
            if woken.load(Ordering::Relaxed) {
                return None;
            }
        };
        if let Some(found) = parker.release(&woken, &mut wait)?.flatten() {
            // Whether the unblock function fired while the wait found something.
            if !woken.load(Ordering::Relaxed) {
                return Ok(Some(found));
            }
            match put_back(found) {
                Back::Waits => {}
                Back::HandedOver(found) => return Ok(Some(found)),
                Back::Held(found) => {
                    parker.check_ints()?;
                    return Ok(Some(found));
                }
            }
        }
        woken.store(false, Ordering::Relaxed);
        // Nothing found, or it went back. A stop ends every later wait on the slot too.
        if stop.load(Ordering::SeqCst) {
            return Ok(None);
        }
        parker.check_ints()?;
    }
}

/// What put_back made of what a wait found as the unblock function fired.
pub enum Back<T> {
    /// Back where another take finds it; the wait goes on once the interrupts ran.
    Waits,
    /// Held while the interrupts run, then the caller's; a raise drops it (its Drop answers).
    Held(T),
    /// The caller's as found; the interrupts fire at Ruby's next check.
    HandedOver(T),
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    use parking_lot::Mutex;

    use super::released_by;
    use crate::testing::InPlace;

    /// The VM's pending interrupts, as the test sets them: none, a wake
    /// (Thread#wakeup, which only ends the wait), or a raise.
    #[derive(Clone, Copy, PartialEq)]
    enum Pending {
        Nothing,
        Wake,
        Raise,
    }

    /// Runs what `pending` holds, as rb_thread_check_ints would, and
    /// takes it, counting the runs.
    fn interrupts<'a>(
        pending: &'a Mutex<Pending>,
        flag: &'a AtomicBool,
        runs: &'a AtomicUsize,
    ) -> impl FnMut() -> Result<(), &'static str> + 'a {
        move || {
            runs.fetch_add(1, Ordering::SeqCst);
            flag.store(false, Ordering::SeqCst);
            match std::mem::replace(&mut *pending.lock(), Pending::Nothing) {
                Pending::Raise => Err("Thread#raise"),
                Pending::Wake | Pending::Nothing => Ok(()),
            }
        }
    }

    #[test]
    fn a_raise_comes_back_with_the_work_unrun_or_its_result_dropped_and_a_wake_runs_it_once() {
        // What the VM has pending as the release would start, and whether
        // a raise comes as the work runs.
        let rows = [
            (Pending::Raise, false),
            (Pending::Wake, false),
            (Pending::Nothing, true),
        ];
        for (before, during) in rows {
            let held = Arc::new(());
            let (pending, flag, runs) = (
                Mutex::new(before),
                AtomicBool::new(before != Pending::Nothing),
                AtomicUsize::new(0),
            );
            let ran_work = AtomicUsize::new(0);
            let work = {
                let (held, pending, ran_work) = (held.clone(), &pending, &ran_work);
                move || {
                    ran_work.fetch_add(1, Ordering::SeqCst);
                    if during {
                        *pending.lock() = Pending::Raise;
                    }
                    held
                }
            };

            // A release as rb_thread_call_without_gvl2 makes it, in place: with an interrupt
            // pending (the flag) it leaves the work where it is; else it runs it.
            let mut parker = InPlace {
                release: |_: &AtomicBool, run: &mut dyn FnMut()| {
                    if !flag.load(Ordering::SeqCst) {
                        run();
                    }
                },
                interrupts: interrupts(&pending, &flag, &runs),
            };
            let result = released_by(&mut parker, &AtomicBool::new(false), work);
            let (raised, ran) = (matches!(result, Err("Thread#raise")), result.is_ok());
            drop(result);
            let (work_runs, checks) =
                (ran_work.load(Ordering::SeqCst), runs.load(Ordering::SeqCst));
            match before {
                // The interrupts run before the work and after it.
                Pending::Wake => assert!(ran && work_runs == 1 && checks == 2, "a wake"),
                Pending::Raise => assert!(raised && work_runs == 0, "a raise pending"),
                Pending::Nothing => assert!(raised && work_runs == 1, "a raise as it ran"),
            }
            assert_eq!(
                Arc::strong_count(&held),
                1,
                "what the work held, or returned, outlived it"
            );
        }
    }
}
