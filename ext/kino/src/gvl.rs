//! Releasing the GVL (per-ractor VM lock) around blocking Rust calls.
//!
//! magnus does not wrap `rb_thread_call_without_gvl`, so this is the one
//! module that talks to rb-sys directly. Everything blocking (queue pops,
//! body reads/writes) must release the GVL through here, so other Ruby threads
//! in the same ractor keep running and the VM can interrupt us via the UBF.

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

/// What a release runs, then what it returned or the panic it raised.
type Slot<F, R> = (Option<F>, Option<std::thread::Result<R>>);

unsafe extern "C" fn trampoline<F, R>(arg: *mut c_void) -> *mut c_void
where
    F: FnOnce() -> R,
{
    // SAFETY: `arg` is the Slot<F, R> that `call` hands its release,
    // borrowed by nothing else until the release returns.
    let slot = unsafe { &mut *(arg as *mut Slot<F, R>) };
    let f = slot.0.take().expect("without_gvl trampoline called twice");
    // A panic must not unwind into the VM's C frame beneath us (that
    // aborts the process); catch it here and let the caller rethrow it
    // as a Ruby exception once the GVL is held again.
    slot.1 = Some(catch_unwind(AssertUnwindSafe(f)));
    std::ptr::null_mut()
}

/// rb_thread_call_without_gvl (interrupt checks around `f`) or _gvl2 (none; skips a pending one).
type Release = unsafe extern "C" fn(
    Option<unsafe extern "C" fn(*mut c_void) -> *mut c_void>,
    *mut c_void,
    rb_sys::rb_unblock_function_t,
    *mut c_void,
) -> *mut c_void;

/// Always inlined, so each release stays a direct call, its slot on the caller's own frame.
#[inline(always)]
fn call<F, R>(release: Release, slot: &mut Slot<F, R>, ubf: Option<Ubf<'_>>)
where
    F: FnOnce() -> R,
{
    let (ubf_func, ubf_data) = match ubf {
        Some(u) => (Some(u.func), u.data),
        None => (None, std::ptr::null_mut()),
    };
    // SAFETY: `slot` is borrowed for as long as `release` runs, which runs
    // `trampoline::<F, R>` on it at most once. The VM calls the unblock
    // function only while `release` runs, which the Ubf's borrow of its
    // data outlives.
    unsafe {
        release(
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
    let ruby = Ruby::get().expect("without_gvl requires a Ruby thread");
    Error::new(
        ruby.exception_runtime_error(),
        format!("Kino: panic in native blocking call: {msg}"),
    )
}

/// Run `f` with the GVL released. Blocks the current Ruby thread but lets
/// every other Ruby thread (in this ractor) run in parallel.
///
/// `f` MUST NOT touch any Ruby API. If `ubf` fires, `f` is responsible for
/// noticing (e.g. a message on an interrupt channel) and returning promptly;
/// pending VM interrupts are then delivered once we're back in Ruby.
///
/// A panic in `f` comes back as `Err` (RuntimeError with the panic
/// message); the default panic hook still reports it to stderr first.
pub fn without_gvl<F, R>(f: F, ubf: Option<Ubf<'_>>) -> Result<R, Error>
where
    F: FnOnce() -> R,
{
    let mut slot = (Some(f), None);
    call(rb_sys::rb_thread_call_without_gvl, &mut slot, ubf);
    match slot.1.expect("without_gvl block did not run") {
        Ok(value) => Ok(value),
        Err(payload) => Err(panic_error(payload)),
    }
}

/// Release the GVL and run `attempt` until it returns Some, or Ok(None) once `flag` rises. A
/// pending VM interrupt raises past the caller's frames afterwards, so only the test natives use
/// this: what Ruby can interrupt mid-request parks.
pub fn interruptible<T>(
    flag: &AtomicBool,
    mut attempt: impl FnMut() -> Option<T>,
) -> Result<Option<T>, Error> {
    without_gvl(
        || loop {
            if flag.load(Ordering::SeqCst) {
                return None;
            }
            if let Some(result) = attempt() {
                return Some(result);
            }
        },
        Some(Ubf::interrupt(flag)),
    )
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
        call(rb_sys::rb_thread_call_without_gvl2, &mut slot, ubf);
        slot.1.map(|ran| ran.map_err(panic_error)).transpose()
    }

    fn check_ints(&mut self) -> Result<(), Error> {
        Ruby::get()
            .expect("park requires a Ruby thread")
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
        // costs what it did under interruptible.
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
    use std::sync::atomic::{AtomicBool, Ordering};

    #[test]
    fn ubf_sets_the_interrupt_flag() {
        let flag = AtomicBool::new(false);
        let ubf = super::Ubf::interrupt(&flag);
        // SAFETY: as the VM calls it, while the flag is borrowed.
        unsafe { (ubf.func)(ubf.data) };
        assert!(flag.load(Ordering::SeqCst));
    }
}
