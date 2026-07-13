//! Releasing the GVL (per-ractor VM lock) around blocking Rust calls.
//!
//! magnus does not wrap `rb_thread_call_without_gvl`, so this is the one
//! module that talks to rb-sys directly. Everything blocking (queue pops,
//! body reads/writes) must go through `without_gvl` so other Ruby threads
//! in the same ractor keep running and the VM can interrupt us via the UBF.

use std::any::Any;
use std::ffi::c_void;
use std::panic::{catch_unwind, AssertUnwindSafe};

use magnus::{Error, Ruby};

/// An unblock function: called by the Ruby VM from another thread when it
/// needs to interrupt the blocking region (Thread#kill, VM shutdown, ...).
/// Implementations must be async-signal-safe in spirit: no locks, no Ruby
/// API. A single lock-free channel send is the intended use.
pub struct Ubf {
    pub func: unsafe extern "C" fn(*mut c_void),
    pub data: *mut c_void,
}

unsafe extern "C" fn trampoline<F, R>(arg: *mut c_void) -> *mut c_void
where
    F: FnOnce() -> R,
{
    let slot = unsafe { &mut *(arg as *mut (Option<F>, Option<std::thread::Result<R>>)) };
    let f = slot.0.take().expect("without_gvl trampoline called twice");
    // A panic must not unwind into the VM's C frame beneath us (that
    // aborts the process); catch it here and let without_gvl rethrow it
    // as a Ruby exception once the GVL is held again.
    slot.1 = Some(catch_unwind(AssertUnwindSafe(f)));
    std::ptr::null_mut()
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
pub fn without_gvl<F, R>(f: F, ubf: Option<Ubf>) -> Result<R, Error>
where
    F: FnOnce() -> R,
{
    let mut slot: (Option<F>, Option<std::thread::Result<R>>) = (Some(f), None);
    let (ubf_func, ubf_data) = match ubf {
        Some(u) => (Some(u.func), u.data),
        None => (None, std::ptr::null_mut()),
    };
    unsafe {
        rb_sys::rb_thread_call_without_gvl(
            Some(trampoline::<F, R>),
            &mut slot as *mut _ as *mut c_void,
            ubf_func,
            ubf_data,
        );
    }
    match slot.1.expect("without_gvl block did not run") {
        Ok(value) => Ok(value),
        Err(payload) => Err(panic_error(payload)),
    }
}

/// The standard UBF used across this crate: `data` points at an `AtomicBool`
/// owned by a `WorkerSlot` kept alive by the caller's stack frame (an Arc
/// clone held across the blocking call). The blocked region polls the flag
/// between bounded waits, so a store here unblocks it within one tick.
pub unsafe extern "C" fn ubf_interrupt(data: *mut c_void) {
    let flag = unsafe { &*(data as *const std::sync::atomic::AtomicBool) };
    flag.store(true, std::sync::atomic::Ordering::SeqCst);
}

/// The crate's one blocking idiom: release the GVL and run `attempt` in a
/// loop (`Some(T)` finishes, `None` means "tick elapsed, go around"), and
/// wake within a tick when `flag` is raised (by the VM's UBF or by
/// interrupt_all_workers). Returns Ok(None) on interruption, Err when
/// `attempt` panicked. `attempt` must bound each wait (recv_timeout-style)
/// and must not touch the Ruby API.
pub fn interruptible<T>(
    flag: &std::sync::atomic::AtomicBool,
    mut attempt: impl FnMut() -> Option<T>,
) -> Result<Option<T>, Error> {
    use std::sync::atomic::Ordering;

    without_gvl(
        || loop {
            if flag.load(Ordering::SeqCst) {
                return None;
            }
            if let Some(result) = attempt() {
                return Some(result);
            }
        },
        Some(Ubf {
            func: ubf_interrupt,
            data: flag as *const _ as *mut c_void,
        }),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    #[test]
    fn ubf_sets_the_interrupt_flag() {
        let flag = AtomicBool::new(false);
        unsafe {
            super::ubf_interrupt(&flag as *const _ as *mut std::ffi::c_void);
        }
        assert!(flag.load(Ordering::SeqCst));
    }
}
