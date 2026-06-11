//! Releasing the GVL (per-ractor VM lock) around blocking Rust calls.
//!
//! magnus does not wrap `rb_thread_call_without_gvl`, so this is the one
//! module that talks to rb-sys directly. Everything blocking (queue pops,
//! body reads/writes) must go through `without_gvl` so other Ruby threads
//! in the same ractor keep running and the VM can interrupt us via the UBF.

use std::ffi::c_void;

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
    let slot = unsafe { &mut *(arg as *mut (Option<F>, Option<R>)) };
    let f = slot.0.take().expect("without_gvl trampoline called twice");
    slot.1 = Some(f());
    std::ptr::null_mut()
}

/// Run `f` with the GVL released. Blocks the current Ruby thread but lets
/// every other Ruby thread (in this ractor) run in parallel.
///
/// `f` MUST NOT touch any Ruby API. If `ubf` fires, `f` is responsible for
/// noticing (e.g. a message on an interrupt channel) and returning promptly;
/// pending VM interrupts are then delivered once we're back in Ruby.
pub fn without_gvl<F, R>(f: F, ubf: Option<Ubf>) -> R
where
    F: FnOnce() -> R,
{
    let mut slot: (Option<F>, Option<R>) = (Some(f), None);
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
    slot.1.expect("without_gvl block did not run")
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
/// interrupt_all_workers). Returns None on interruption. `attempt` must
/// bound each wait (recv_timeout-style) and must not touch the Ruby API.
pub fn interruptible<T>(
    flag: &std::sync::atomic::AtomicBool,
    mut attempt: impl FnMut() -> Option<T>,
) -> Option<T> {
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
