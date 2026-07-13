//! Test-only natives proving the Phase 0 primitives: blocking channel takes
//! under `without_gvl`, UBF interruptibility, and ractor-safe method calls.
//! Exposed as `Kino::Native::_test_*`; not part of the public API.

use std::collections::HashMap;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::OnceLock;

use parking_lot::Mutex;

use crate::gvl;

struct TestChannel {
    tx: Mutex<Option<flume::Sender<String>>>,
    rx: flume::Receiver<String>,
    interrupted: AtomicBool,
}

static CHANNELS: OnceLock<Mutex<HashMap<u64, &'static TestChannel>>> = OnceLock::new();
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn channels() -> &'static Mutex<HashMap<u64, &'static TestChannel>> {
    CHANNELS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn lookup(ruby: &magnus::Ruby, id: u64) -> Result<&'static TestChannel, magnus::Error> {
    channels().lock().get(&id).copied().ok_or_else(|| {
        magnus::Error::new(
            ruby.exception_arg_error(),
            format!("unknown test channel {id}"),
        )
    })
}

pub fn create(depth: usize) -> u64 {
    let (tx, rx) = flume::bounded(depth);
    let chan = Box::leak(Box::new(TestChannel {
        tx: Mutex::new(Some(tx)),
        rx,
        interrupted: AtomicBool::new(false),
    }));
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    channels().lock().insert(id, chan);
    id
}

pub fn push(ruby: &magnus::Ruby, id: u64, value: String) -> Result<(), magnus::Error> {
    let chan = lookup(ruby, id)?;
    let guard = chan.tx.lock();
    let Some(tx) = guard.as_ref() else {
        return Err(magnus::Error::new(
            ruby.exception_runtime_error(),
            "test channel is closed",
        ));
    };
    tx.try_send(value).map_err(|e| {
        magnus::Error::new(ruby.exception_runtime_error(), format!("push failed: {e}"))
    })
}

/// Blocking take, GVL released, interruptible. nil = closed or interrupted
/// (the VM delivers any pending interrupt right after we return to Ruby).
pub fn take(ruby: &magnus::Ruby, id: u64) -> Result<Option<String>, magnus::Error> {
    let chan = lookup(ruby, id)?;
    chan.interrupted.store(false, Ordering::SeqCst);

    let taken = gvl::interruptible(&chan.interrupted, || {
        match chan.rx.recv_timeout(crate::queue::TICK) {
            Ok(v) => Some(Some(v)),
            Err(flume::RecvTimeoutError::Timeout) => None,
            Err(flume::RecvTimeoutError::Disconnected) => Some(None),
        }
    })?;
    Ok(taken.flatten())
}

/// Panic inside a GVL-released block: proves it surfaces as a Ruby
/// exception instead of unwinding into the VM and killing the process.
/// The default panic hook is silenced around the intentional panic so
/// spec output stays clean.
pub fn panic_in_release() -> Result<(), magnus::Error> {
    let hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let result = gvl::without_gvl(|| panic!("intentional test panic"), None);
    std::panic::set_hook(hook);
    result
}

pub fn close(ruby: &magnus::Ruby, id: u64) -> Result<(), magnus::Error> {
    let chan = lookup(ruby, id)?;
    chan.tx.lock().take();
    Ok(())
}
