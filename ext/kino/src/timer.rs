//! High-resolution sleep for worker code. MRI's own `sleep` parks the
//! thread on the VM timer, whose wakeups inside non-main ractors are
//! observably coarse (≈+2.5 ms on Linux). This path instead releases the
//! GVL and uses the OS timer directly (std::thread::sleep → nanosleep),
//! which is microsecond-accurate regardless of which ractor calls it.
//!
//! Only one bounded chunk is slept per call: the caller (Kino.sleep in
//! Ruby) loops, so pending VM interrupts are processed between chunks and
//! Thread#kill / shutdown stay responsive within one tick.

use std::ffi::c_void;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use magnus::{Error, Ruby};

use crate::gvl::{self, Ubf};

/// Sleep up to `seconds` (capped at one interrupt tick), GVL released.
/// Returns the seconds actually remaining after this chunk (0.0 = done),
/// so the Ruby loop knows when to stop.
pub fn sleep_chunk(ruby: &Ruby, seconds: f64) -> Result<f64, Error> {
    if !seconds.is_finite() || seconds < 0.0 {
        return Err(Error::new(
            ruby.exception_arg_error(),
            "sleep duration must be a non-negative number",
        ));
    }

    let requested = Duration::from_secs_f64(seconds);
    let chunk = requested.min(crate::queue::TICK);
    let deadline = Instant::now() + chunk;

    let interrupted = AtomicBool::new(false);
    gvl::without_gvl(
        || {
            // std::thread::sleep retries on EINTR, so the UBF can't cut a
            // sleep short: the flag is only observed once the chunk ends.
            // That bounds interrupt latency at one chunk, which is why the
            // chunk is capped at TICK above.
            while !interrupted.load(Ordering::Relaxed) {
                let now = Instant::now();
                if now >= deadline {
                    break;
                }
                std::thread::sleep(deadline - now);
            }
        },
        Some(Ubf {
            func: gvl::ubf_interrupt,
            data: &interrupted as *const _ as *mut c_void,
        }),
    );

    let remaining = requested.saturating_sub(chunk);
    Ok(remaining.as_secs_f64())
}
