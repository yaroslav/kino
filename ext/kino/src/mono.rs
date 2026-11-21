//! Process-monotonic microsecond clock, shared by the request hot path
//! (recording), the lane dispatcher and the control thread (reading busy
//! age). Independent of wall-clock and of Ruby, so it is identical in
//! :ractor and :threaded.

use std::sync::OnceLock;
use std::time::Instant;

static MONO_EPOCH: OnceLock<Instant> = OnceLock::new();

/// Microseconds since the first call anywhere in the process. The u128
/// micros fit u64 for any realistic uptime (u64 us is ~584,000 years).
pub fn mono_us() -> u64 {
    MONO_EPOCH.get_or_init(Instant::now).elapsed().as_micros() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mono_us_is_monotonic_nondecreasing() {
        let a = mono_us();
        let b = mono_us();
        assert!(b >= a, "clock went backwards: {a} then {b}");
    }
}
