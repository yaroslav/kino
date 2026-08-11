//! Process-monotonic millisecond clock, shared by the request hot path
//! (recording) and the control thread (reading busy age). Independent of
//! wall-clock and of Ruby, so it is identical in :ractor and :threaded.

use std::sync::OnceLock;
use std::time::Instant;

static MONO_EPOCH: OnceLock<Instant> = OnceLock::new();

/// Milliseconds since the first call anywhere in the process. The u128
/// millis fit u64 for any realistic uptime (u64 ms is ~584 million years).
pub fn mono_ms() -> u64 {
    MONO_EPOCH.get_or_init(Instant::now).elapsed().as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mono_ms_is_monotonic_nondecreasing() {
        let a = mono_ms();
        let b = mono_ms();
        assert!(b >= a, "clock went backwards: {a} then {b}");
    }
}
