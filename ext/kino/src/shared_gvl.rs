//! A GVL that worker threads share (every worker in :threaded mode, a Ractor's `threads` past
//! one). A worker that keeps finding work never waits, so it keeps the GVL until Ruby's timer
//! preempts it, 100 ms on and most often mid-request. Under a backlog every worker is in line for
//! the GVL, and that request then waits for each other worker's 100 ms: over two seconds with 24
//! threads. A request the queue hands a parked worker waits as long for the running one. So
//! between two items a worker that has held the GVL for HOLD_US passes it, and a request waits
//! about that long for each worker ahead of it (doc/architecture.md).
//!
//! A classic take judges the hold at its last item's start, so it passes up to an item late, and
//! a batch (`batch` above one) passes only before it. A worker_loop's hold runs from its last
//! pass: its scheduler's waits are its own, unseen here, so after one it may pass at once and wait
//! a round of the line.

use magnus::Error;

use crate::registry::ServerInner;
use crate::slot::WorkerSlot;

/// Each pass costs the switches to the threads waiting; 2 or 5 ms cost more throughput under a
/// backlog, not less, for a longer wait.
const HOLD_US: u64 = 1_000;

/// A classic worker got the GVL back from a park.
pub fn got_back(server: &ServerInner, slot: &WorkerSlot) {
    if server.config.shared_gvl {
        slot.park.got_gvl(crate::mono::mono_us());
    }
}

/// Whether a worker sharing its GVL has held it for HOLD_US by `now` (mono µs), read only for such
/// a worker.
pub fn held_long(server: &ServerInner, slot: &WorkerSlot, now: impl FnOnce() -> u64) -> bool {
    server.config.shared_gvl && now().saturating_sub(slot.park.held_gvl_since()) >= HOLD_US
}

/// gvl::pass, after which the hold starts over, whether a raise came back from it or not.
pub fn pass(slot: &WorkerSlot) -> Result<(), Error> {
    let passed = crate::gvl::pass();
    slot.park.got_gvl(crate::mono::mono_us());
    passed
}

/// A worker_loop's take, or an inline connection's next request, held long by a clock read: a
/// loop's busy stamps run ahead of the clock (Load::waits_on_doorbell).
pub fn pass_in_loop(server: &ServerInner, slot: &WorkerSlot) -> Result<(), Error> {
    if held_long(server, slot, crate::mono::mono_us) {
        pass(slot)?;
    }
    Ok(())
}
