//! The natives Ruby calls on a server: the pool's and the drain's steps, and its counters. All
//! but those that raise for a caller bug treat a removed server as a no-op: shutdown is idempotent.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use magnus::{Error, Ruby};

use crate::registry::{self, Counters};

pub fn register_worker(ruby: &Ruby, server_id: u64) -> Result<usize, Error> {
    registry::get(ruby, server_id)?
        .register_worker()
        .map_err(|e| super::io_error(ruby, "worker doorbell failed", e))
}

pub fn stop_accepting(server_id: u64) {
    if let Some(server) = registry::try_get(server_id) {
        server
            .state
            .store(registry::STATE_DRAINING, Ordering::Relaxed);
        let _ = server.shutdown_tx.send(true);
    }
}

/// Ruby reports the pool up; /ready answers 200.
pub fn control_ready(server_id: u64) {
    if let Some(server) = registry::try_get(server_id) {
        server.state.store(registry::STATE_READY, Ordering::Relaxed);
    }
}

/// The drain is over: queues close, and switched connections, so an app serving one sees EOF.
pub fn close_queue(server_id: u64) {
    if let Some(server) = registry::try_get(server_id) {
        // Bridges first: a worker leaving for its closed queue may drop a long poll, whose bridge
        // must find the drain over.
        crate::hijack::close(&server, crate::hijack::Closed::Switched);
        server.close_queues();
    }
}

/// Each bridge closes in well under a millisecond; the bound is for clients that stopped reading.
const BRIDGES_WIND_DOWN: Duration = Duration::from_millis(100);

/// The workers are gone: close every bridge, then wait without the GVL until each ended and
/// hyper took its answer, so teardown cuts none, or BRIDGES_WIND_DOWN passed.
pub fn close_bridges(_ruby: &Ruby, server_id: u64) -> Result<(), Error> {
    if let Some(server) = registry::try_get(server_id) {
        crate::hijack::close(&server, crate::hijack::Closed::All);
        let running = server.bridges_running.clone();
        crate::gvl::without_gvl(
            move || {
                let deadline = std::time::Instant::now() + BRIDGES_WIND_DOWN;
                while running.load(Ordering::SeqCst) > 0 && std::time::Instant::now() < deadline {
                    std::thread::sleep(Duration::from_millis(1));
                }
            },
            None,
        )?;
    }
    Ok(())
}

/// What a drain waits on: [queued, in_flight, sockets still owed their on_close].
pub fn queue_stats(server_id: u64) -> (usize, usize, usize) {
    registry::try_get(server_id).map_or((0, 0, 0), |server| {
        let in_flight = server.counters.in_flight.load(Ordering::Relaxed);
        (server.queued(), in_flight, server.open_sockets())
    })
}

/// The worker on this slot crashed (WorkerSlot::crash).
pub fn abort_inflight(ruby: &Ruby, server_id: u64, worker_id: usize) -> Result<(), Error> {
    if let Some(server) = registry::try_get(server_id) {
        server.slot(ruby, worker_id)?.crash();
    }
    Ok(())
}

/// The drain's deadline passed: answer every request still in flight.
pub fn abort_all_inflight(server_id: u64) {
    if let Some(server) = registry::try_get(server_id) {
        for slot in server.slots.read().iter() {
            slot.abort_held();
            // Its sockets stop waiting on their handlers too.
            crate::ws::abandon(slot);
        }
    }
}

pub fn interrupt_all_workers(server_id: u64) {
    if let Some(server) = registry::try_get(server_id) {
        for slot in server.slots.read().iter() {
            slot.interrupt();
        }
    }
}

pub fn shutdown_runtime(server_id: u64, timeout_ms: u64) {
    if let Some(server) = registry::remove(server_id) {
        let ticker = server.ticker.lock().take();
        if let Some(ticker) = ticker {
            ticker.stop();
        }
        let _ = server.shutdown_tx.send(true);
        std::mem::take(&mut *server.runtime.lock()).shutdown(Duration::from_millis(timeout_ms));
        // The listener is closed with the runtime; its socket file is not.
        if let Some(path) = &server.config.unix_path {
            crate::listen::cleanup_unix(path);
        }
    }
}

/// WorkerSlot::quarantine_at, for the quarantine monitor.
pub fn quarantine_slot(
    ruby: &Ruby,
    server_id: u64,
    worker_id: usize,
    generation: u64,
) -> Result<bool, Error> {
    match registry::try_get(server_id) {
        Some(server) => Ok(server.slot(ruby, worker_id)?.quarantine_at(generation)),
        None => Ok(false),
    }
}

/// WorkerSlot::retire_at, for the pool scaler. An unknown slot id is a caller bug: raise.
pub fn retire_slot(
    ruby: &Ruby,
    server_id: u64,
    worker_id: usize,
    generation: u64,
) -> Result<bool, Error> {
    match registry::try_get(server_id) {
        Some(server) => Ok(server.slot(ruby, worker_id)?.retire_at(generation)),
        None => Ok(false),
    }
}

/// WorkerSlot::recycle: the new generation, or nil while the slot is not quiet.
pub fn reset_slot(ruby: &Ruby, server_id: u64, worker_id: usize) -> Result<Option<u64>, Error> {
    match registry::try_get(server_id) {
        Some(server) => Ok(server.slot(ruby, worker_id)?.recycle()),
        None => Ok(None),
    }
}

/// ws::leave_all, spread as ws::close_spread allows with `left` seconds of the deadline.
pub fn ws_leave_all(server_id: u64, left: f64) -> usize {
    let spread = crate::ws::close_spread(left);
    registry::try_get(server_id).map_or(0, |server| crate::ws::leave_all(&server, spread))
}

pub fn record_respawn(server_id: u64) {
    record(server_id, |counters| &counters.respawns)
}

pub fn record_scale_up(server_id: u64) {
    record(server_id, |counters| &counters.scale_ups)
}

pub fn record_scale_down(server_id: u64) {
    record(server_id, |counters| &counters.scale_downs)
}

pub fn record_quarantine_replacement(server_id: u64) {
    record(server_id, |counters| &counters.quarantine_replacements)
}

fn record(server_id: u64, counter: fn(&Counters) -> &AtomicU64) {
    if let Some(server) = registry::try_get(server_id) {
        counter(&server.counters).fetch_add(1, Ordering::Relaxed);
    }
}

/// The Ruby Server holds it for life, so pinned buffers survive worker-Ractor crashes.
pub fn pin_keeper(
    ruby: &Ruby,
    server_id: u64,
) -> Result<magnus::typed_data::Obj<crate::pin::PinKeeper>, Error> {
    let server = registry::get(ruby, server_id)?;
    Ok(ruby.obj_wrap(crate::pin::PinKeeper(server.pin_slab.clone())))
}

/// Server#stats: every stat /stats reports but state and version, in order; nil once gone.
pub fn stats(ruby: &Ruby, server_id: u64) -> Result<Option<magnus::RHash>, Error> {
    let Some(server) = registry::try_get(server_id) else {
        return Ok(None);
    };
    crate::stats::Snapshot::take(&server).hash(ruby).map(Some)
}

/// [index, served, in_flight, busy_ms, quarantined, retired, sockets, hijacked, generation].
pub type WorkerStatRow = (usize, u64, usize, u64, bool, bool, usize, usize, u64);

/// Every slot's row, for the pool scaler, the quarantine monitor and the
/// slot bank; empty once gone.
pub fn worker_stats(server_id: u64) -> Vec<WorkerStatRow> {
    let Some(server) = registry::try_get(server_id) else {
        return Vec::new();
    };
    crate::stats::collect_worker_status(&server)
        .into_iter()
        .map(|w| {
            (
                w.index,
                w.served,
                w.in_flight,
                w.busy_ms,
                w.quarantined,
                w.retired,
                w.sockets,
                w.hijacked,
                w.generation,
            )
        })
        .collect()
}

pub fn set_active_workers(server_id: u64, count: usize) {
    if let Some(server) = registry::try_get(server_id) {
        let counters = &server.counters;
        counters.active_workers.store(count, Ordering::Relaxed);
    }
}
