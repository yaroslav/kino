//! The worker-side bridge: batched request intake and the fused
//! respond-and-take call. This is where FFI crossings per request went
//! from three in early designs (take, env, respond) to amortized ~one.
//!
//! Blocking discipline (everywhere in this crate): bounded `recv_timeout`
//! ticks + an AtomicBool interrupt flag. No flume::Selector: it loses
//! wakeups under churn, observed as workers going permanently deaf to a
//! non-empty queue after ~100k requests.

use std::cell::RefCell;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use magnus::prelude::*;
use magnus::{Error, RArray, RHash, RString, Ruby};

use crate::gvl;
use crate::registry::{self, BoxedCtx, ServerInner, WorkerSlot};
use crate::request::Request;

pub const TICK: Duration = Duration::from_millis(50);

/// None = shutdown (queue closed or interrupted); the caller can't tell
/// the difference and doesn't need to.
type Taken = Option<BoxedCtx>;

/// Block until one request arrives (GVL released, interruptible).
/// No busy-poll before parking, deliberately: the wake-per-request futex
/// cost is real (~20% of cycles at saturation, per perf), but a measured
/// 20µs spin made things WORSE on oversubscribed cores: spinners steal
/// exactly the CPU the tokio threads need.
fn block_take(server: &ServerInner, slot: &Arc<WorkerSlot>) -> Result<Taken, Error> {
    if slot.lane_rx.is_some() {
        return lane_take(server, slot);
    }
    let req_rx = &server.req_rx;

    // Fast path: a request is already queued (the common case under load).
    // try_recv never blocks, so the whole GVL release/reacquire (two
    // scheduler round-trips per request) is skipped entirely.
    match req_rx.try_recv() {
        Ok(ctx) => Ok(Some(ctx)),
        Err(flume::TryRecvError::Disconnected) => Ok(None),
        Err(flume::TryRecvError::Empty) => {
            let taken =
                gvl::interruptible(&slot.interrupted, || match req_rx.recv_timeout(TICK) {
                    Ok(ctx) => Some(Some(ctx)),
                    Err(flume::RecvTimeoutError::Timeout) => None,
                    Err(flume::RecvTimeoutError::Disconnected) => Some(None),
                })?;
            Ok(taken.flatten())
        }
    }
}

/// Lane-mode take: own lane first (no wake needed while the dispatcher
/// keeps feeding an awake lane), then steal from siblings, then park on
/// the own lane with the parked flag raised so the dispatcher avoids it.
fn lane_take(server: &ServerInner, slot: &Arc<WorkerSlot>) -> Result<Taken, Error> {
    let lane_rx = slot.lane_rx.as_ref().expect("lane_take without lane");

    let steal = || -> Option<BoxedCtx> {
        let slots = server.slots.read();
        for other in slots.iter() {
            if Arc::ptr_eq(other, slot) {
                continue;
            }
            if let Some(rx) = other.lane_rx.as_ref() {
                if let Ok(ctx) = rx.try_recv() {
                    return Some(ctx);
                }
            }
        }
        None
    };

    // Hot path, GVL still held: own lane, then a steal sweep.
    match lane_rx.try_recv() {
        Ok(ctx) => return Ok(Some(ctx)),
        Err(flume::TryRecvError::Disconnected) => return Ok(None),
        Err(flume::TryRecvError::Empty) => {}
    }
    if let Some(ctx) = steal() {
        return Ok(Some(ctx));
    }

    // Park. The flag-then-recheck order closes the race with a dispatcher
    // that read parked=false just before we set it: anything it sent lands
    // in the lane, and recv_timeout checks the queue before sleeping.
    slot.parked.store(true, Ordering::SeqCst);
    let taken = gvl::interruptible(&slot.interrupted, || {
        match lane_rx.recv_timeout(TICK) {
            Ok(ctx) => Some(Some(ctx)),
            // Periodic steal so a backlog behind a slow sibling can't
            // outlive a tick.
            Err(flume::RecvTimeoutError::Timeout) => steal().map(Some),
            Err(flume::RecvTimeoutError::Disconnected) => Some(None),
        }
    });
    // Unpark before propagating any panic, or the dispatcher shuns this
    // lane for good.
    slot.parked.store(false, Ordering::SeqCst);
    Ok(taken?.flatten())
}

/// Wrap a ctx into its env Hash, with the Ruby request handle embedded
/// under the frozen "kino.request" key (one Hash carries everything, no
/// per-request pair array). Registered in the slot's in-flight list;
/// created inside the calling ractor, so handle ownership is correct by
/// construction.
fn admit(
    ruby: &Ruby,
    server: &ServerInner,
    slot: &Arc<WorkerSlot>,
    mut ctx: BoxedCtx,
) -> Result<RHash, Error> {
    server.served.fetch_add(1, Ordering::Relaxed);
    slot.served.fetch_add(1, Ordering::Relaxed);
    slot.last_started_ms.store(crate::mono::mono_ms(), Ordering::Relaxed);
    slot.in_flight.fetch_add(1, Ordering::Relaxed);
    slot.current.lock().push(Arc::downgrade(&ctx.responder));
    // Wire the slot into the request so blocked body reads/writes are
    // interruptible the same way the queue pop is.
    ctx.slot = Some(slot.clone());
    let env = crate::request::build_env(ruby, &ctx)?;
    let request = ruby.obj_wrap(Request(RefCell::new(*ctx)));
    let key = ruby.get_inner(crate::env_strings::get().kino_request);
    env.aset(key, request.as_value())?;
    Ok(env)
}

type Checkout = (Arc<ServerInner>, Arc<WorkerSlot>, BoxedCtx);

fn checkout(ruby: &Ruby, server_id: u64, worker_id: usize) -> Result<Option<Checkout>, Error> {
    let Some(server) = registry::try_get(server_id) else {
        return Ok(None); // server torn down → clean shutdown signal
    };
    let slot = server.slot(ruby, worker_id)?;

    // The previous batch is fully answered once the worker comes back.
    slot.current.lock().clear();
    slot.in_flight.store(0, Ordering::Relaxed);
    slot.interrupted.store(false, Ordering::SeqCst);

    Ok(block_take(&server, &slot)?.map(|ctx| (server, slot, ctx)))
}

/// Take one request; returns its env Hash (request handle inside under
/// "kino.request") or nil on shutdown. The batch-of-one hot path: no
/// arrays allocated at all.
pub fn take_one(ruby: &Ruby, server_id: u64, worker_id: usize) -> Result<Option<RHash>, Error> {
    match checkout(ruby, server_id, worker_id)? {
        Some((server, slot, ctx)) => Ok(Some(admit(ruby, &server, &slot, ctx)?)),
        None => Ok(None),
    }
}

/// Take up to `max` requests: block for the first, drain the rest
/// non-blocking (they only batch when the queue is already deep).
/// Returns nil on shutdown; otherwise an Array of env Hashes.
pub fn take_batch(
    ruby: &Ruby,
    server_id: u64,
    worker_id: usize,
    max: usize,
) -> Result<Option<RArray>, Error> {
    let Some((server, slot, first)) = checkout(ruby, server_id, worker_id)? else {
        return Ok(None);
    };

    let batch = ruby.ary_new_capa(max.max(1));
    batch.push(admit(ruby, &server, &slot, first)?)?;
    for _ in 1..max {
        match server.req_rx.try_recv() {
            Ok(ctx) => batch.push(admit(ruby, &server, &slot, ctx)?)?,
            Err(_) => break,
        }
    }
    Ok(Some(batch))
}

/// The fused hot path: answer `request` (complete response in one shot)
/// and immediately take the next request. One FFI crossing per request
/// once the loop is warm.
pub fn respond_and_take_one(
    ruby: &Ruby,
    request: &Request,
    server_id: u64,
    worker_id: usize,
    status: u16,
    headers: RHash,
    body: RString,
) -> Result<Option<RHash>, Error> {
    crate::request::respond_simple(ruby, request, status, headers, body)?;
    take_one(ruby, server_id, worker_id)
}

/// Batch variant of the fused call.
#[allow(clippy::too_many_arguments)]
pub fn respond_and_take(
    ruby: &Ruby,
    request: &Request,
    server_id: u64,
    worker_id: usize,
    max: usize,
    status: u16,
    headers: RHash,
    body: RString,
) -> Result<Option<RArray>, Error> {
    crate::request::respond_simple(ruby, request, status, headers, body)?;
    take_batch(ruby, server_id, worker_id, max)
}
