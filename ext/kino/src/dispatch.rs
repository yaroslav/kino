//! Handing a request to a worker: its lane, in hot-lane order, or the shared queue.

use std::ops::ControlFlow;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use crate::registry::{BoxedCtx, LaneItem, ServerInner};
use crate::slot::WorkerSlot;

#[cfg(test)]
mod tests;

/// One dispatch attempt: Break(sent) once it is over, sent or with every lane closed;
/// Continue hands the request back, every open lane full, for another attempt.
type Attempt = ControlFlow<bool, BoxedCtx>;

/// A send down a list of lanes: Break once a lane took the request, Continue with it back.
type Unsent = ControlFlow<(), BoxedCtx>;

fn try_dispatch(server: &ServerInner, ctx: BoxedCtx) -> Attempt {
    let slots = server.slots.read();
    if slots.is_empty() {
        return Attempt::Continue(ctx);
    }
    let start = server.lane_cursor.fetch_add(1, Ordering::Relaxed);
    let mut any_open = false;
    match send(&slots, start, &mut any_open, ctx) {
        Unsent::Break(()) => Attempt::Break(true),
        Unsent::Continue(ctx) if any_open => Attempt::Continue(ctx), // overload: every open lane is full
        Unsent::Continue(_) => Attempt::Break(false),                // draining: all lanes closed
    }
}

/// An awake lane with room (a hot worker never pays a futex wake), unless its worker is deep
/// in its item and another idles; then the idle worker that parked last (warmest caches); then
/// any lane with room. A handshake goes first to the workers with the fewest sockets, as a
/// socket stays with the worker that accepts it.
fn send(slots: &[Arc<WorkerSlot>], start: usize, any_open: &mut bool, mut ctx: BoxedCtx) -> Unsent {
    if ctx.offers_websocket() {
        if let Some(fewest) = fewest_sockets(slots) {
            let least_loaded =
                |slot: &WorkerSlot| slot.sockets.open() == fewest && takes_sockets(slot);
            ctx = send_hot_first(slots, start, any_open, ctx, least_loaded)?;
        }
    }
    send_hot_first(slots, start, any_open, ctx, |_| true)
}

/// In the pool, and its sockets not being closed.
fn takes_sockets(slot: &WorkerSlot) -> bool {
    !slot.life.seems_retired() && slot.sockets.accepting()
}

fn fewest_sockets(slots: &[Arc<WorkerSlot>]) -> Option<usize> {
    slots
        .iter()
        .filter(|slot| takes_sockets(slot))
        .map(|slot| slot.sockets.open())
        .min()
}

/// In hot-lane order among the slots `eligible` picks, from `start`; back when no lane has room.
fn send_hot_first(
    slots: &[Arc<WorkerSlot>],
    start: usize,
    any_open: &mut bool,
    mut ctx: BoxedCtx,
    eligible: impl Fn(&WorkerSlot) -> bool,
) -> Unsent {
    let n = slots.len();
    // A lane with no room for a request (full, retired) is still open.
    let mut try_lane = |slot: &WorkerSlot, ctx: BoxedCtx| -> Unsent {
        let Err(refused) = slot.send_lane(LaneItem::Request(ctx)) else {
            return Unsent::Break(());
        };
        *any_open |= matches!(refused, flume::TrySendError::Full(_));
        match refused.into_inner() {
            LaneItem::Request(ctx) => Unsent::Continue(ctx),
            LaneItem::Bell => unreachable!("only requests are dispatched"),
        }
    };

    // Only an empty lane: a burst into an idle server must spread, not queue behind one hot
    // worker. A crashed worker's slot stays parked and retired until it is recycled.
    let find_hottest = || {
        slots
            .iter()
            .filter(|slot| slot.park.is_parked())
            .filter(|slot| !slot.life.seems_retired())
            .filter(|slot| eligible(slot))
            .filter(|slot| slot.lane.open_and_empty())
            .max_by_key(|slot| slot.park.parked_at())
    };
    let mut hottest = None;
    let mut now = None;
    for k in 0..n {
        let slot = &slots[(start + k) % n];
        if slot.park.is_parked() || !eligible(slot) {
            continue;
        }
        // Passed over only for an idle sibling; with none, any busy worker will do.
        if deep_in_work(slot, &mut now) && hottest.get_or_insert_with(find_hottest).is_some() {
            continue;
        }
        ctx = try_lane(slot, ctx)?;
    }
    if let Some(slot) = hottest.unwrap_or_else(find_hottest) {
        ctx = try_lane(slot, ctx)?;
    }
    // Any lane with room, round-robin; this also tells a full lane from a closed one.
    for k in 0..n {
        let slot = &slots[(start + k) % n];
        if eligible(slot) {
            ctx = try_lane(slot, ctx)?;
        }
    }
    Unsent::Continue(ctx)
}

/// How far into its item a busy worker stops counting as hot while a sibling idles: past it,
/// waking the sibling (tens of microseconds) beats waiting out the rest.
const DEEP_US: u64 = 100;

/// `now` is read on first need, for a busy worker only.
fn deep_in_work(slot: &WorkerSlot, now: &mut Option<u64>) -> bool {
    let Some(started) = slot.load.busy_since() else {
        return false;
    };
    let now = *now.get_or_insert_with(crate::mono::mono_us);
    now.saturating_sub(started) >= DEEP_US
}

/// How often a request looks again while every lane is full.
const LANE_RETRY: Duration = Duration::from_micros(500);

/// In a lane, retrying while all are full, or on the shared queue; false (a 503) when every
/// queue is closed or still full at queue_timeout.
pub(crate) async fn enqueue(server: &ServerInner, ctx: BoxedCtx) -> bool {
    let timeout = Duration::from_millis(server.config.queue_timeout_ms);
    if server.config.lanes {
        let attempt = retry_for_lane_room(server, ctx, timeout).await;
        return matches!(attempt, Attempt::Break(true));
    }
    // Cloned per request, so a keep-alive connection cannot hold the queue open during drain.
    let Some(tx) = server.req_tx.lock().clone() else {
        return false;
    };
    // try_send first: room is the common case, and it skips a tokio timer.
    match tx.try_send(ctx) {
        Ok(()) => true,
        Err(flume::TrySendError::Disconnected(_)) => false,
        // Timed out or closed mid-send: ctx was dropped unsent either way.
        Err(flume::TrySendError::Full(ctx)) => matches!(
            tokio::time::timeout(timeout, tx.send_async(ctx)).await,
            Ok(Ok(()))
        ),
    }
}

/// A lanes server's dispatch: it looks again every LANE_RETRY. Continue: no room within `timeout`.
async fn retry_for_lane_room(
    server: &ServerInner,
    mut ctx: BoxedCtx,
    timeout: Duration,
) -> Attempt {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        ctx = try_dispatch(server, ctx)?;
        if tokio::time::Instant::now() >= deadline {
            return Attempt::Continue(ctx);
        }
        tokio::time::sleep(LANE_RETRY).await;
    }
}
