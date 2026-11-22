//! `Kino::Native::Worker`, a worker's handle on its slot, and the
//! admission of what it takes into Ruby values.

use std::cell::RefCell;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use magnus::prelude::*;
use magnus::{Error, RArray, RHash, RString, Ruby, Value};

use super::take::Taker;
use super::Item;
use crate::gvl;
use crate::registry::{self, BoxedCtx, ServerInner};
use crate::request::{Request, RequestCtx};
use crate::slot::WorkerSlot;

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
    enlist(server, slot, &mut ctx);
    let env = crate::request::build_env(ruby, &ctx)?;
    let websocket = ctx.offers_websocket();
    let hijack = crate::hijack::offered(&ctx);
    let request = ruby.obj_wrap(Request(RefCell::new(*ctx)));
    let strings = crate::env_strings::get();
    env.aset(ruby.get_inner(strings.kino_request), request.as_value())?;
    if hijack {
        // The request handle is its own hijack callable (Request#call).
        env.aset(ruby.get_inner(strings.rack_hijack_offered), true)?;
        env.aset(ruby.get_inner(strings.rack_hijack), request.as_value())?;
    }
    if websocket {
        env.aset("rack.upgrade?", ruby.to_symbol("websocket"))?;
    }
    Ok(env)
}

/// admit's part before the env.
#[inline]
pub(super) fn enlist(server: &ServerInner, slot: &Arc<WorkerSlot>, ctx: &mut RequestCtx) {
    let (counters, load) = (&server.counters, &slot.load);
    counters.served.fetch_add(1, Ordering::Relaxed);
    load.admit(&ctx.responder);
    // One clock read serves the histogram and, for the access log, the
    // request's queue wait and the start of its time in Ruby.
    let now = std::time::Instant::now();
    let wait = now.duration_since(ctx.enqueued_at);
    counters.queue_histogram.record(wait.as_micros() as u64);
    ctx.wait = wait;
    ctx.admitted_at = now;
    // Wire the slot into the request so blocked body reads/writes are
    // interruptible the same way the queue pop is.
    ctx.slot = Some(slot.clone());
}

/// A request as its env Hash, a socket event as WsEvent::into_ruby makes it, or :wake.
#[inline]
fn item_value(
    ruby: &Ruby,
    server: &ServerInner,
    slot: &Arc<WorkerSlot>,
    item: Item,
) -> Result<Value, Error> {
    match item {
        Item::Ctx(ctx) => Ok(admit(ruby, server, slot, ctx)?.as_value()),
        Item::Ws(event) => event.into_ruby(ruby),
        Item::Wake => Ok(ruby.to_symbol("wake").as_value()),
    }
}

/// Per-worker native handle: the registry lookup and slot resolution are
/// paid once at worker boot instead of on every take. Created inside the
/// worker thread/ractor that uses it (like Request handles), so ownership
/// is correct by construction. Holds the server weakly: after teardown the
/// upgrade fails, which is the same clean shutdown signal the per-take
/// registry lookup used to give.
#[magnus::wrap(class = "Kino::Native::Worker", free_immediately)]
pub struct Worker {
    server: std::sync::Weak<ServerInner>,
    server_id: u64,
    worker_id: usize,
    slot: Arc<WorkerSlot>,
}

/// Resolve a (server, worker) pair into a Worker handle; nil when the
/// server is already gone (the caller treats that as shutdown).
pub fn worker(ruby: &Ruby, server_id: u64, worker_id: usize) -> Result<Option<Worker>, Error> {
    let Some(server) = registry::try_get(server_id) else {
        return Ok(None);
    };
    let slot = server.slot(ruby, worker_id)?;
    Ok(Some(Worker::new(&server, worker_id, slot)))
}

/// Behind Worker::Waker: wake the worker on slot `worker_id` at `generation`. False once
/// it left or was retired, the server is gone, or there are no lanes; callable from any Ractor.
pub fn wake_worker(server_id: u64, worker_id: usize, generation: u64) -> bool {
    let Some(server) = registry::try_get(server_id) else {
        return false;
    };
    let slot = server.slots.read().get(worker_id).cloned();
    slot.is_some_and(|slot| slot.generation() == generation && !slot.life.retired() && slot.wake())
}

impl Worker {
    pub(crate) fn new(
        server: &Arc<ServerInner>,
        worker_id: usize,
        slot: Arc<WorkerSlot>,
    ) -> Worker {
        Worker {
            server: Arc::downgrade(server),
            server_id: server.id,
            worker_id,
            slot,
        }
    }

    pub(super) fn checkout(&self) -> Result<Option<(Arc<ServerInner>, Item)>, Error> {
        let Some(server) = self.server.upgrade() else {
            return Ok(None); // server torn down → clean shutdown signal
        };

        // The previous batch is fully answered once the worker comes back.
        let load = &self.slot.load;
        load.idle();
        let interrupted = &self.slot.life.interrupted;
        interrupted.store(false, Ordering::SeqCst);

        let taken = Taker::new(&server, &self.slot)
            .take(|attempt| gvl::interruptible(interrupted, attempt))?;
        if matches!(taken, Some(Item::Ws(_) | Item::Wake)) {
            if matches!(taken, Some(Item::Wake)) {
                server.counters.wakes.fetch_add(1, Ordering::Relaxed);
            }
            load.runs_item();
        }
        Ok(taken.map(|item| (server, item)))
    }

    /// The next item as item_value makes it; nil on shutdown. Allocates no array.
    pub fn take_one(ruby: &Ruby, rb_self: &Worker) -> Result<Option<Value>, Error> {
        match rb_self.checkout()? {
            Some((server, item)) => item_value(ruby, &server, &rb_self.slot, item).map(Some),
            None => Ok(None),
        }
    }

    /// Up to `max` items: the first as take_one takes it (an event or a wake alone), then
    /// requests from the shared queue without blocking; a lane worker takes one at a time.
    pub fn take_batch(ruby: &Ruby, rb_self: &Worker, max: usize) -> Result<Option<RArray>, Error> {
        let Some((server, first)) = rb_self.checkout()? else {
            return Ok(None);
        };

        let batch = ruby.ary_new_capa(max.max(1));
        let request = matches!(first, Item::Ctx(_));
        batch.push(item_value(ruby, &server, &rb_self.slot, first)?)?;
        if !request {
            return Ok(Some(batch));
        }
        for _ in 1..max {
            match server.req_rx.try_recv() {
                Ok(ctx) => batch.push(admit(ruby, &server, &rb_self.slot, ctx)?)?,
                Err(_) => break,
            }
        }
        Ok(Some(batch))
    }

    /// [server id, slot id, generation]: all a Waker needs.
    pub fn identity(rb_self: &Worker) -> (u64, usize, u64) {
        let generation = rb_self.slot.generation();
        (rb_self.server_id, rb_self.worker_id, generation)
    }

    /// The worker leaves its slot; its wakers stop waking.
    pub fn release(rb_self: &Worker) {
        rb_self.slot.release();
    }

    /// Whether the worker has a lane, which wakes and WebSocket events need.
    pub fn lanes(rb_self: &Worker) -> bool {
        rb_self.slot.lane.exists()
    }

    /// True for the first claim of `key` among the server's workers, which prints the warning.
    pub fn claim_warning(rb_self: &Worker, key: String) -> bool {
        rb_self
            .server
            .upgrade()
            .is_some_and(|server| server.warned.lock().insert(key))
    }
}

/// The fused hot path: answer `request` (complete response in one shot)
/// and immediately take the next request. One FFI crossing per request
/// once the loop is warm.
pub fn respond_and_take_one(
    ruby: &Ruby,
    request: &Request,
    worker: magnus::typed_data::Obj<Worker>,
    status: u16,
    headers: RHash,
    body: RString,
) -> Result<Option<Value>, Error> {
    crate::request::respond_simple(ruby, request, status, headers, body)?;
    Worker::take_one(ruby, &worker)
}

/// Batch variant of the fused call.
pub fn respond_and_take(
    ruby: &Ruby,
    request: &Request,
    worker: magnus::typed_data::Obj<Worker>,
    max: usize,
    status: u16,
    headers: RHash,
    body: RString,
) -> Result<Option<RArray>, Error> {
    crate::request::respond_simple(ruby, request, status, headers, body)?;
    Worker::take_batch(ruby, &worker, max)
}
