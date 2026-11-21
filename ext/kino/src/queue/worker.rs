//! `Kino::Native::Worker`, a worker's handle on its slot, and the
//! admission of what it takes into Ruby values.

use std::cell::RefCell;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use magnus::prelude::*;
use magnus::{Error, RArray, RHash, RString, Ruby, Value};

use super::take::{Room, Take, Taker};
use super::Item;
use crate::doorbell::DoorbellSockets;
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

/// admit's part before the env. On a worker_loop server, a request taken from a lane left room
/// there for the first request waiting for some.
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
    if slot.lane.is_worker_loop() {
        load.hold(Arc::as_ptr(&ctx.responder) as usize);
    }
    // Wire the slot into the request so blocked body reads/writes are
    // interruptible the same way the queue pop is.
    ctx.slot = Some(slot.clone());
    if server.config.worker_loop {
        server.lane_room.notify_taken();
    }
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

/// The item, false when nothing is ready, nil once the lane is closed; a wake counts toward
/// the server's wakes, as at checkout.
fn nonblock_value(
    ruby: &Ruby,
    server: &ServerInner,
    slot: &Arc<WorkerSlot>,
    found: Take,
) -> Result<Value, Error> {
    match found {
        Take::Nothing => Ok(ruby.qfalse().as_value()),
        Take::Closed => Ok(ruby.qnil().as_value()),
        Take::Item(item) => {
            if matches!(item, Item::Wake) {
                server.counters.wakes.fetch_add(1, Ordering::Relaxed);
            }
            item_value(ruby, server, slot, item)
        }
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
    /// Its own hold on the doorbell's sockets, so ready_fd stays open while it may wait on it.
    doorbell: Option<Arc<DoorbellSockets>>,
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

/// Labels of the worker_loop workers whose serve returned at least `after` seconds ago and
/// whose loop still runs, each reported once per drain.
pub fn lingering_loops(server_id: u64, after: f64) -> Vec<String> {
    let Some(server) = registry::try_get(server_id) else {
        return Vec::new();
    };
    let due = crate::mono::mono_us().saturating_sub((after * 1e6) as u64);
    let slots = server.slots.read();
    slots
        .iter()
        .filter_map(|slot| slot.life.take_drained_by(due))
        .collect()
}

impl Worker {
    pub(crate) fn new(
        server: &Arc<ServerInner>,
        worker_id: usize,
        slot: Arc<WorkerSlot>,
    ) -> Worker {
        let doorbell = slot.lane.doorbell.as_ref().and_then(|d| d.sockets());
        Worker {
            server: Arc::downgrade(server),
            server_id: server.id,
            worker_id,
            slot,
            doorbell,
        }
    }

    fn worker_loop_only(&self, ruby: &Ruby, native: &str) -> Result<(), Error> {
        match self.slot.lane.is_worker_loop() {
            true => Ok(()),
            false => Err(Error::new(
                ruby.exception_runtime_error(),
                format!("Kino: {native} needs a worker_loop server"),
            )),
        }
    }

    pub(super) fn checkout(&self) -> Result<Option<(Arc<ServerInner>, Item)>, Error> {
        let Some(server) = self.server.upgrade() else {
            return Ok(None); // server torn down → clean shutdown signal
        };

        // The previous batch is fully answered once the worker comes back.
        let load = &self.slot.load;
        load.idle();

        let taken = Taker::new(&server, &self.slot).take(&mut crate::gvl::Released)?;
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

    /// Its serve returned: lingering_loops reports `label` once the loop is overdue to return too.
    pub fn drained(rb_self: &Worker, label: String) {
        rb_self.slot.life.drained(label);
    }

    /// Kino::Native::Worker#take_nonblock(only = nil): an env Hash, a socket event, :wake; false
    /// when nothing is ready (the doorbell then armed: wait on #ready_fd); nil once the lane is
    /// closed or a retired worker has nothing left. `only` (:events, :requests) narrows it, and
    /// then it never returns nil.
    pub fn take_nonblock(ruby: &Ruby, rb_self: &Worker, args: &[Value]) -> Result<Value, Error> {
        rb_self.take(ruby, "take_nonblock", args, false)
    }

    /// The wait on #ready_fd ended: off the doorbell, what rang read, then #take_nonblock(only).
    pub fn take_woken(ruby: &Ruby, rb_self: &Worker, args: &[Value]) -> Result<Value, Error> {
        rb_self.take(ruby, "take_woken", args, true)
    }

    fn take(&self, ruby: &Ruby, native: &str, args: &[Value], woken: bool) -> Result<Value, Error> {
        ruby.check_arity(args.len(), 0..=1)?;
        self.worker_loop_only(ruby, native)?;
        let Some(server) = self.server.upgrade() else {
            return Ok(ruby.qnil().as_value());
        };
        let room = match args.first() {
            None => Room::All,
            Some(&only) => match magnus::Symbol::try_convert(only)?.name()?.as_ref() {
                "events" => Room::Events,
                "requests" => Room::Requests,
                other => {
                    return Err(Error::new(
                        ruby.exception_arg_error(),
                        format!("{native} takes :events or :requests, not :{other}"),
                    ))
                }
            },
        };
        if woken {
            self.off_doorbell();
        }
        let found = Taker::new(&server, &self.slot).nonblock(room);
        nonblock_value(ruby, &server, &self.slot, found)
    }

    /// Disarmed first, so what is sent from now on lands without a write, for the next take.
    pub(super) fn off_doorbell(&self) {
        if let Some(doorbell) = &self.slot.lane.doorbell {
            doorbell.disarm();
        }
        if let Some(sockets) = &self.doorbell {
            sockets.take_rings();
        }
    }

    /// A job finished while its loop waits for room: wakes the loop if it armed.
    pub fn ring(rb_self: &Worker) {
        rb_self.slot.lane.ring_doorbell();
    }

    /// What the app's scheduler waits on when take_nonblock finds nothing; nil without worker_loop.
    pub fn ready_fd(rb_self: &Worker) -> Option<i32> {
        rb_self.doorbell.as_ref().map(|sockets| sockets.fd())
    }

    /// The wait on #ready_fd, or one for a job to end, ended; a take comes next.
    pub fn woke(rb_self: &Worker) {
        if rb_self.slot.lane.is_worker_loop() {
            rb_self.slot.load.back_in_loop();
        }
        rb_self.off_doorbell();
        rb_self.slot.park.unpark();
    }

    /// The most requests this worker runs at once; 0 = no limit.
    pub fn set_capacity(rb_self: &Worker, capacity: usize) {
        rb_self.slot.load.set_capacity(capacity);
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
