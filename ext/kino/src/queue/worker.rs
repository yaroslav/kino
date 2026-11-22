//! `Kino::Native::Worker`, a worker's handle on its slot, and the
//! admission of what it takes into Ruby values.

use std::cell::RefCell;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use magnus::prelude::*;
use magnus::{Error, RHash, RString, Ruby, Value};

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
    let strings = crate::env_strings::get();
    let request = embed(ruby, strings, env, ctx)?;
    if hijack {
        // The request handle is its own hijack callable (Request#call).
        env.aset(ruby.get_inner(strings.rack_hijack_offered), true)?;
        env.aset(ruby.get_inner(strings.rack_hijack), request.as_value())?;
    }
    if websocket {
        env.aset("rack.upgrade?", crate::symbols::get().websocket.value())?;
    }
    Ok(env)
}

/// admit's part before the env. On a worker_loop server, a request taken from a lane left room
/// there for the first request waiting for some.
#[inline]
pub(super) fn enlist(server: &ServerInner, slot: &Arc<WorkerSlot>, ctx: &mut RequestCtx) {
    // One clock read serves the busy age, the histogram and, for the access log, the
    // request's queue wait and the start of its time in Ruby.
    let now = std::time::Instant::now();
    serves(server, slot, ctx, now);
    let wait = now.duration_since(ctx.enqueued_at);
    server
        .counters
        .queue_histogram
        .record(wait.as_micros() as u64);
    ctx.wait = wait;
    ctx.admitted_at = now;
    // Wire the slot into the request so blocked body reads/writes are
    // interruptible the same way the queue pop is.
    ctx.slot = Some(slot.clone());
    if server.config.worker_loop {
        server.lane_room.notify_taken();
    }
}

/// An inline request, admitted on its connection's job as admit admits one from a lane, except
/// that it waited in no queue, took no lane's room and counts in flight from its parse. Never a
/// hijack or a WebSocket.
pub(crate) fn admit_inline(
    ruby: &Ruby,
    server: &ServerInner,
    slot: &Arc<WorkerSlot>,
    ctx: BoxedCtx,
) -> Result<RHash, Error> {
    enlist_inline(server, slot, &ctx);
    let env = crate::request::build_env(ruby, &ctx)?;
    embed(ruby, crate::env_strings::get(), env, ctx)?;
    Ok(env)
}

pub(crate) fn enlist_inline(server: &ServerInner, slot: &WorkerSlot, ctx: &RequestCtx) {
    serves(server, slot, ctx, std::time::Instant::now());
}

/// What every admission counts: the request served, in flight on `slot` from `now`, and held by
/// a worker_loop worker's thread until answered.
#[inline]
fn serves(server: &ServerInner, slot: &WorkerSlot, ctx: &RequestCtx, now: std::time::Instant) {
    server.counters.served.fetch_add(1, Ordering::Relaxed);
    slot.load.admit(&ctx.responder, now);
    if slot.lane.is_worker_loop() {
        slot.load.hold(Arc::as_ptr(&ctx.responder) as usize);
    }
}

/// `ctx` wrapped in the calling Ractor, which owns it from here on, under the "kino.request" key.
#[inline]
fn embed(
    ruby: &Ruby,
    strings: &crate::env_strings::EnvStrings,
    env: RHash,
    ctx: BoxedCtx,
) -> Result<magnus::typed_data::Obj<Request>, Error> {
    let request = ruby.obj_wrap(Request(RefCell::new(*ctx)));
    env.aset(ruby.get_inner(strings.kino_request), request.as_value())?;
    Ok(request)
}

/// A request as its env Hash, a socket event as WsEvent::into_ruby makes it, :wake, or a Conn.
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
        Item::Wake => Ok(crate::symbols::get().wake.value()),
        Item::Conn(conn) => Ok(crate::inline::wrap(ruby, conn)),
    }
}

/// A take as Ruby gets it: what `value` makes of an item, false when nothing is ready, nil at
/// shutdown (the queues closed, a retired worker with nothing left).
fn take_value(
    ruby: &Ruby,
    take: Take,
    value: impl FnOnce(Item) -> Result<Value, Error>,
) -> Result<Value, Error> {
    match take {
        Take::Item(item) => value(item),
        Take::Closed => Ok(ruby.qnil().as_value()),
        Take::Nothing => Ok(ruby.qfalse().as_value()),
    }
}

/// What a take hands Ruby: a wake counts toward the server's wakes.
#[inline]
fn handed(server: &ServerInner, take: &Take) {
    if matches!(take, Take::Item(Item::Wake)) {
        server.counters.wakes.fetch_add(1, Ordering::Relaxed);
    }
}

/// Per-worker native handle: the registry lookup and slot resolution are
/// paid once at worker boot instead of on every take. Created inside the
/// worker thread/ractor that uses it (like Request handles), so ownership
/// is correct by construction. Holds the server weakly: after teardown the
/// upgrade fails, which its takes read as a clean shutdown.
/// Wrapped in typed_data.rs, whose promise its drop keeps: a compare-and-swap, then its Arcs.
#[derive(magnus::DataTypeFunctions)]
pub struct Worker {
    server: std::sync::Weak<ServerInner>,
    server_id: u64,
    worker_id: usize,
    slot: Arc<WorkerSlot>,
    /// The slot's generation as it got it; it holds the slot as this plus one (Life::holder).
    generation: u64,
    /// Its own hold on the doorbell's sockets, so ready_fd stays open while it may wait on it.
    doorbell: Option<Arc<DoorbellSockets>>,
}

impl Drop for Worker {
    /// A handle gone without its release (its heap freed) holds its slot no more.
    fn drop(&mut self) {
        self.slot.life.let_go(self.generation);
    }
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
        let generation = slot.life.hold();
        Worker {
            server: Arc::downgrade(server),
            server_id: server.id,
            worker_id,
            slot,
            generation,
            doorbell,
        }
    }

    fn holds(&self) -> bool {
        self.slot.life.holds(self.generation)
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

    /// A classic worker's next take, by `take` (`waiting` or `fused`), as take_value hands it to
    /// Ruby; nil too once the server is gone.
    fn checkout(
        &self,
        ruby: &Ruby,
        take: impl FnOnce(&Worker, &ServerInner) -> Result<Take, Error>,
        value: impl FnOnce(&Arc<ServerInner>, Item) -> Result<Value, Error>,
    ) -> Result<Value, Error> {
        let Some(server) = self.server.upgrade() else {
            return Ok(ruby.qnil().as_value());
        };
        let take = take(self, &server)?;
        take_value(ruby, take, |item| value(&server, item))
    }

    /// Whether the worker is due a pass of a shared GVL, judged at its last item's start, a clock
    /// read its admission made, so a classic take reads none of its own (shared_gvl.rs).
    #[inline]
    fn held_long(&self, server: &ServerInner) -> bool {
        crate::shared_gvl::held_long(server, &self.slot, || self.slot.load.last_started())
    }

    /// The take that waits for an item, the GVL passed first if held long; Closed at shutdown.
    pub(super) fn waiting(&self, server: &ServerInner) -> Result<Take, Error> {
        if self.held_long(server) {
            crate::shared_gvl::pass(&self.slot)?;
        }
        // The previous batch is fully answered once the worker comes back.
        self.slot.load.idle();
        let taken = Taker::new(server, &self.slot).take(&mut crate::gvl::Released)?;
        Ok(self.took(server, taken.map_or(Take::Closed, Take::Item)))
    }

    /// The take that rides an answer: only what is already there. With nothing there the worker
    /// stays the answered request's while its body closes, so a close that hangs reads as a
    /// wedge. One due a pass takes nothing either: the body closes first, then `waiting` passes.
    fn fused(&self, server: &ServerInner) -> Result<Take, Error> {
        if self.held_long(server) {
            return Ok(Take::Nothing);
        }
        let take = Taker::new(server, &self.slot).now();
        if matches!(take, Take::Nothing) {
            return Ok(take);
        }
        self.slot.load.idle();
        Ok(self.took(server, take))
    }

    /// A socket callback or a wake hook is the worker's one item until its next take.
    #[inline]
    fn took(&self, server: &ServerInner, take: Take) -> Take {
        if matches!(take, Take::Item(Item::Ws(_) | Item::Wake)) {
            handed(server, &take);
            self.slot.load.runs_item();
        }
        take
    }

    /// The next item as item_value makes it; nil on shutdown. Allocates no array.
    pub fn take_one(ruby: &Ruby, rb_self: &Worker) -> Result<Value, Error> {
        rb_self.checkout(ruby, Worker::waiting, |server, item| {
            item_value(ruby, server, &rb_self.slot, item)
        })
    }

    /// Up to `max` items: the first as take_one takes it (an event or a wake alone), then
    /// requests from the shared queue without blocking; a lane worker takes one at a time.
    pub fn take_batch(ruby: &Ruby, rb_self: &Worker, max: usize) -> Result<Value, Error> {
        rb_self.checkout(ruby, Worker::waiting, |server, first| {
            rb_self.batch(ruby, server, first, max)
        })
    }

    /// `first`, and after a request, up to `max` in all from the shared queue.
    fn batch(
        &self,
        ruby: &Ruby,
        server: &Arc<ServerInner>,
        first: Item,
        max: usize,
    ) -> Result<Value, Error> {
        let batch = ruby.ary_new_capa(max.max(1));
        let request = matches!(first, Item::Ctx(_));
        batch.push(item_value(ruby, server, &self.slot, first)?)?;
        if request {
            for _ in 1..max {
                match server.req_rx.try_recv() {
                    Ok(ctx) => batch.push(admit(ruby, server, &self.slot, ctx)?)?,
                    Err(_) => break,
                }
            }
        }
        Ok(batch.as_value())
    }

    /// [server id, slot id, generation]: all a Waker needs.
    pub fn identity(rb_self: &Worker) -> (u64, usize, u64) {
        let generation = rb_self.slot.generation();
        (rb_self.server_id, rb_self.worker_id, generation)
    }

    /// The worker leaves its slot; its wakers stop waking.
    /// Whatever ended it, placement passes over the slot from now on: its capacity goes under the
    /// lane lock send_placed checks, and its unplaced connections go to hyper.
    pub fn release(rb_self: &Worker) {
        let slot = &rb_self.slot;
        // Once only, and never once the slot is another worker's.
        if !rb_self.holds() {
            return;
        }
        slot.load.let_go();
        if !slot.life.let_go(rb_self.generation) {
            return;
        }
        slot.release();
        {
            let _lane = slot.lane.tx.lock();
            slot.load.set_capacity(0);
        }
        crate::inline::unplace(slot);
    }

    /// Kino::Native::Worker#crashed: answer what it held and retire its slot (WorkerSlot::crash)
    /// while it still holds it, before its release.
    pub fn crashed(rb_self: &Worker) {
        if let Some(server) = rb_self.server.upgrade().filter(|_| rb_self.holds()) {
            rb_self.slot.crash(&server);
        }
    }

    /// Its serve returned: lingering_loops reports `label` once the loop is overdue to return too.
    pub fn drained(rb_self: &Worker, label: String) {
        rb_self.slot.life.drained(label);
    }

    /// Kino::Native::Worker#take_nonblock(only = nil): an env Hash, a socket event, :wake; false
    /// when nothing is ready (the doorbell then armed: wait on #ready_fd); nil once the lane is
    /// closed or a retired worker has nothing left. `only` (:events, :requests, :connections)
    /// narrows it, and then it never returns nil. A connection to serve inline comes as a
    /// Kino::Native::Conn.
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
        // By the Symbol's identity: reading its name takes the VM's lock (symbols.rs).
        let symbols = crate::symbols::get();
        let room = match args.first() {
            None => Room::All,
            Some(&only) if symbols.events.is(only) => Room::Events,
            Some(&only) if symbols.requests.is(only) => Room::Requests,
            Some(&only) if symbols.connections.is(only) => Room::Connections,
            Some(&only) => {
                let other = magnus::Symbol::try_convert(only)?.name()?;
                return Err(Error::new(
                    ruby.exception_arg_error(),
                    format!("{native} takes :events, :requests or :connections, not :{other}"),
                ));
            }
        };
        crate::shared_gvl::pass_in_loop(&server, &self.slot)?;
        if woken {
            self.off_doorbell();
        }
        let found = Taker::new(&server, &self.slot).nonblock(room);
        handed(&server, &found);
        take_value(ruby, found, |item| {
            item_value(ruby, &server, &self.slot, item)
        })
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
/// and take what is already there: one FFI crossing per request while requests keep coming.
/// False when nothing is, so the body closes before take_one waits; nil on shutdown.
pub fn respond_and_take_one(
    ruby: &Ruby,
    request: &Request,
    worker: magnus::typed_data::Obj<Worker>,
    status: u16,
    headers: RHash,
    body: RString,
) -> Result<Value, Error> {
    crate::request::respond_simple(ruby, request, status, headers, body)?;
    worker.checkout(ruby, Worker::fused, |server, item| {
        item_value(ruby, server, &worker.slot, item)
    })
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
) -> Result<Value, Error> {
    crate::request::respond_simple(ruby, request, status, headers, body)?;
    worker.checkout(ruby, Worker::fused, |server, first| {
        worker.batch(ruby, server, first, max)
    })
}
