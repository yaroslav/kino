//! A worker's takes. What is ready goes first: its sockets' events and its wake, its lane, a
//! sibling's lane. With nothing ready a classic worker parks (Taker::take), and a worker_loop
//! arms its doorbell and leaves the wait to the app's scheduler (Taker::nonblock).

use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::{Item, Taken, TICK};
use crate::gvl::{self, Back, Parker};
use crate::registry::{LaneItem, ServerInner};
use crate::slot::{WorkerSlot, HOLDING_ITEM};

/// Socket events a worker takes in a row while a request or a wake may wait;
/// then its lane gets a look and a due wake its turn. A wake counts as a whole
/// run, so wakes and requests alternate.
pub(super) const OWN_RUN: u32 = 8;
/// Park::own_run past a full run: a wake filled it (WAKE_RUN), or a due wake goes next (WAKE_TURN).
const WAKE_RUN: u32 = OWN_RUN + 1;
const WAKE_TURN: u32 = OWN_RUN + 2;

/// What a take that does not wait found.
pub(crate) enum Take {
    Item(Item),
    /// The worker's loop ends: its lane closed and its inbox is empty, or it retired with
    /// nothing left.
    Closed,
    Nothing,
}

impl Take {
    fn or_else(self, next: impl FnOnce() -> Take) -> Take {
        match self {
            Take::Nothing => next(),
            found => found,
        }
    }

    fn item(&self) -> Option<&Item> {
        match self {
            Take::Item(item) => Some(item),
            Take::Closed | Take::Nothing => None,
        }
    }

    /// As a wait's attempt reports it: None to wait on, Some(None) at the loop's end.
    fn found(self) -> Option<Taken> {
        match self {
            Take::Item(item) => Some(Some(item)),
            Take::Closed => Some(None),
            Take::Nothing => None,
        }
    }
}

impl From<Option<Item>> for Take {
    fn from(item: Option<Item>) -> Take {
        item.map_or(Take::Nothing, Take::Item)
    }
}

/// Whether a lane's own worker idles on it and takes what it holds within moments. A parked
/// classic worker blocks on its lane. A parked worker_loop one waits on its doorbell, unless the
/// wait is a tick overdue: its thread is away on a job's fiber, and rings go unanswered. A
/// retired or crashed worker's lane is never left to it.
fn owner_waits(owner: &WorkerSlot, now: &mut Option<u64>) -> bool {
    if !owner.park.is_parked() || owner.life.seems_retired() {
        return false;
    }
    if !owner.lane.is_worker_loop() {
        return true;
    }
    let now = *now.get_or_insert_with(crate::mono::mono_us);
    let due = owner.load.last_started();
    now < due.saturating_add(TICK.as_micros() as u64)
}

/// What a worker_loop take has room for (Kino::Worker::Loop#take).
/// A placed connection is taken whatever the room: one placed before the worker filled up sends
/// its first request to hyper, where queue_timeout bounds its wait.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Room {
    All,
    Events,
    Requests,
    /// Room for neither: placed connections alone.
    Connections,
}

/// A worker's side of the queues: its slot, and through the server its siblings' lanes or the
/// shared queue.
#[derive(Clone, Copy)]
pub(crate) struct Taker<'a> {
    server: &'a ServerInner,
    slot: &'a Arc<WorkerSlot>,
}

impl<'a> Taker<'a> {
    pub(crate) fn new(server: &'a ServerInner, slot: &'a Arc<WorkerSlot>) -> Self {
        Taker { server, slot }
    }

    /// A connection placed on the worker to serve inline, apart from its lane: no sibling's
    /// steal meets one.
    fn placed(self) -> Option<Item> {
        let conn = self.slot.lane.placed_rx.as_ref()?.try_recv().ok()?;
        Some(Item::Conn(conn))
    }

    /// What the worker owes its own sockets and wake hook ahead of any request: the
    /// next inbox event, else a due wake. None once a full run owes the lane a look.
    fn own_item(self) -> Option<Item> {
        let park = &self.slot.park;
        let mut run = park.own_run.load(Ordering::Relaxed);
        if run == WAKE_TURN {
            if let Some(wake) = self.wake() {
                return Some(wake);
            }
            run = 0;
            park.own_run.store(run, Ordering::Relaxed);
        }
        if run >= OWN_RUN {
            return None;
        }
        match self.slot.lane.ws_rx.as_ref()?.try_recv() {
            Ok(event) => {
                park.own_run.store(run + 1, Ordering::Relaxed);
                Some(Item::Ws(event))
            }
            Err(_) => self.wake(),
        }
    }

    /// The wake hook's turn, if due; it fills the run.
    fn wake(self) -> Option<Item> {
        if !self.slot.lane.take_wake() {
            return None;
        }
        self.slot.park.own_run.store(WAKE_RUN, Ordering::Relaxed);
        Some(Item::Wake)
    }

    /// The run starts over (a full run hands a due wake the next turn); true if it deferred anything.
    fn lane_looked(self) -> bool {
        let own_run = &self.slot.park.own_run;
        let run = own_run.load(Ordering::Relaxed);
        if run != 0 {
            let next = if run == OWN_RUN { WAKE_TURN } else { 0 };
            own_run.store(next, Ordering::Relaxed);
        }
        run >= OWN_RUN
    }

    /// A doorbell clears the pending flag before the inbox and the wake flag are
    /// read, so what arrives meanwhile rings again.
    fn lane_item(self, item: LaneItem) -> Option<Item> {
        match item {
            LaneItem::Request(ctx) => {
                self.lane_looked();
                Some(Item::Ctx(ctx))
            }
            LaneItem::Bell => {
                self.slot.lane.took_bell();
                self.own_item()
            }
        }
    }

    /// What a take does when its bounded wait times out: keep waiting (Nothing),
    /// or end the loop because the pool scaler retired this slot. A retired
    /// lane worker first empties its own lane, one request per timeout: the
    /// dispatcher stopped feeding it when the flag went up (see
    /// WorkerSlot::retire_at for the ordering), so whatever is
    /// still there is the last of it, and nothing already assigned is
    /// orphaned. Only reached with the queue momentarily empty, so the fast
    /// path pays nothing for it. A retired lane worker then waits for its
    /// sockets' closes (their handlers live on its thread), and any worker
    /// stays while its hijacked connections are open.
    fn after_timeout(self) -> Take {
        let slot = self.slot;
        if !slot.life.retired() || slot.hijacked.load(Ordering::SeqCst) > 0 {
            return Take::Nothing;
        }
        // Placed before the retirement: its own to serve (or close).
        if let Some(conn) = self.placed() {
            return Take::Item(conn);
        }
        let Some(lane_rx) = slot.lane.rx.as_ref() else {
            return Take::Closed;
        };
        while let Ok(item) = lane_rx.try_recv() {
            if let Some(taken) = self.lane_item(item) {
                return Take::Item(taken);
            }
        }
        if slot.sockets.open() > 0 {
            return Take::Nothing;
        }
        // A socket's close reaches the inbox before it stops counting.
        self.own_item().map_or(Take::Closed, Take::Item)
    }

    /// Requests only: a sibling's doorbell goes back to its owner, whose events stay with it.
    /// A worker that `waited` (a tick woke it, or a ring for nothing) leaves a lane to its own
    /// idle worker, which the send woke: taking from it would move one client's run of requests
    /// to a cold worker whenever a tick falls between a send and its take.
    fn steal(self, waited: bool) -> Option<Item> {
        if self.slot.life.seems_retired() {
            return None;
        }
        let slots = self.server.slots.read();
        let mut now = None;
        for other in slots.iter() {
            if Arc::ptr_eq(other, self.slot) {
                continue;
            }
            if waited && owner_waits(other, &mut now) {
                continue;
            }
            if let Some(rx) = other.lane.rx.as_ref() {
                match rx.try_recv() {
                    Ok(LaneItem::Request(ctx)) => return Some(Item::Ctx(ctx)),
                    Ok(LaneItem::Bell) => {
                        other.lane.took_bell();
                        other.notify();
                    }
                    Err(_) => {}
                }
            }
        }
        None
    }

    /// Inbox and wake (after the lane, once a full run owes it a look), a connection placed on
    /// it, the lane, then a steal. Closed once the lane is closed and the inbox empty.
    pub(crate) fn ready(self) -> Take {
        self.ready_after(false)
    }

    /// `ready`, for a worker that `waited` before this look or comes straight from its work.
    fn ready_after(self, waited: bool) -> Take {
        let lane_rx = self
            .slot
            .lane
            .rx
            .as_ref()
            .expect("a lane take without a lane");
        if let Some(item) = self.own_item().or_else(|| self.placed()) {
            return Take::Item(item);
        }
        loop {
            match lane_rx.try_recv() {
                Ok(item) => {
                    if let Some(taken) = self.lane_item(item) {
                        return Take::Item(taken);
                    }
                }
                // Placed as the lane closed: still taken, as its send came first under the lane
                // lock.
                Err(flume::TryRecvError::Disconnected) => {
                    return Take::from(self.placed()).or_else(|| self.closing_item())
                }
                Err(flume::TryRecvError::Empty) => break,
            }
        }
        if self.lane_looked() {
            if let Some(item) = self.own_item() {
                return Take::Item(item);
            }
        }
        self.steal(waited).into()
    }

    /// After shutdown closed the lane: the rest of the inbox past the run cap, so every
    /// on_close runs before the worker leaves. No wake: a self-waking hook would never let go.
    fn closing_item(self) -> Take {
        let inbox = self.slot.lane.ws_rx.as_ref();
        let event = inbox.and_then(|inbox| inbox.try_recv().ok());
        event.map_or(Take::Closed, |event| Take::Item(Item::Ws(event)))
    }

    /// Inbox and wake first (a doorbell that met a full lane is never sent), then a retired
    /// worker's exit, then a steal, so a backlog behind a slow sibling cannot outlive a tick.
    pub(crate) fn tick(self) -> Take {
        Take::from(self.own_item())
            .or_else(|| self.after_timeout())
            .or_else(|| self.steal(true).into())
    }

    /// The next item, waiting through `parker` (the GVL released; in place in tests). What is
    /// ready goes first with the GVL held, sparing two scheduler round-trips under load;
    /// a lane worker parks with its flag raised, so the dispatcher avoids it.
    /// A VM interrupt only wakes the park, or raises out of it.
    pub(super) fn take<P: Parker>(self, parker: &mut P) -> Result<Taken, P::Error> {
        let (server, slot) = (self.server, self.slot);
        if let Some(taken) = self.now().found() {
            return Ok(taken);
        }
        let Some(lane) = &slot.lane.rx else {
            return self.park_shared(parker);
        };

        // Park. The flag-then-recheck order closes the race with a dispatcher
        // that read parked=false just before we set it: anything it sent lands
        // in the lane, and recv_timeout checks the queue before sleeping.
        slot.park.park(server);
        let wait = || match lane.recv_timeout(TICK) {
            Ok(item) => self.lane_item(item).map(Some),
            Err(flume::RecvTimeoutError::Timeout) => self.tick().found(),
            Err(flume::RecvTimeoutError::Disconnected) => self.closing_item().found(),
        };
        let put_back = |taken| self.put_back_lane(taken);
        let taken = gvl::park(parker, slot.life.halted(), wait, put_back);
        crate::shared_gvl::got_back(server, slot);
        // Unpark before propagating any panic, or the dispatcher shuns this
        // lane for good.
        slot.park.unpark();
        let taken = taken?.flatten();
        slot.park.took(taken.as_ref());
        Ok(taken)
    }

    /// What is there without a wait, the GVL held: `ready` for a lane worker, else the shared
    /// queue's next request, which a retired worker leaves.
    pub(super) fn now(self) -> Take {
        if self.slot.lane.rx.is_some() {
            let take = self.ready();
            if !matches!(take, Take::Nothing) {
                self.slot.park.took(take.item());
            }
            return take;
        }
        if self.slot.life.seems_retired() {
            return Take::Nothing;
        }
        match self.server.req_rx.try_recv() {
            Ok(ctx) => Take::Item(Item::Ctx(ctx)),
            Err(flume::TryRecvError::Disconnected) => Take::Closed,
            Err(flume::TryRecvError::Empty) => Take::Nothing,
        }
    }

    /// A wait on the shared queue. A retired worker takes nothing new, and leaves as
    /// after_timeout says, or at close.
    fn park_shared<P: Parker>(self, parker: &mut P) -> Result<Taken, P::Error> {
        let (queue, life) = (&self.server.req_rx, &self.slot.life);
        let retired = || life.seems_retired();
        let wait = || {
            if !retired() {
                return match queue.recv_timeout(TICK) {
                    Ok(ctx) => Some(Some(Item::Ctx(ctx))),
                    Err(flume::RecvTimeoutError::Timeout) => None,
                    Err(flume::RecvTimeoutError::Disconnected) => Some(None),
                };
            }
            if queue.is_disconnected() {
                return Some(None);
            }
            self.after_timeout().found().or_else(|| {
                std::thread::sleep(TICK);
                None
            })
        };
        let put_back = |taken| self.put_back_shared(taken);
        let taken = gvl::park(parker, life.halted(), wait, put_back);
        crate::shared_gvl::got_back(self.server, self.slot);
        Ok(taken?.flatten())
    }

    /// A request taken as a Thread#raise came goes back at the queue's end, for whichever worker
    /// takes next; held when the queue is closed or full, so the raise answers it at once.
    fn put_back_shared(self, taken: Taken) -> Back<Taken> {
        let Some(Item::Ctx(ctx)) = taken else {
            return Back::HandedOver(taken);
        };
        let Some(tx) = self.server.req_tx.lock().clone() else {
            return Back::Held(Some(Item::Ctx(ctx)));
        };
        match tx.try_send(ctx) {
            Ok(()) => Back::Waits,
            Err(refused) => Back::Held(Some(Item::Ctx(refused.into_inner()))),
        }
    }

    /// A request taken as a Thread#raise came goes back at its lane's end for the next take (a
    /// crash answers it with the rest of the lane), held when there is no room; a wake is due
    /// again. A socket event is handed over as taken, so its socket's events stay in order: the
    /// raise fires before its callback, losing only that event.
    fn put_back_lane(self, taken: Taken) -> Back<Taken> {
        match taken {
            Some(Item::Ctx(ctx)) => match self.slot.send_lane(LaneItem::Request(ctx)) {
                Ok(()) => Back::Waits,
                Err(refused) => match refused.into_inner() {
                    LaneItem::Request(ctx) => Back::Held(Some(Item::Ctx(ctx))),
                    LaneItem::Bell => unreachable!("a request went in"),
                },
            },
            Some(Item::Wake) if self.slot.wake() => Back::Waits,
            Some(Item::Wake) => Back::Held(Some(Item::Wake)),
            other => {
                debug_assert!(
                    !matches!(other, Some(Item::Conn(_))),
                    "a park took a placed connection"
                );
                Back::HandedOver(other)
            }
        }
    }

    /// worker_loop's take, of what its loop has `room` for. Nothing: the doorbell is armed, and
    /// the loop waits on it; the ticker rings it meanwhile, so this is its lane tick too.
    /// - All: what `ready` finds, else a retired worker's exit. Parked while it waits.
    /// - Requests (its backlog of events full): its lane's requests; the inbox waits until the
    ///   backlog drains, pushing back on its sockets. Never Closed: the full take reports a
    ///   closed lane, after what the inbox still holds.
    /// - Events (every job slot busy): socket events and the wake hook, never Closed. A full
    ///   worker is not idle.
    pub(crate) fn nonblock(self, room: Room) -> Take {
        // Parked: it comes from a wait on its doorbell, not from a job.
        let waited = self.slot.park.is_parked();
        let all = |taker: Self| taker.ready_after(waited).or_else(|| taker.after_timeout());
        match room {
            Room::All => self.take_with_room(all, true),
            Room::Requests => {
                let requests = |taker: Self| taker.placed().or_else(|| taker.lane_request());
                self.take_with_room(|taker| requests(taker).into(), true)
            }
            Room::Events => {
                let events = |taker: Self| taker.own_ready().or_else(|| taker.placed());
                self.take_with_room(|taker| events(taker).into(), false)
            }
            Room::Connections => self.take_placed(),
        }
    }

    fn doorbell(self) -> &'a crate::doorbell::Doorbell {
        let doorbell = self.slot.lane.doorbell.as_ref();
        doorbell.expect("a worker_loop take without a doorbell")
    }

    /// One worker_loop take: `look`, and if it finds nothing, arm and look again, as a send just
    /// before the arm found the doorbell disarmed and did not ring. Only a worker with a job slot
    /// free is `idle`: it prunes its in-flight list (what still counts keeps dispatch away from
    /// a full one) and parks while it waits.
    fn take_with_room(self, look: impl Fn(Self) -> Take, idle: bool) -> Take {
        let (server, slot, doorbell) = (self.server, self.slot, self.doorbell());
        let now = slot.load.back_in_loop();
        if idle && slot.load.keep_open() {
            server.lane_room.notify();
        }
        let found = look(self).or_else(|| {
            if idle {
                slot.park.park(server);
            }
            doorbell.arm();
            look(self)
        });
        match &found {
            Take::Nothing => {
                slot.load.waits_on_doorbell(now);
                return found;
            }
            Take::Item(_) => slot.load.hold(HOLDING_ITEM),
            Take::Closed => slot.load.loop_ended(),
        }
        match idle {
            true => slot.park.awake(doorbell),
            false => doorbell.disarm(),
        }
        slot.park.took(found.item());
        found
    }

    /// Requests alone: a doorbell found in the lane is dropped, the inbox read once the backlog
    /// drains.
    fn lane_request(self) -> Option<Item> {
        let lane_rx = self.slot.lane.rx.as_ref().expect("a take without lane");
        while let Ok(item) = lane_rx.try_recv() {
            match item {
                LaneItem::Request(ctx) => {
                    self.lane_looked();
                    return Some(Item::Ctx(ctx));
                }
                LaneItem::Bell => self.slot.lane.took_bell(),
            }
        }
        None
    }

    /// Room::Connections: it neither arms the doorbell nor parks the slot.
    fn take_placed(self) -> Take {
        let slot = self.slot;
        let now = slot.load.back_in_loop();
        let Some(conn) = self.placed() else {
            slot.load.waits_on_doorbell(now);
            return Take::Nothing;
        };
        slot.load.hold(HOLDING_ITEM);
        slot.park.took(Some(&conn));
        Take::Item(conn)
    }

    /// With the lane out of play, a full run's look at it is moot: the run restarts, a due wake first.
    fn own_ready(self) -> Option<Item> {
        self.own_item()
            .or_else(|| self.lane_looked().then(|| self.own_item()).flatten())
    }
}
