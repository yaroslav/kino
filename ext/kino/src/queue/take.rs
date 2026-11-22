//! A worker's takes. What is ready goes first: its sockets' events and its wake, its lane, a
//! sibling's lane. With nothing ready the worker parks (Taker::take).

use std::sync::atomic::Ordering;
use std::sync::Arc;

use magnus::Error;

use super::{Item, Taken, TICK};
use crate::registry::{LaneItem, ServerInner};
use crate::slot::WorkerSlot;

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

/// One bounded wait of a take: None to go around again.
pub(super) type Attempt<'a> = &'a mut dyn FnMut() -> Option<Taken>;
/// What the wait made of its attempts: Ok(None) once interrupted.
pub(super) type WaitResult = Result<Option<Taken>, Error>;

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
    fn steal(self) -> Option<Item> {
        if self.slot.life.seems_retired() {
            return None;
        }
        let slots = self.server.slots.read();
        for other in slots.iter() {
            if Arc::ptr_eq(other, self.slot) {
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

    /// Inbox and wake (after the lane, once a full run owes it a look), the lane, then a
    /// steal. Closed once the lane is closed and the inbox empty.
    pub(crate) fn ready(self) -> Take {
        let lane_rx = self.slot.lane.rx.as_ref().expect("take_ready without lane");
        if let Some(item) = self.own_item() {
            return Take::Item(item);
        }
        loop {
            match lane_rx.try_recv() {
                Ok(item) => {
                    if let Some(taken) = self.lane_item(item) {
                        return Take::Item(taken);
                    }
                }
                Err(flume::TryRecvError::Disconnected) => return self.closing_item(),
                Err(flume::TryRecvError::Empty) => break,
            }
        }
        if self.lane_looked() {
            if let Some(item) = self.own_item() {
                return Take::Item(item);
            }
        }
        self.steal().into()
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
            .or_else(|| self.steal().into())
    }

    /// The next item, waiting in `wait` (the GVL released; in place in tests). What is
    /// ready goes first with the GVL held, sparing two scheduler round-trips under load;
    /// a lane worker parks with its flag raised, so the dispatcher avoids it.
    pub(super) fn take(self, wait: impl FnOnce(Attempt<'_>) -> WaitResult) -> Result<Taken, Error> {
        let (server, slot) = (self.server, self.slot);
        let Some(lane) = &slot.lane.rx else {
            return self.take_shared(wait);
        };
        if let Some(taken) = self.ready().found() {
            slot.park.took(taken.as_ref());
            return Ok(taken);
        }

        // Park. The flag-then-recheck order closes the race with a dispatcher
        // that read parked=false just before we set it: anything it sent lands
        // in the lane, and recv_timeout checks the queue before sleeping.
        slot.park.park(server);
        let taken = wait(&mut || match lane.recv_timeout(TICK) {
            Ok(item) => self.lane_item(item).map(Some),
            Err(flume::RecvTimeoutError::Timeout) => self.tick().found(),
            Err(flume::RecvTimeoutError::Disconnected) => self.closing_item().found(),
        });
        // Unpark before propagating any panic, or the dispatcher shuns this
        // lane for good.
        slot.park.unpark();
        let taken = taken?.flatten();
        slot.park.took(taken.as_ref());
        Ok(taken)
    }

    /// From the shared queue. A retired worker takes nothing new, and leaves as after_timeout
    /// says, or at close.
    fn take_shared(self, wait: impl FnOnce(Attempt<'_>) -> WaitResult) -> Result<Taken, Error> {
        let (queue, life) = (&self.server.req_rx, &self.slot.life);
        let retired = || life.seems_retired();
        if !retired() {
            match queue.try_recv() {
                Ok(ctx) => return Ok(Some(Item::Ctx(ctx))),
                Err(flume::TryRecvError::Disconnected) => return Ok(None),
                Err(flume::TryRecvError::Empty) => {}
            }
        }
        let taken = wait(&mut || {
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
        });
        Ok(taken?.flatten())
    }
}
