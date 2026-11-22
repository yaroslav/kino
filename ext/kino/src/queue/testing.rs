//! Test helpers for the takes: a worker's takes as its loop makes them,
//! and the dispatcher's and Ruby's parts.

use std::sync::Arc;

use super::worker::enlist;
use super::{Item, Room, Take, Taker, Worker};
use crate::registry::{BoxedCtx, ServerInner};
use crate::slot::WorkerSlot;
use crate::ws::WsEvent;

/// What a take found, by name.
pub fn kind(take: &Take) -> &'static str {
    match take {
        Take::Item(Item::Ctx(_)) => "request",
        Take::Item(Item::Ws(_)) => "event",
        Take::Item(Item::Wake) => "wake",
        Take::Item(Item::Conn(_)) => "connection",
        Take::Closed => "the end",
        Take::Nothing => "nothing",
    }
}

/// What `slot`'s worker finds ready, by name (queue::testing::kind).
pub fn ready(server: &ServerInner, slot: &Arc<WorkerSlot>) -> &'static str {
    kind(&Taker::new(server, slot).ready())
}

/// What `slot`'s parked worker finds at a tick, by name.
pub fn tick(server: &ServerInner, slot: &Arc<WorkerSlot>) -> &'static str {
    kind(&Taker::new(server, slot).tick())
}

/// What `slot`'s worker_loop worker takes with `room`, by name; "nothing" leaves it armed.
pub fn nonblock(server: &ServerInner, slot: &Arc<WorkerSlot>, room: Room) -> &'static str {
    kind(&Taker::new(server, slot).nonblock(room))
}

/// The request a take found; panics naming what it found instead.
pub fn expect_request(take: Take) -> BoxedCtx {
    match take {
        Take::Item(Item::Ctx(ctx)) => ctx,
        other => panic!("expected a request, took {}", kind(&other)),
    }
}

/// Whether the doorbell of `worker` (its handle) rang, as a scheduler
/// waiting on its ready_fd sees it; the ring is taken, as woke takes it.
pub fn rung(worker: &Worker) -> bool {
    rings(Worker::ready_fd(worker).expect("a worker_loop worker")) > 0
}

/// How many times the doorbell waited on through `fd` rang since it
/// was last emptied, which this empties. The caller keeps `fd` open.
pub fn rings(fd: std::os::fd::RawFd) -> usize {
    use std::io::Read;
    use std::os::fd::BorrowedFd;
    // SAFETY: the caller keeps the descriptor open; reading goes
    // through a duplicate, closed on return.
    let fd = unsafe { BorrowedFd::borrow_raw(fd) }
        .try_clone_to_owned()
        .expect("a duplicate of the doorbell");
    let mut rings = [0u8; 64];
    std::os::unix::net::UnixStream::from(fd)
        .read(&mut rings)
        .unwrap_or(0)
}

/// Slot `index` and the handle its worker holds from boot on, as
/// Kino::Native.worker makes it.
pub fn boot(server: &Arc<ServerInner>, index: usize) -> (Arc<WorkerSlot>, Worker) {
    let slot = crate::testing::slot(server, index);
    let worker = Worker::new(server, index, slot.clone());
    (slot, worker)
}

/// A socket's client sends message `n` (its text is the number); it
/// lands in `slot`'s inbox, as the socket's reader puts it there.
pub fn socket_sends(slot: &WorkerSlot, n: usize) {
    let event = WsEvent::Message {
        id: 1,
        data: bytes::Bytes::from(n.to_string()),
        text: true,
    };
    let inbox = slot.lane.ws_tx.as_ref().expect("inbox");
    inbox.try_send(event).expect("inbox has room");
}

/// The next request `slot`'s worker_loop takes, admitted as the
/// take_nonblock native admits it (the env aside).
pub fn admit_next(server: &ServerInner, slot: &Arc<WorkerSlot>) -> BoxedCtx {
    admitted(server, slot, Taker::new(server, slot).nonblock(Room::All))
}

/// The next request `slot`'s classic lane worker takes without
/// parking (its lane, then a steal), admitted as take_one admits it
/// (the env aside).
pub fn take_admitted(server: &ServerInner, slot: &Arc<WorkerSlot>) -> BoxedCtx {
    admitted(server, slot, Taker::new(server, slot).ready())
}

/// The request a take found, admitted by `slot`.
pub fn admitted(server: &ServerInner, slot: &Arc<WorkerSlot>, took: Take) -> BoxedCtx {
    let mut ctx = expect_request(took);
    enlist(server, slot, &mut ctx);
    ctx
}
