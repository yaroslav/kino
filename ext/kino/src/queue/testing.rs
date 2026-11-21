//! Test helpers for the takes: a worker's takes as its loop makes them.

use std::sync::Arc;

use super::{Item, Take, Taker, Worker};
use crate::registry::{BoxedCtx, ServerInner};
use crate::slot::WorkerSlot;
use crate::ws::WsEvent;

/// What a take found, by name.
pub fn kind(take: &Take) -> &'static str {
    match take {
        Take::Item(Item::Ctx(_)) => "request",
        Take::Item(Item::Ws(_)) => "event",
        Take::Item(Item::Wake) => "wake",
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

/// The request a take found; panics naming what it found instead.
pub fn expect_request(take: Take) -> BoxedCtx {
    match take {
        Take::Item(Item::Ctx(ctx)) => ctx,
        other => panic!("expected a request, took {}", kind(&other)),
    }
}

/// Slot `index` and the handle its worker holds from boot on, as
/// Kino::Native.worker makes it.
pub fn boot(server: &Arc<ServerInner>, index: usize) -> (Arc<WorkerSlot>, Worker) {
    let slot = server.slots.read()[index].clone();
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
