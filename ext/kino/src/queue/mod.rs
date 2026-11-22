//! The worker-side bridge: batched request intake and the fused
//! respond-and-take call, which make a request's take, env and answer
//! about one FFI crossing.
//!
//! Blocking discipline (everywhere in this crate): bounded `recv_timeout`
//! ticks + an AtomicBool interrupt flag. No flume::Selector: it loses
//! wakeups under churn, observed as workers going permanently deaf to a
//! non-empty queue after ~100k requests.

use std::time::Duration;

use crate::inline::InlineConn;
use crate::registry::BoxedCtx;
use crate::ws::WsEvent;

mod take;
#[cfg(test)]
pub(crate) mod testing;
#[cfg(test)]
mod tests;
mod worker;

#[cfg(test)]
pub(crate) use take::{Room, Take, Taker};
pub(crate) use worker::admit_inline;
#[cfg(test)]
pub(crate) use worker::enlist_inline;
pub use worker::{
    lingering_loops, respond_and_take, respond_and_take_one, wake_worker, worker, Worker,
};

pub const TICK: Duration = Duration::from_millis(50);

pub enum Item {
    Ctx(BoxedCtx),
    Ws(WsEvent),
    Wake,
    Conn(Box<InlineConn>),
}

/// None = shutdown (queue closed, or slot halted past the deadline); the caller can't tell
/// the difference and doesn't need to.
pub(crate) type Taken = Option<Item>;

/// One bounded wait on `rx`, as a park's attempt reports it: None to wait on, Some(None) once
/// the channel is closed and empty.
#[inline]
pub(crate) fn recv_tick<T>(rx: &flume::Receiver<T>) -> Option<Option<T>> {
    match rx.recv_timeout(TICK) {
        Ok(found) => Some(Some(found)),
        Err(flume::RecvTimeoutError::Timeout) => None,
        Err(flume::RecvTimeoutError::Disconnected) => Some(None),
    }
}
