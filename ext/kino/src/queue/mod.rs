//! The worker-side bridge: batched request intake and the fused
//! respond-and-take call. This is where FFI crossings per request went
//! from three in early designs (take, env, respond) to amortized ~one.
//!
//! Blocking discipline (everywhere in this crate): bounded `recv_timeout`
//! ticks + an AtomicBool interrupt flag. No flume::Selector: it loses
//! wakeups under churn, observed as workers going permanently deaf to a
//! non-empty queue after ~100k requests.

use std::time::Duration;

use crate::registry::BoxedCtx;
use crate::ws::WsEvent;

mod take;
#[cfg(test)]
pub(crate) mod testing;
#[cfg(test)]
mod tests;
mod worker;

#[cfg(test)]
pub(crate) use take::{Take, Taker};
pub use worker::{respond_and_take, respond_and_take_one, wake_worker, worker, Worker};

pub const TICK: Duration = Duration::from_millis(50);

pub enum Item {
    Ctx(BoxedCtx),
    Ws(WsEvent),
    Wake,
}

/// None = shutdown (queue closed or interrupted); the caller can't tell
/// the difference and doesn't need to.
pub(crate) type Taken = Option<Item>;
