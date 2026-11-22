//! Native WebSockets: the id-keyed API over the socket registry. A socket belongs to the
//! slot that accepted it: its messages land in that slot's inbox in order, reading pauses
//! at its inbound credit, and writes never block Ruby (a full queue returns false).

mod codec;
mod drive;
mod handshake;
#[cfg(test)]
mod server_tests;
mod socket;
mod stats;
#[cfg(test)]
pub mod testing;
pub mod topics;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use bytes::Bytes;
use magnus::prelude::*;
use magnus::{Error, RString, Ruby};
use parking_lot::RwLock;

pub(crate) use codec::Opcode;
use drive::drain_inbox;
pub(crate) use drive::drive;
pub use handshake::{accept, Accept, Offer};
pub(crate) use handshake::{offer, Accepted};
use socket::update;
use socket::Conn;
pub use socket::SlotSockets;
pub use stats::{snapshot, Cause, Refusal, WsCount, WsSnapshot};

use crate::registry::ServerInner;
use crate::slot::WorkerSlot;

/// Outgoing frames a socket may have queued; past this, writes fail.
const QUEUE_FRAMES: usize = 256;
/// Bytes of outgoing frames a socket may have queued; whichever budget fills first counts.
const QUEUE_BYTES: usize = 4 << 20;
/// Messages of a socket its worker may have waiting or in a callback.
pub const INBOUND_CREDIT: usize = 16;
/// Largest message, all fragments together (`websocket_max_message`).
pub const DEFAULT_MAX_MESSAGE: usize = 1 << 20;
/// Silence before a ping; silent for two, the client is dropped (`websocket_ping_interval`).
pub const DEFAULT_PING_INTERVAL: Duration = Duration::from_secs(30);

/// A server's WebSocket budgets and its open count; the queue and inbound budgets are fixed.
pub struct Limits {
    pub max_message: usize,
    pub queue: usize,
    pub queue_bytes: usize,
    pub inbound: usize,
    /// Zero: no pings, and no client is dropped for its silence.
    pub ping_interval: Duration,
    /// Most sockets open at once (`websocket_max`); 0: no cap.
    pub max: usize,
    pub(crate) open: AtomicUsize,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            max_message: DEFAULT_MAX_MESSAGE,
            queue: QUEUE_FRAMES,
            queue_bytes: QUEUE_BYTES,
            inbound: INBOUND_CREDIT,
            ping_interval: DEFAULT_PING_INTERVAL,
            max: 0,
            open: AtomicUsize::new(0),
        }
    }
}

impl Limits {
    fn open_one(&self) -> bool {
        update(&self.open, |open| {
            (self.max == 0 || open < self.max).then_some(open + 1)
        })
        .is_ok()
    }
}

/// How long a client has to answer Kino's close, and a writer to flush, before the drop.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

/// The longest a drain spreads its 1001s over, so clients do not all reconnect at once.
const CLOSE_SPREAD: Duration = Duration::from_secs(1);

/// Half the drain's `left` seconds, at most CLOSE_SPREAD (unbounded: all of it; past or NaN: none).
pub fn close_spread(left: f64) -> Duration {
    if left > 0.0 {
        Duration::try_from_secs_f64(left / 2.0).map_or(CLOSE_SPREAD, |half| half.min(CLOSE_SPREAD))
    } else {
        Duration::ZERO
    }
}

pub const GOING_AWAY: u16 = 1001;
pub const SERVER_ERROR: u16 = 1011;

/// What a socket hands its worker: a message, on_shutdown's turn, on_drained, or its end.
#[derive(Debug, PartialEq, Eq)]
pub enum WsEvent {
    Message { id: u64, data: Bytes, text: bool },
    Shutdown { id: u64 },
    Drained { id: u64 },
    Close { id: u64 },
}

impl WsEvent {
    pub fn into_ruby(self, ruby: &Ruby) -> Result<magnus::Value, Error> {
        let ary = ruby.ary_new_capa(3);
        let (id, kind) = match self {
            WsEvent::Message { id, data, text } => {
                ary.push(id)?;
                ary.push(ruby.to_symbol("message"))?;
                let s = ruby.str_from_slice(&data);
                if text {
                    s.enc_associate(ruby.utf8_encindex())?;
                }
                ary.push(s)?;
                return Ok(ary.as_value());
            }
            WsEvent::Shutdown { id } => (id, "shutdown"),
            WsEvent::Drained { id } => (id, "drained"),
            WsEvent::Close { id } => (id, "close"),
        };
        ary.push(id)?;
        ary.push(ruby.to_symbol(kind))?;
        Ok(ary.as_value())
    }
}

static NEXT_ID: AtomicU64 = AtomicU64::new(1);
static CONNS: LazyLock<RwLock<HashMap<u64, Conn>>> = LazyLock::new(Default::default);

/// Forget a socket and its subscriptions. CONNS is released before the topic lock: a
/// fan-out holds that one a while, and writes to every other socket need CONNS meanwhile.
/// A socket that never subscribed skips the topic lock.
fn unregister(id: u64) {
    let mut conns = CONNS.write();
    let removed = conns.remove(&id);
    if let Some(conn) = &removed {
        conn.abort.gone.store(true, Ordering::SeqCst);
    }
    drop(conns);
    if removed.is_some_and(|conn| conn.abort.subscribed.load(Ordering::SeqCst)) {
        topics::forget(id);
    }
}

pub fn registered(id: u64) -> bool {
    CONNS.read().contains_key(&id)
}

/// `then` on socket `id` under the registry's read lock.
fn with<T>(id: u64, then: impl FnOnce(&Conn) -> T) -> Option<T> {
    CONNS.read().get(&id).map(then)
}

/// A callback returned, or the message was dropped: the reader may read another.
pub fn handled(id: u64) {
    with(id, |conn| conn.abort.return_credit());
}

/// on_shutdown ran, or will not: the drain may send its 1001.
pub fn shutdown_ran(id: u64) {
    with(id, |conn| conn.abort.goodbye.notify_one());
}

pub fn pending(id: u64) -> usize {
    with(id, |conn| conn.abort.pending()).unwrap_or(0)
}

/// False once the socket is gone or its queue is full; the socket stays open either way.
pub fn write(id: u64, opcode: Opcode, payload: &[u8]) -> bool {
    let frame = codec::encode(opcode, payload);
    with(id, |conn| conn.write(frame)).unwrap_or(false)
}

/// Complete frames as they are (server frames are unmasked, so one suits every socket).
pub fn write_raw(id: u64, frames: &[u8]) -> bool {
    if frames.is_empty() {
        return registered(id);
    }
    let frames = Bytes::copy_from_slice(frames);
    with(id, |conn| conn.write(frames)).unwrap_or(false)
}

/// After what is queued; the client has CLOSE_TIMEOUT to answer. The first close stands.
pub fn close(id: u64, code: u16) -> bool {
    with(id, |conn| conn.close(code)).unwrap_or(false)
}

/// `code` to each of `slot`'s sockets (dropped on a full queue); handshakes refused until reset.
pub fn close_slot(slot: &WorkerSlot, code: u16, cause: Cause) -> usize {
    slot.sockets.close_code.store(code, Ordering::SeqCst);
    let conns = CONNS.read();
    let mut had = 0;
    for conn in conns.values().filter(|conn| conn.owned_by(slot)) {
        if !conn.abort.close(&conn.tx, code, cause) {
            conn.abort.drop_as(cause);
        }
        had += 1;
    }
    had
}

/// The drain's goodbye: refuse handshakes, run each on_shutdown, then its 1001, the sockets'
/// turns spread over `spread`, oldest first. How many there were.
pub fn leave_all(server: &ServerInner, spread: Duration) -> usize {
    let slots = server.slots.read().clone();
    for slot in &slots {
        slot.sockets.close_code.store(GOING_AWAY, Ordering::SeqCst);
    }
    let mut leaving: Vec<(u64, Arc<socket::Abort>)> = CONNS
        .read()
        .iter()
        .filter(|(_, conn)| slots.iter().any(|slot| conn.owned_by(slot)))
        .map(|(id, conn)| (*id, conn.abort.clone()))
        .collect();
    leaving.sort_unstable_by_key(|(id, _)| *id);
    let count = leaving.len();
    for (turn, (_, abort)) in leaving.iter().enumerate() {
        let after = spread.mul_f64(turn as f64 / count as f64);
        abort
            .leave_after_us
            .store(after.as_micros() as u64, Ordering::SeqCst);
        abort.leave.notify_one();
    }
    count
}

/// No worker takes `slot`'s events any more: drop them, waiting ones too, so no socket waits on
/// an unread inbox, on credit a stuck callback holds, or on an on_shutdown that never returns.
pub fn abandon(slot: &WorkerSlot) {
    slot.sockets.abandoned.store(true, Ordering::SeqCst);
    drain_inbox(slot);
    for conn in CONNS.read().values().filter(|conn| conn.owned_by(slot)) {
        conn.abort.credit.notify_one();
        conn.abort.goodbye.notify_one();
    }
}

/// Crashed or wedged: abandon the events and close with 1011, the handlers gone. How many.
pub fn give_up_slot(slot: &WorkerSlot) -> usize {
    abandon(slot);
    close_slot(slot, SERVER_ERROR, Cause::Crash)
}

/// Before a new worker: the last one's sockets lost their handlers, so close them with 1011.
pub fn hand_over(slot: &WorkerSlot) {
    close_slot(slot, SERVER_ERROR, Cause::Crash);
    drain_inbox(slot);
}

/// Drop the connection now, queued frames and all; works on a full queue.
pub fn abort(id: u64) -> bool {
    drop_as(id, Cause::Normal)
}

/// `abort` for a socket whose callback raised: it counts as a crash.
pub fn fail(id: u64) -> bool {
    drop_as(id, Cause::Crash)
}

fn drop_as(id: u64, cause: Cause) -> bool {
    with(id, |conn| conn.abort.drop_as(cause)).is_some()
}

pub fn ws_write(id: u64, data: RString) -> bool {
    write(id, Opcode::Text, unsafe { data.as_slice() })
}

pub fn ws_write_binary(id: u64, data: RString) -> bool {
    write(id, Opcode::Binary, unsafe { data.as_slice() })
}

pub fn ws_write_raw(id: u64, frames: RString) -> bool {
    write_raw(id, unsafe { frames.as_slice() })
}

/// ArgumentError for a code no endpoint may send.
pub fn ws_close(ruby: &Ruby, id: u64, code: i64) -> Result<bool, Error> {
    match u16::try_from(code) {
        Ok(code) if codec::valid_close_code(code) => Ok(close(id, code)),
        _ => Err(Error::new(
            ruby.exception_arg_error(),
            format!("invalid close code {code}: send 1000-1003, 1007-1011 or 3000-4999"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_drain_spreads_its_closes_over_half_what_is_left_up_to_a_second() {
        let ms = Duration::from_millis;
        for (left, spread) in [
            (f64::INFINITY, CLOSE_SPREAD),
            (1e30, CLOSE_SPREAD),
            (2.0, CLOSE_SPREAD),
            (0.4, ms(200)),
            (0.0, Duration::ZERO),
            (f64::NEG_INFINITY, Duration::ZERO),
            (f64::NAN, Duration::ZERO),
        ] {
            assert_eq!(close_spread(left), spread, "{left} s left");
        }
    }
}
