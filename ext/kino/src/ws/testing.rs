//! What the WebSocket tests share: client frames and handshakes, counts of
//! closes and refusals, and sockets with no connection behind them.

use std::future::Future;
use std::task::{Context, Waker};

use tokio::sync::mpsc::{self, error::TryRecvError};

use super::handshake::{register, Registration};
use super::socket::{Abort, Out};
use super::topics::{subscribe_with, Wait, WhenFull};
use super::*;
use crate::counters::Tallied;

pub use super::codec::encode;

/// A masked client frame (RFC 6455 5.3), the only kind a client may
/// send: `first` is the FIN bit and opcode.
pub fn client_frame(first: u8, payload: &[u8]) -> Vec<u8> {
    const MASK: [u8; 4] = [0x37, 0xfa, 0x21, 0x3d];
    let mut out = vec![first];
    match payload.len() {
        n if n < 126 => out.push(0x80 | n as u8),
        n if n < 65_536 => {
            out.push(0x80 | 126);
            out.extend_from_slice(&(n as u16).to_be_bytes());
        }
        n => {
            out.push(0x80 | 127);
            out.extend_from_slice(&(n as u64).to_be_bytes());
        }
    }
    out.extend_from_slice(&MASK);
    out.extend(payload.iter().enumerate().map(|(i, b)| b ^ MASK[i & 3]));
    out
}

/// A client's opening handshake (RFC 6455 4.1) with the RFC's sample
/// key.
pub fn handshake() -> http::request::Builder {
    hyper::Request::get("/ws")
        .header(http::header::HOST, "kino.test")
        .header(http::header::UPGRADE, "websocket")
        .header(http::header::CONNECTION, "Upgrade")
        .header(http::header::SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==")
        .header(http::header::SEC_WEBSOCKET_VERSION, "13")
}

/// Sockets closed since `before`, by cause.
pub fn closes_since(before: &WsSnapshot) -> Vec<(&'static str, u64)> {
    moved(Cause::named(snapshot().closed), Cause::named(before.closed))
}

/// Handshakes refused since `before`, by cause.
pub fn refusals_since(before: &WsSnapshot) -> Vec<(&'static str, u64)> {
    let refused = |snapshot: &WsSnapshot| Refusal::named(snapshot.handshakes_refused);
    moved(refused(&snapshot()), refused(before))
}

fn moved<const N: usize>(
    now: [(&'static str, u64); N],
    then: [(&'static str, u64); N],
) -> Vec<(&'static str, u64)> {
    let since = now.into_iter().zip(then);
    let since = since.map(|((cause, now), (_, then))| (cause, now - then));
    since.filter(|(_, count)| *count > 0).collect()
}

/// A registered socket with no connection behind it: the test reads
/// its queue of outgoing frames, as its writer would. Dropping it
/// ends the socket.
pub struct QueuedSocket {
    pub id: u64,
    out: mpsc::Receiver<Out>,
    abort: Arc<Abort>,
    registration: Registration,
}

impl QueuedSocket {
    /// A socket whose queue holds `queue` frames.
    pub fn new(queue: usize) -> Self {
        let limits = Limits {
            queue,
            ..Limits::default()
        };
        Self::with_limits(Arc::new(WorkerSlot::new(true, 1)), limits)
    }

    /// A socket the worker on `owner` accepted, with these budgets.
    pub fn with_limits(owner: Arc<WorkerSlot>, limits: Limits) -> Self {
        // Its `reply` sender goes: nothing reads from this socket.
        let (_, switched) = tokio::sync::oneshot::channel();
        let (id, mut accepted) =
            register(owner, &Arc::new(limits), switched).expect("no websocket_max");
        accepted.registration.open();
        QueuedSocket {
            id,
            out: accepted.out_rx,
            abort: accepted.registration.abort.clone(),
            registration: accepted.registration,
        }
    }

    /// The next frame queued for it, which leaves the byte budget as
    /// once written; panics after five seconds.
    pub fn next_frame(&mut self) -> Bytes {
        let what = format!("a frame queued for socket {}", self.id);
        crate::testing::eventually_blocking(&what, || match self.out.try_recv() {
            Ok(Out::Frame(frame)) => {
                self.abort
                    .queued_bytes
                    .fetch_sub(frame.len(), Ordering::SeqCst);
                self.abort.queued_frames.fetch_sub(1, Ordering::SeqCst);
                Some(frame)
            }
            Ok(Out::Control(frame) | Out::Ping(frame)) => Some(frame),
            Ok(Out::Close(_)) => panic!("socket {} got a close frame", self.id),
            Err(TryRecvError::Disconnected) => {
                panic!("socket {} ended with nothing queued", self.id)
            }
            Err(TryRecvError::Empty) => None,
        })
    }

    /// Whether something asked its connection to drop.
    pub fn aborted(&self) -> bool {
        let notified = std::pin::pin!(self.abort.notify.notified());
        notified
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    }

    /// Whether nothing can queue frames to it any more: its writer
    /// would flush and end.
    pub fn released(&self) -> bool {
        self.out.is_closed()
    }

    /// End it, as its connection task does when the client goes.
    pub fn close(&self) {
        self.registration.unregister();
        self.registration.uncount();
    }
}

/// Whether socket `id` is unregistered; None while CONNS is held or
/// awaited, so a test can poll it without queuing on the lock.
pub fn unregistered(id: u64) -> Option<bool> {
    CONNS.try_read().map(|conns| !conns.contains_key(&id))
}

/// `subscribe_with`, waiting for the lock.
pub fn subscribe(id: u64, topic: &[u8], prefix: &[u8], suffix: &[u8], when_full: WhenFull) -> bool {
    let [topic, prefix, suffix] = [topic, prefix, suffix].map(Bytes::copy_from_slice);
    subscribe_with(Wait::Block, id, &topic, &prefix, &suffix, when_full).unwrap_or(false)
}
