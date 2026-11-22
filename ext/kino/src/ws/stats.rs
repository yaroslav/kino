//! The process's WebSocket counters: closes by cause, refused handshakes, and what moved.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::counters::{tallied, Tallied, Tally};

tallied! {
    /// Why a socket ended; the first cause recorded stands. From 1: Abort::cause holds 0 for none.
    pub enum Cause {
        /// A close frame from either side, or the app's abort.
        Normal = 1 => "normal",
        /// Kino's 1001: the server drained, or the worker retired.
        GoingAway => "going_away",
        Protocol => "protocol",
        TooBig => "too_big",
        Slow => "slow",
        DeadPeer => "dead_peer",
        ClientGone => "client_gone",
        /// Kino's 1011 (its worker crashed or was given up on), or a callback raised.
        Crash => "crash",
        /// The 101 never went out: the client never got the socket.
        HandshakeFailed => "handshake_failed",
    }
}

impl Cause {
    pub(super) fn of(code: u8) -> Option<Cause> {
        Cause::ALL.get(usize::from(code).checked_sub(1)?).copied()
    }
}

tallied! {
    /// Why Kino answered a handshake the app accepted with a 503.
    pub enum Refusal {
        WebsocketMax => "websocket_max",
        Draining => "draining",
        WorkerClosing => "worker_closing",
    }
}

tallied! {
    /// What moved on the process's sockets, in the order /stats lists it.
    pub enum WsCount {
        Opened => "opened",
        MessagesReceived => "messages_received",
        /// Data frames written, one per write_raw however many frames it held.
        MessagesSent => "messages_sent",
        /// One per subscriber per publish left out; one dropped instead counts as closed slow.
        MessagesSkipped => "messages_skipped",
        WritesRefused => "writes_refused",
        PublishesRefused => "publishes_refused",
        InboundPauses => "inbound_pauses",
        /// Frame headers and control frames included.
        ReceivedBytes => "received_bytes",
        SentBytes => "sent_bytes",
    }
}

// Process-wide, Relaxed and advisory, read through `snapshot`.
pub(super) static OPEN: AtomicU64 = AtomicU64::new(0);
pub(super) static COUNTS: Tally<WsCount, { WsCount::ALL.len() }> = Tally::new();
pub(super) static CLOSED: Tally<Cause, { Cause::ALL.len() }> = Tally::new();
pub(super) static HANDSHAKES_REFUSED: Tally<Refusal, { Refusal::ALL.len() }> = Tally::new();

/// Every socket opened is open, or closed once under its cause.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct WsSnapshot {
    pub open: u64,
    /// Indexed as WsCount::ALL.
    pub counts: [u64; WsCount::ALL.len()],
    pub closed: [u64; Cause::ALL.len()],
    pub handshakes_refused: [u64; Refusal::ALL.len()],
}

impl WsSnapshot {
    pub fn count(&self, count: WsCount) -> u64 {
        self.counts[count.index()]
    }
}

pub fn snapshot() -> WsSnapshot {
    WsSnapshot {
        open: OPEN.load(Ordering::Acquire),
        counts: COUNTS.read(),
        closed: CLOSED.read(),
        handshakes_refused: HANDSHAKES_REFUSED.read(),
    }
}
