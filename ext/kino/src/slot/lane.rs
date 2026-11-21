//! A slot's lane: the queue the dispatcher feeds, its worker's inbox of socket events, and
//! the bell and wake flags.

use std::sync::atomic::{AtomicBool, Ordering};

use parking_lot::Mutex;

use super::LANE_DEPTH;
use crate::registry::LaneItem;
use crate::ws::WsEvent;

/// Lane mode only: the queue the dispatcher feeds, the sockets' events in order, and
/// whether a doorbell already sits in the lane and a wake is due (wakes coalesce).
#[derive(Default)]
pub struct Lane {
    pub tx: Mutex<Option<flume::Sender<LaneItem>>>,
    pub rx: Option<flume::Receiver<LaneItem>>,
    pub ws_tx: Option<flume::Sender<WsEvent>>,
    pub ws_rx: Option<flume::Receiver<WsEvent>>,
    pub bell_pending: AtomicBool,
    pub wake_pending: AtomicBool,
}

impl Lane {
    pub(super) fn new(lanes: bool, inbox: usize) -> Self {
        if !lanes {
            return Lane::default();
        }
        let (tx, rx) = flume::bounded(LANE_DEPTH);
        let (ws_tx, ws_rx) = flume::bounded(inbox);
        Lane {
            tx: Mutex::new(Some(tx)),
            rx: Some(rx),
            ws_tx: Some(ws_tx),
            ws_rx: Some(ws_rx),
            ..Lane::default()
        }
    }

    /// Whether the slot has a lane at all, which wakes and WebSocket events need.
    pub fn exists(&self) -> bool {
        self.rx.is_some()
    }

    /// The wake hook's turn, if due. The load first keeps the every-take check from writing
    /// the flag.
    pub fn take_wake(&self) -> bool {
        self.wake_pending.load(Ordering::SeqCst) && self.wake_pending.swap(false, Ordering::SeqCst)
    }

    /// A take found the lane's bell: the flag goes down before the inbox and the wake flag
    /// are read, so what arrives meanwhile rings again.
    #[inline]
    pub fn took_bell(&self) {
        self.bell_pending.store(false, Ordering::SeqCst);
    }

    /// Requests waiting in the lane (None without one); not a bell, so sockets don't grow
    /// the pool.
    pub fn requests(&self) -> Option<usize> {
        let bell = self.bell_pending.load(Ordering::Relaxed);
        Some(self.rx.as_ref()?.len().saturating_sub(usize::from(bell)))
    }

    #[inline]
    pub fn open_and_empty(&self) -> bool {
        let lane = self.rx.as_ref();
        lane.is_some_and(|rx| !rx.is_disconnected() && rx.is_empty())
    }
}
