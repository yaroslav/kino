//! The line of requests that found no lane with room on a worker_loop server: they go in
//! first come first served, and only the first in line dispatches.

use std::collections::VecDeque;
use std::sync::atomic::{fence, AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::Notify;

/// While nobody waits, a take pays one load for it.
#[derive(Default)]
pub struct LaneRoom {
    waiting: AtomicUsize,
    line: Mutex<VecDeque<Arc<Notify>>>,
}

impl LaneRoom {
    pub fn waiting(&self) -> usize {
        self.waiting.load(Ordering::Relaxed)
    }

    /// After a lane may have gained room (a worker parked, a full worker went below its
    /// capacity, lanes closed). The fence pairs with line_up's: either the newcomer's first
    /// look sees the change, or this sees the newcomer.
    pub fn notify(&self) {
        fence(Ordering::SeqCst);
        self.notify_taken();
    }

    /// notify after a take from a lane, with no fence: that lane's channel lock orders the
    /// take and a newcomer's look at it, and the newcomer lined up before it looked.
    pub fn notify_taken(&self) {
        if self.waiting.load(Ordering::Relaxed) != 0 {
            self.wake_first();
        }
    }

    fn wake_first(&self) {
        let first = self.line.lock().front().cloned();
        if let Some(first) = first {
            first.notify_one();
        }
    }

    /// The place leaves the line when dropped, handing the turn on if it was first.
    pub fn line_up(&self) -> Place<'_> {
        let turn = Arc::new(Notify::new());
        {
            let mut line = self.line.lock();
            line.push_back(turn.clone());
            self.waiting.store(line.len(), Ordering::Relaxed);
        }
        fence(Ordering::SeqCst);
        Place { room: self, turn }
    }
}

pub struct Place<'a> {
    room: &'a LaneRoom,
    /// A notification before the wait is kept for it.
    turn: Arc<Notify>,
}

impl Place<'_> {
    pub fn is_first(&self) -> bool {
        self.room
            .line
            .lock()
            .front()
            .is_some_and(|first| Arc::ptr_eq(first, &self.turn))
    }

    /// May resolve for nothing (a notification the last look acted on): look again.
    pub async fn turn(&self) {
        self.turn.notified().await;
    }
}

impl Drop for Place<'_> {
    fn drop(&mut self) {
        let next = {
            let mut line = self.room.line.lock();
            let Some(at) = line.iter().position(|place| Arc::ptr_eq(place, &self.turn)) else {
                return;
            };
            line.remove(at);
            self.room.waiting.store(line.len(), Ordering::Relaxed);
            if at == 0 {
                line.front().cloned()
            } else {
                None
            }
        };
        if let Some(next) = next {
            next.notify_one();
        }
    }
}
