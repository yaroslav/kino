//! worker_loop's lane tick. A doorbell wait has no timeout, which would cost the app's scheduler
//! a timer per wait: instead one thread per server rings every armed doorbell each TICK, and
//! the take that answers (queue::Taker::nonblock) does what a parked classic worker's tick does.

use std::sync::{Arc, Weak};
use std::time::Instant;

use crate::queue::TICK;
use crate::registry::ServerInner;

pub struct Ticker {
    /// Never sent on: the thread runs until this goes.
    stop: flume::Sender<()>,
    thread: std::thread::JoinHandle<()>,
}

impl Ticker {
    pub fn start(server: &Arc<ServerInner>) -> std::io::Result<Ticker> {
        let (stop, stopped) = flume::bounded(0);
        let server = Arc::downgrade(server);
        let thread = std::thread::Builder::new()
            .name("kino-ticker".to_string())
            .spawn(move || run(&server, &stopped))?;
        Ok(Ticker { stop, thread })
    }

    pub fn stop(self) {
        drop(self.stop);
        let _ = self.thread.join();
    }
}

/// A tick that fell behind (the process was suspended) starts over from now, not catching up.
fn run(server: &Weak<ServerInner>, stopped: &flume::Receiver<()>) {
    let mut next = Instant::now() + TICK;
    while let Err(flume::RecvTimeoutError::Timeout) = stopped.recv_deadline(next) {
        let Some(server) = server.upgrade() else {
            return;
        };
        for slot in server.slots.read().iter() {
            slot.lane.ring_doorbell();
        }
        next += TICK;
        let now = Instant::now();
        if next <= now {
            next = now + TICK;
        }
    }
}
