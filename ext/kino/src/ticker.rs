//! worker_loop's lane tick. A doorbell wait has no timeout, which would cost the app's scheduler
//! a timer per wait: instead one thread per server rings every armed doorbell each TICK, and
//! the take that answers (queue::Taker::nonblock) does what a parked classic worker's tick does.
//! The same look cuts off inline connections past their deadline (inline::tick).

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
        crate::inline::tick(&server);
        next += TICK;
        let now = Instant::now();
        if next <= now {
            next = now + TICK;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::server::HEADER_READ_TIMEOUT;

    /// Long enough for a ring due within a TICK on a loaded CI runner, which
    /// can be 100 ms late. A cut that never comes fails at the client's 5 s
    /// read timeout.
    const A_FEW_TICKS: Duration = Duration::from_millis(10 * TICK.as_millis() as u64);

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn it_cuts_off_an_inline_connection_within_a_tick_of_its_deadline() {
        let server = crate::inline::testing::inline_loop_server(1, 8);
        let (client, _conn) = crate::inline::testing::loopback(&server);
        let ticker = Ticker::start(&server).expect("the ticker thread");

        // Its deadline is 100 ms off: the ticker's clock runs on to it.
        let left = Duration::from_millis(100);
        crate::inline::testing::advance(&server, HEADER_READ_TIMEOUT - left);
        let advanced = Instant::now();
        assert_eq!(
            crate::inline::testing::read_to_end(&client),
            b"",
            "a connection that sent nothing got an answer"
        );
        assert!(
            advanced.elapsed() < left + A_FEW_TICKS,
            "cut off {:?} after its deadline",
            advanced.elapsed().saturating_sub(left)
        );
        ticker.stop();
    }
}
