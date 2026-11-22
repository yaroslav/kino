//! The inline spin: once its answer is out, the job of a connection alone on its worker looks at
//! the socket for up to BUDGET, GVL released, before it waits on the app's scheduler, so a next
//! request sent at once is read without a park and a wake. Catch runs are bounded (Run), and a
//! client that keeps coming late backs the spin off (Backoff). See doc/architecture.md.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use crate::gvl::Parker;
use crate::registry::ServerInner;
use crate::slot::WorkerSlot;

/// Past this many requests in flight server-wide a spinner would hold a CPU the busy threads
/// need: each keeps up to two threads busy, so half the CPUs' worth leave CPUs for the spinners.
pub(super) fn in_flight_limit(cpus: usize) -> usize {
    (cpus / 2).max(1)
}

const ONE: u16 = 1024;

/// 9/16: a spinner whose waits alternate quick and late peaks at 547 just after a quick one,
/// and backs off; at 1/2 it would spin before each late one only.
const SPIN_AT: u16 = ONE * 9 / 16;

const PROBE: u8 = 32;

const SEEN_MISS: u32 = 2;

/// How a spin ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Spin {
    /// Its client's bytes or close came within the budget.
    Hit,
    Miss,
    /// The gate shut, or the VM's unblock function fired.
    Stopped,
}

/// When a connection's spins pay: it spins while its share of hits (in 1/ONE, each chance
/// weighing 1/8) is at least SPIN_AT, else once in PROBE chances. A skipped chance is judged by
/// when what it waited for came (observed): within the budget a hit, past SEEN_MISS budgets a
/// miss. Its connection's job alone reads and writes it.
pub(crate) struct Backoff {
    share: u16,
    skipped: u8,
    /// When its last skipped chance was, in mono µs; 0 once judged.
    skipped_at: u64,
}

impl Default for Backoff {
    fn default() -> Self {
        Backoff {
            share: ONE,
            skipped: 0,
            skipped_at: 0,
        }
    }
}

impl Backoff {
    /// Whether this chance spins; one it skips is stamped, for observed.
    pub(crate) fn chance(&mut self) -> bool {
        if self.share >= SPIN_AT || self.skipped + 1 >= PROBE {
            (self.skipped, self.skipped_at) = (0, 0);
            return true;
        }
        self.skipped += 1;
        self.skipped_at = crate::mono::mono_us().max(1);
        false
    }

    /// Whether the spin that ended as `spun` caught what it waited for; a stop counts neither
    /// way.
    pub(crate) fn record(&mut self, spun: Spin) -> bool {
        if spun != Spin::Stopped {
            self.share = counted(self.share, spun == Spin::Hit);
        }
        spun == Spin::Hit
    }

    pub(crate) fn observed(&mut self, budget: Duration) {
        if self.skipped_at != 0 {
            let seen = crate::mono::mono_us().saturating_sub(self.skipped_at);
            let seen = Duration::from_micros(seen);
            if seen <= budget {
                self.share = counted(self.share, true);
            } else if seen > budget * SEEN_MISS {
                self.share = counted(self.share, false);
            }
            self.skipped_at = 0;
        }
    }
}

/// The eighth is rounded to the nearest unit, so rounding favours neither side.
fn counted(hits: u16, hit: bool) -> u16 {
    hits - (hits + 4) / 8 + if hit { ONE / 8 } else { 0 }
}

/// Whether the server's inline connections may spin at all: on (ServerConfig::inline_spin) and
/// not draining.
fn on(server: &ServerInner) -> bool {
    let inline = server.inline.as_ref();
    inline.is_some_and(|inline| inline.spin_on.load(Ordering::Relaxed))
}

/// Whether a spin may go on: on, and the server lightly loaded.
#[inline]
fn lightly_loaded(server: &ServerInner) -> bool {
    server.inline.as_ref().is_some_and(|inline| {
        inline.spin_on.load(Ordering::Relaxed)
            && server.counters.in_flight.load(Ordering::Relaxed) <= inline.spin_in_flight
    })
}

pub(crate) const BUDGET: Duration = Duration::from_micros(100);

/// Past this many reads in a row through spins, or this long since the job's last wait, a catch
/// is left to the job's wait: the bound on how long the worker's other fibers wait for a pass.
const RUN_HITS: u32 = 16;
pub(crate) const RUN: Duration = Duration::from_millis(1);

/// The gate's costlier checks (WorkerSlot::has_waiting) run once in this many passes.
const WAITING_EVERY: u32 = 8;

#[derive(Default)]
pub(super) struct Run {
    /// From the job's return from that wait if a spin could follow then, else its first hit.
    since: Option<Instant>,
    hits: u32,
}

impl Run {
    /// The job came back from a wait; the clock is read only where a spin may follow.
    pub(super) fn waited(&mut self, server: &ServerInner, slot: &WorkerSlot) {
        self.hits = 0;
        self.since =
            (slot.inline.conns.load(Ordering::Relaxed) == 1 && on(server)).then(Instant::now);
    }

    /// A spin caught the next request: whether the job reads it now or leaves it to its wait.
    pub(super) fn reads(&mut self, run: Duration) -> bool {
        let since = *self.since.get_or_insert_with(Instant::now);
        self.hits += 1;
        self.hits <= RUN_HITS && since.elapsed() < run
    }
}

/// At most spin_in_flight; on its own cache line, as each spin changes it twice.
#[derive(Default)]
#[repr(align(128))]
pub(crate) struct Spinners(AtomicUsize);

impl Spinners {
    /// One more spinning, while fewer than `most` do: held until it drops.
    pub(crate) fn join(&self, most: usize) -> Option<Spinning<'_>> {
        if self.0.fetch_add(1, Ordering::Relaxed) >= most {
            self.0.fetch_sub(1, Ordering::Relaxed);
            return None;
        }
        Some(Spinning(&self.0))
    }
}

pub(crate) struct Spinning<'a>(&'a AtomicUsize);

impl Drop for Spinning<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Runs the spin it is given, the unblock function raising the flag; None if it did not run.
pub(crate) type Release<'a> = dyn FnMut(&AtomicBool, &mut dyn FnMut() -> Spin) -> Option<Spin> + 'a;

/// With the GVL released, so other Ruby threads run and the VM's barriers never wait on it; not
/// at all with an interrupt pending. A panic reads as no spin.
pub(crate) fn released(woken: &AtomicBool, spin: &mut dyn FnMut() -> Spin) -> Option<Spin> {
    crate::gvl::Released.release(woken, spin).ok().flatten()
}

pub(super) fn poll(
    fd: std::os::fd::RawFd,
    budget: Duration,
    woken: &AtomicBool,
    server: &ServerInner,
    slot: &WorkerSlot,
) -> Spin {
    let start = Instant::now();
    for pass in 1u32.. {
        if super::io::poll_for(fd, libc::POLLIN, 0).is_some() {
            return Spin::Hit;
        }
        if start.elapsed() >= budget {
            return Spin::Miss;
        }
        // `woken` is the release's own flag, for the VM's unblock function; the slot's halt is
        // Kino's alone, read beside it.
        if woken.load(Ordering::Relaxed)
            || slot.life.halted().load(Ordering::Relaxed)
            || !going(server, slot)
            || (pass.is_multiple_of(WAITING_EVERY) && slot.lane.has_waiting())
        {
            return Spin::Stopped;
        }
    }
    Spin::Miss
}

/// The connection is alone on `slot`'s worker, which has nothing open or waiting and is neither
/// retired nor quarantined, and the server is lightly loaded.
pub(super) fn may_spin(server: &ServerInner, slot: &WorkerSlot) -> bool {
    going(server, slot) && !slot.lane.has_waiting()
}

fn going(server: &ServerInner, slot: &WorkerSlot) -> bool {
    slot.inline.conns.load(Ordering::Relaxed) == 1
        && lightly_loaded(server)
        && slot.load.in_flight.load(Ordering::Relaxed) == 0
        && !slot.lane.wake_pending.load(Ordering::Relaxed)
        && !slot.life.seems_retired()
        && !slot.life.quarantined()
}

#[cfg(test)]
mod tests {
    use std::net::TcpStream;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use super::Spin;
    use crate::inline::testing::{
        answer, answer_dispatched, await_bytes, handbacks, loopback, placed, send_get,
        spinning_server, wedge, Wire,
    };
    use crate::inline::{InlineConn, Next};
    use crate::registry::{BoxedCtx, ServerInner};
    use crate::slot::WorkerSlot;
    use crate::testing::{eventually_blocking, slot};

    /// The spin's switch, as boot and the drain set it.
    fn spin_on(server: &ServerInner, on: bool) {
        let inline = server.inline.as_ref().expect("an inline server");
        inline.spin_on.store(on, Ordering::Relaxed);
    }

    /// Far longer than any test waits: a look still spinning when a test
    /// checks fails it.
    const LONG: Duration = Duration::from_secs(20);
    /// How soon a spin must let go once it has to.
    const PROMPTLY: Duration = Duration::from_secs(1);

    /// A spin's release in place, as on a thread with no GVL to release.
    fn in_place(_: &AtomicBool, spin: &mut dyn FnMut() -> Spin) -> Option<Spin> {
        Some(spin())
    }

    /// How many spins `server`'s connections ended since `before`; None
    /// for all so far.
    fn spun_since(server: &ServerInner, before: Option<u64>) -> u64 {
        crate::inline::testing::inline_stats(server).spins - before.unwrap_or(0)
    }

    /// The request `conn`'s job reads once its client's bytes are there,
    /// its looks spinning in place.
    fn request_on(conn: &mut InlineConn) -> BoxedCtx {
        await_bytes(conn);
        eventually_blocking("the request", || {
            match conn.next_with(true, &mut in_place) {
                Next::Request(ctx) => Some(ctx),
                Next::Readable => None,
                Next::Writable | Next::Closed => panic!("no request came"),
            }
        })
    }

    /// `ctx` answered, and its answer read by `client`.
    fn answered(ctx: &BoxedCtx, client: &TcpStream) {
        let path = ctx.uri.path().to_string();
        answer(ctx, path.as_bytes());
        assert_eq!(Wire::of(client).response().2, path.as_bytes());
    }

    /// A server of `workers` workers whose connections' jobs spin for
    /// LONG, the spin's gate open, and a connection alone on its
    /// worker that has served a request: its job's next look spins.
    fn alone_after_a_request(workers: usize) -> (Arc<ServerInner>, TcpStream, Box<InlineConn>) {
        let server = spinning_server(workers, LONG);
        let (client, mut conn) = loopback(&server);
        send_get(&client, "/first", false);
        answered(&request_on(&mut conn), &client);
        (server, client, conn)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_lone_connections_job_waits_through_the_scheduler_every_16_requests() {
        // Requests that are there as each look spins: the job reads 16
        // in a row through its spins, then the 17th waits, its request
        // unread, for the scheduler to resume the job, which reads it.
        let (server, client, mut conn) = alone_after_a_request(1);
        let mut caught = 0;
        for i in 0..40 {
            send_get(&client, &format!("/{i}"), false);
            await_bytes(&conn);
            let before = Some(spun_since(&server, None));
            let ctx = match conn.next_with(true, &mut in_place) {
                Next::Request(ctx) => {
                    caught += 1;
                    assert!(caught <= 16, "{caught} requests read without a wait");
                    ctx
                }
                Next::Readable => {
                    assert_eq!(caught, 16, "request {i} waited after {caught}");
                    assert!(conn.readable(), "request {i} was read before the wait");
                    caught = 0;
                    request_on(&mut conn)
                }
                _ => panic!("the connection ended"),
            };
            let spun = spun_since(&server, before);
            assert_eq!(spun, 1, "request {i} came with no spin");
            answered(&ctx, &client);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_lone_connections_job_spins_only_while_its_worker_has_nothing_else_to_do() {
        type Set = fn(&Arc<ServerInner>, &Arc<WorkerSlot>) -> Box<dyn std::any::Any>;
        type Then = fn(&Next) -> bool;
        let readable: Then = |next| matches!(next, Next::Readable);
        let closed: Then = |next| matches!(next, Next::Closed);
        // What holds as the look begins, so that it never spins; or what
        // comes as it spins, which stops it, and what the look then finds.
        #[rustfmt::skip] // One row per line: a table of conditions.
        let rows: [(&str, bool, Set, Then); 7] = [
            ("another connection on its worker", false, |server, _| Box::new(loopback(server)), readable),
            ("a request open on its worker", false, |server, slot| {
                crate::testing::assign(slot);
                Box::new(crate::queue::testing::admit_next(server, slot))
            }, readable),
            ("the spin off, as on one CPU", false, |server, _| {
                spin_on(server, false);
                Box::new(())
            }, readable),
            ("a second connection placed on its worker", true, |server, _| Box::new(placed(server)), readable),
            ("a request sent to its worker", true, |_, slot| { crate::testing::assign(slot); Box::new(()) }, readable),
            ("the drain", true, |server, _| { server.stop_accepting(); Box::new(()) }, closed),
            ("its worker given up on as wedged", true, |_, slot| {
                wedge(slot);
                Box::new(())
            }, closed),
        ];
        for (what, during, set, then) in rows {
            let (server, _client, conn) = alone_after_a_request(1);
            let slot = slot(&server, 0);
            let before = Some(spun_since(&server, None));
            let (started, spinning) = std::sync::mpsc::channel();
            let held = (!during).then(|| set(&server, &slot));
            let look = std::thread::spawn(move || {
                let mut conn = conn;
                let begun = Instant::now();
                let next = conn.next_with(true, &mut |_, spin| {
                    let _ = started.send(());
                    Some(spin())
                });
                (next, begun.elapsed())
            });
            let spun = spinning.recv_timeout(PROMPTLY).is_ok();
            assert_eq!(spun, during, "{what}: spun {spun}");
            let _held = held.or_else(|| Some(set(&server, &slot)));
            let (next, took) = look.join().expect("the look");
            assert!(took < PROMPTLY, "{what}: still spun for {took:?}");
            assert!(then(&next), "{what}: the look went on as without a spin");
            assert_eq!(spun_since(&server, before), u64::from(during), "{what}");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_request_a_spin_catches_as_its_worker_retires_goes_to_hyper_with_the_connection() {
        // Two workers: hyper sends the request to the one that stays.
        let (server, client, mut conn) = alone_after_a_request(2);
        let slot = slot(&server, 0);
        send_get(&client, "/second", false);
        await_bytes(&conn);

        // The retirement lands as the spin starts, which then catches the
        // request at its first look, before it reads the gate again.
        let next = conn.next_with(true, &mut |_, spin| {
            slot.retire();
            Some(spin())
        });

        assert!(
            matches!(next, Next::Closed),
            "the retiring worker served it inline"
        );
        assert_eq!(handbacks(&server), 1);
        answer_dispatched(&server, &["/second"]).await;
        assert_eq!(Wire::of(&client).response().2, b"hyper:/second");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_request_a_lone_connections_spin_sees_as_its_thread_is_unblocked_waits_for_the_jobs_wait(
    ) {
        let (server, client, mut conn) = alone_after_a_request(1);
        send_get(&client, "/second", false);
        await_bytes(&conn);
        let before = Some(spun_since(&server, None));

        // The VM's unblock function fires as the spin starts (a
        // Thread#raise, say): the job waits, where the raise meets it, as
        // without a spin, rather than run a request first.
        let next = conn.next_with(true, &mut |woken, spin| {
            woken.store(true, Ordering::SeqCst);
            Some(spin())
        });
        assert!(
            matches!(next, Next::Readable),
            "the spin took the request past its unblock"
        );
        assert_eq!(spun_since(&server, before), 1, "the spin never ran");
        let Next::Request(ctx) = conn.next_with(true, &mut in_place) else {
            panic!("the wait's look missed the request");
        };
        assert_eq!(ctx.uri.path(), "/second");
    }
}
