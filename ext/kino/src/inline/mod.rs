//! Inline connections: on a worker_loop server, a plain HTTP/1.1 connection is served on its
//! worker's thread, Rust reading a strict subset of HTTP/1.1 (parse.rs) and writing the answers;
//! any other request goes back to hyper, its bytes replayed, before the app sees it. Only the
//! connection's job closes its socket or hands it back (or the last write still waiting as the
//! job lets go); other threads write under its lock or shut it down. doc/architecture.md has the
//! mechanism.

use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use magnus::prelude::*;
use magnus::{Error, Ruby, Value};
use parking_lot::Mutex;

use crate::registry::ServerInner;
use crate::response::Cut;
use crate::server::{Conn, Held, Peer, StdConn};
use crate::slot::WorkerSlot;

mod answer;
mod conn;
mod deadline;
mod encode;
mod io;
#[cfg(test)]
mod parity;
mod parse;
mod spin;
#[cfg(test)]
pub mod testing;

pub use answer::Wrote;
pub use conn::{InlineConn, Next};
pub use deadline::tick;
pub(crate) use io::abandon as abandon_slot;
pub(crate) use io::Origin;
pub use io::{await_writable, InlineIo, Listed};
use io::{take_seat, Shut};

const INLINE_BODY_MAX: u64 = 64 * 1024;

pub struct InlineServer {
    /// Held while placing, so two I/O threads never pick the same least-loaded worker.
    placing: Mutex<()>,
    declined: AtomicU64,
    /// Connections that went back to hyper once a worker had them, whatever for.
    handbacks: AtomicU64,
    /// mono_us as of the ticker's last look, up to a TICK behind: a deadline costs a load, not a
    /// clock read.
    clock: Clock,
    pub(crate) spinners: spin::Spinners,
    /// The inline spin's switch (ServerConfig::inline_spin), off from the drain on; and the most
    /// requests in flight server-wide it runs under (spin::in_flight_limit).
    pub(crate) spin_on: AtomicBool,
    pub(crate) spin_in_flight: usize,
    /// A spin's budget and a run of spins' bound (spin::BUDGET, spin::RUN), which tests lengthen.
    pub(crate) spin_budget: std::time::Duration,
    pub(crate) spin_run: std::time::Duration,
    /// How far tests moved the clock on past mono_us (testing::advance),
    /// which the ticker's looks keep.
    #[cfg(test)]
    ahead: AtomicU64,
    /// What happens to the worker placement picks before the connection
    /// is sent to it (tests).
    #[cfg(test)]
    pub(crate) on_pick: Mutex<Option<OnPick>>,
}

#[cfg(test)]
pub(crate) type OnPick = Box<dyn FnOnce(&WorkerSlot) + Send>;

/// On its own cache line: every answer reads it, and the counters beside it churn.
#[repr(align(128))]
struct Clock(AtomicU64);

impl Default for InlineServer {
    fn default() -> Self {
        InlineServer {
            placing: Mutex::default(),
            declined: AtomicU64::default(),
            handbacks: AtomicU64::default(),
            clock: Clock(AtomicU64::new(crate::mono::mono_us())),
            spinners: spin::Spinners::default(),
            spin_on: AtomicBool::new(false),
            spin_in_flight: spin::in_flight_limit(crate::cpus::count()),
            spin_budget: spin::BUDGET,
            spin_run: spin::RUN,
            #[cfg(test)]
            ahead: AtomicU64::new(0),
            #[cfg(test)]
            on_pick: Mutex::default(),
        }
    }
}

impl InlineServer {
    pub fn declined(&self) -> u64 {
        self.declined.load(Ordering::Relaxed)
    }

    pub fn handbacks(&self) -> u64 {
        self.handbacks.load(Ordering::Relaxed)
    }
}

fn now(server: &ServerInner) -> u64 {
    server
        .inline
        .as_ref()
        .map_or_else(crate::mono::mono_us, |inline| {
            inline.clock.0.load(Ordering::Relaxed)
        })
}

fn real_now(server: &ServerInner) -> u64 {
    let now = crate::mono::mono_us();
    #[cfg(test)]
    let now = now
        + server
            .inline
            .as_ref()
            .map_or(0, |inline| inline.ahead.load(Ordering::Relaxed));
    #[cfg(not(test))]
    let _ = server;
    now
}

/// At accept: a plain connection goes to a worker_loop worker (pick); with none free, or one that
/// no longer takes it, it goes to hyper, counted as declined.
pub(crate) fn place(
    server: &Arc<ServerInner>,
    conn: Conn,
    held: Held,
    peer: Peer,
) -> Result<(), (Conn, Held)> {
    let Some(inline) = &server.inline else {
        return Err((conn, held));
    };
    let seat = {
        let _placing = inline.placing.lock();
        pick(server).map(|slot| take_seat(&slot))
    };
    let Some(seat) = seat else {
        inline.declined.fetch_add(1, Ordering::Relaxed);
        return Err((conn, held));
    };
    // The worker's scheduler waits on it from now on; failing that, it closes.
    let Ok(stream) = StdConn::detach(conn) else {
        return Ok(());
    };
    let home = tokio::runtime::Handle::current();
    let conn = InlineConn::new(server, seat, stream, held, peer, home);
    let slot = conn.slot().clone();
    #[cfg(test)]
    {
        let picked = inline.on_pick.lock().take();
        if let Some(meanwhile) = picked {
            meanwhile(&slot);
        }
    }
    let Err(mut conn) = slot.send_placed(conn) else {
        return Ok(());
    };
    // Never taken: to hyper, registered anew, which tokio does not know readable yet.
    conn.send_to_hyper(false);
    Ok(())
}

/// A serving worker_loop worker in the pool with fewer inline connections than its capacity and
/// room for a request (one that filled up stays full until its loop next takes with a slot free).
/// Ties go to the fewest open connections, then requests, then the last parked.
fn pick(server: &ServerInner) -> Option<Arc<WorkerSlot>> {
    server
        .slots
        .read()
        .iter()
        .filter(|slot| slot.lane.doorbell.is_some())
        .filter(|slot| {
            !slot.life.retired.load(Ordering::Relaxed)
                && !slot.life.quarantined.load(Ordering::Relaxed)
        })
        .filter(|slot| {
            slot.lane
                .rx
                .as_ref()
                .is_some_and(|lane| !lane.is_disconnected())
        })
        .filter(|slot| {
            let capacity = slot.load.capacity.load(Ordering::Relaxed);
            capacity > 0 && slot.inline.conns.load(Ordering::Relaxed) < capacity
        })
        .filter(|slot| !slot.load.at_capacity())
        .min_by_key(|slot| {
            (
                open_conns(slot),
                slot.load.in_flight.load(Ordering::Relaxed),
                std::cmp::Reverse(slot.park.parked_at.load(Ordering::Relaxed)),
            )
        })
        .cloned()
}

/// Inline connections less those answered whose job has not looked for the next request yet.
fn open_conns(slot: &WorkerSlot) -> usize {
    slot.inline
        .conns
        .load(Ordering::Relaxed)
        .saturating_sub(slot.inline.answered.load(Ordering::Relaxed))
}

/// The drain began: idle connections are shut down, which wakes each job to close its own.
pub fn drain(server: &ServerInner) {
    for slot in server.slots.read().iter() {
        shut_idle(slot, Shut::Drain);
    }
}

fn shut_idle(slot: &WorkerSlot, shut: Shut) {
    for io in ios(slot) {
        io.shut_if_idle(shut);
    }
}

/// `slot`'s worker was quarantined and may come back: its idle connections are shut so their
/// clients go elsewhere, and those it never took go to hyper. A request it serves stays its own.
pub fn quarantine(slot: &WorkerSlot) {
    shut_idle(slot, Shut::Quarantine);
    unplace(slot);
}

/// What was placed on `slot`'s worker and never taken goes to hyper, handed back.
pub fn unplace(slot: &WorkerSlot) {
    if let Some(placed) = slot.lane.placed_rx.as_ref() {
        while let Ok(mut conn) = placed.try_recv() {
            conn.send_to_hyper(true);
        }
    }
}

/// `slot`'s worker crashed (Cut::Crash) or the shutdown passed its deadline (Cut::Shutdown): each
/// connection not ending already counts as cut off, once, in the one total of those Kino cut
/// (InlineSeats::closed); on a crash an idle one goes to hyper instead (abandon_slot). Runs
/// before the worker's requests are given up on, which ends their connections.
pub fn giving_up(slot: &WorkerSlot, why: Cut) {
    for io in ios(slot) {
        io.giving_up(why);
    }
}

pub fn shut_unserved(slot: &WorkerSlot) {
    for io in ios(slot) {
        io.shut_unless_serving();
    }
}

pub(super) fn ios(slot: &WorkerSlot) -> Vec<Arc<InlineIo>> {
    slot.inline
        .ios
        .lock()
        .iter()
        .filter_map(Listed::io)
        .collect()
}

/// Kino::Native::Conn, made in the worker's Ractor by the take that found it. Its drop takes the
/// state lock and the worker's list, then hands the socket to hyper on the accepting runtime or
/// answers and shuts it, with no Ruby.
#[magnus::wrap(class = "Kino::Native::Conn", free_immediately)]
pub struct ConnHandle(RefCell<Option<Box<InlineConn>>>);

pub fn wrap(ruby: &Ruby, conn: Box<InlineConn>) -> Value {
    ruby.obj_wrap(ConnHandle(RefCell::new(Some(conn))))
        .as_value()
}

impl ConnHandle {
    pub fn fd(rb_self: &ConnHandle) -> Option<i32> {
        rb_self.0.borrow().as_ref().and_then(|conn| conn.fd())
    }

    /// Conn#next_request(room): the admitted env; :readable or :writable to wait first; :yield to
    /// step aside first; nil once over. Without `room`, a request goes back to hyper with the
    /// connection.
    pub fn next_request(ruby: &Ruby, rb_self: &ConnHandle, room: bool) -> Result<Value, Error> {
        let mut conn = rb_self.0.borrow_mut();
        let Some(conn) = conn.as_mut() else {
            return Ok(ruby.qnil().as_value());
        };
        let symbols = crate::symbols::get();
        if conn.yield_turn() {
            return Ok(symbols.step_aside.value());
        }
        crate::shared_gvl::pass_in_loop(conn.server(), conn.slot())?;
        Ok(match conn.next(room) {
            Next::Request(ctx) => {
                crate::queue::admit_inline(ruby, conn.server(), conn.slot(), ctx)?.as_value()
            }
            Next::Readable => symbols.readable.value(),
            Next::Writable => symbols.writable.value(),
            Next::Closed => ruby.qnil().as_value(),
        })
    }

    /// Conn#close: a request still owed its answer gets a 500, then the socket closes. The borrow
    /// cannot fail: the Conn never leaves its job's thread, and next_request's GVL pass, the one
    /// way other Ruby code runs inside it, switches no fiber.
    pub fn close(rb_self: &ConnHandle) {
        if let Ok(mut conn) = rb_self.0.try_borrow_mut() {
            conn.take();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::atomic::Ordering;

    use tokio_util::either::Either;

    use super::*;
    use crate::inline::testing::{
        accepted, answer, answer_dispatched, await_bytes, cut_offs, handbacks, inline_loop_server,
        look, loopback, loopback_holding, narrowed, next_request, once_written, permit, placed,
        placed_on_first, read_to_end, reads_within, send_get, wedge, Event, Wire,
    };
    use crate::queue::{Item, Take, Taker};
    use crate::request::test_ctx;

    fn conns(server: &ServerInner) -> Vec<usize> {
        let slots = server.slots.read();
        slots
            .iter()
            .map(|slot| slot.inline.conns.load(Ordering::Relaxed))
            .collect()
    }

    fn declined(server: &ServerInner) -> u64 {
        server.inline.as_ref().expect("inline on").declined()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_unix_socket_connection_is_served_inline_and_handed_back_as_one() {
        let server = inline_loop_server(1, 8);
        let (client, accepted) = std::os::unix::net::UnixStream::pair().expect("a socket pair");
        accepted.set_nonblocking(true).expect("nonblocking");
        let accepted = tokio::net::UnixStream::from_std(accepted).expect("registered");
        let peer = crate::server::UNIX_PEER;
        let held = Held::new(permit(), None);
        assert!(place(&server, Either::Right(accepted), held, peer).is_ok());
        let slot = server.slots.read()[0].clone();
        let Take::Item(Item::Conn(mut conn)) = Taker::new(&server, &slot).ready() else {
            panic!("its worker did not take the unix-socket connection");
        };

        // Served inline, with the addresses hyper gives a unix-socket
        // request.
        let timeout = Some(std::time::Duration::from_secs(5));
        client.set_read_timeout(timeout).expect("read timeout");
        (&client)
            .write_all(b"GET /inline HTTP/1.1\r\nhost: x\r\n\r\n")
            .unwrap();
        let ctx = next_request(&mut conn);
        assert_eq!((ctx.peer.remote, ctx.peer.local), (peer.remote, peer.local));
        answer(&ctx, b"inline");
        drop(ctx);
        let mut wire = Wire::of(&client);
        assert_eq!(wire.response().2, b"inline");
        assert!(
            !conn.readable(),
            "an idle unix socket read as holding its client's bytes"
        );

        // An HTTP/1.0 request goes back to hyper, which answers it on the
        // same socket, registered anew as a unix stream.
        (&client).write_all(b"GET /back HTTP/1.0\r\n\r\n").unwrap();
        await_bytes(&conn);
        assert!(matches!(look(&mut conn, true), Next::Closed));
        answer_dispatched(&server, &["/back"]).await;
        let (status, _, body) = wire.response();
        assert_eq!(
            (status.as_str(), &body[..]),
            ("HTTP/1.0 200 OK", &b"hyper:/back"[..])
        );
        assert_eq!((handbacks(&server), conns(&server)), (1, vec![0]));
    }

    /// How a worker leaves what was placed on it and not taken yet.
    #[derive(Clone, Copy, Debug)]
    enum Leaves {
        /// Its loop takes it as the worker retires.
        TakenRetiring,
        /// Its thread ends with no crash to tell.
        Ends,
        /// Its thread ends once it retired.
        EndsRetired,
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn what_was_placed_on_a_leaving_worker_goes_to_hyper_with_its_client_and_no_more_comes() {
        for leaves in [Leaves::TakenRetiring, Leaves::Ends, Leaves::EndsRetired] {
            let server = inline_loop_server(2, 8);
            let (slot, worker) = crate::queue::testing::boot(&server, 0);
            let sibling = server.slots.read()[1].clone();
            let client = placed_on_first(&server).await;
            send_get(&client, "/placed", false);
            if !matches!(leaves, Leaves::Ends) {
                slot.retire();
            }
            if let Leaves::TakenRetiring = leaves {
                let sibling = Taker::new(&server, &sibling).ready();
                assert!(matches!(sibling, Take::Nothing), "a sibling took it");
                let Take::Item(Item::Conn(mut conn)) = Taker::new(&server, &slot).ready() else {
                    panic!("the retiring worker did not take its connection");
                };
                await_bytes(&conn);
                assert!(
                    matches!(conn.next(true), Next::Closed),
                    "a retired worker served it"
                );
            } else {
                crate::queue::Worker::release(&worker);
            }

            answer_dispatched(&server, &["/placed"]).await;
            let (_, headers, body) = Wire::of(&client).response();
            assert_eq!(body, b"hyper:/placed", "{leaves:?}");
            let close = "connection: close".to_string();
            assert!(
                !headers.contains(&close),
                "{leaves:?}: its client lost the connection"
            );
            assert_eq!(handbacks(&server), 1, "{leaves:?}");
            for _ in 0..2 {
                drop(placed(&server).await);
            }
            assert_eq!(
                conns(&server),
                [0, 2],
                "{leaves:?}: placed on the worker that left"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn quarantine_shuts_idle_connections_hands_back_placed_ones_and_ends_the_rest_after_their_answers(
    ) {
        let server = inline_loop_server(2, 8);
        let slots = server.slots.read().clone();
        // One idle on it, looked at by its job once, its client silent.
        let (idle_client, mut idle) = loopback(&server);
        assert!(matches!(idle.next(true), Next::Readable));
        // As its worker is given up on as wedged, a request it is stuck
        // in, not answered yet, and one answered, its job not back yet.
        let (stuck_client, mut stuck) = loopback(&server);
        send_get(&stuck_client, "/stuck", false);
        let late = next_request(&mut stuck);
        crate::queue::enlist_inline(&server, &slots[0], &late);
        let (done_client, mut done) = loopback(&server);
        send_get(&done_client, "/done", false);
        answer(&next_request(&mut done), b"done");
        // Placed on it, and not taken: its client's request comes.
        let placed_client = placed_on_first(&server).await;
        send_get(&placed_client, "/placed", false);

        wedge(&slots[0]);

        assert_eq!(
            read_to_end(&idle_client),
            b"",
            "an idle connection stayed open"
        );
        answer_dispatched(&server, &["/placed"]).await;
        assert_eq!(Wire::of(&placed_client).response().2, b"hyper:/placed");
        drop(placed(&server).await);
        assert_eq!(
            conns(&server)[1],
            1,
            "placement did not pass over the quarantined worker"
        );

        // Its worker comes back, and its jobs look again: what each owes
        // goes out, and its connection ends.
        assert!(
            matches!(done.next(true), Next::Closed),
            "kept alive after its answer"
        );
        let mut wire = Wire::of(&done_client);
        assert_eq!(
            (wire.response().2, wire.answer(false)),
            (b"done".to_vec(), Event::Closed)
        );
        answer(&late, b"late");
        let mut wire = Wire::of(&stuck_client);
        let (_, headers, body) = wire.response();
        let close = headers.contains(&"connection: close".to_string());
        assert!(
            body == b"late" && close,
            "the answer kept its connection alive: {headers:?}"
        );
        assert!(matches!(stuck.next(true), Next::Closed));
        assert_eq!(wire.answer(false), Event::Closed);
        assert!(matches!(idle.next(true), Next::Closed));
        assert_eq!(
            (cut_offs(&server), handbacks(&server), conns(&server)[0]),
            (2, 1, 0),
            "the idle one and the one answered before cut off, the one placed handed back, and \
             none counted on the worker"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_answer_still_going_out_as_the_drain_begins_or_its_worker_is_given_up_on_goes_out_whole_and_ends_its_connection(
    ) {
        // The request behind comes in the first one's write, read with it, or in a second write
        // once the job read the first, left unread in the socket: a close with bytes unread
        // resets the connection.
        for (writes, quarantined) in [(1, false), (1, true), (2, false), (2, true)] {
            let server = inline_loop_server(2, 8);
            let slot = server.slots.read()[0].clone();
            let (client, mut conn) = narrowed(&server);
            let behind = b"GET /behind HTTP/1.1\r\nhost: x\r\n\r\n";
            let mut sent = b"GET /big HTTP/1.1\r\nhost: x\r\n\r\n".to_vec();
            if writes == 1 {
                sent.extend_from_slice(behind);
            }
            (&client).write_all(&sent).unwrap();
            let ctx = next_request(&mut conn);
            if writes == 2 {
                (&client).write_all(behind).unwrap();
                let fd = conn.fd().expect("open");
                assert!(reads_within(fd, 5_000), "the request behind never came");
            }
            let big = vec![b'x'; 1 << 20];
            answer(&ctx, &big);
            drop(ctx);
            assert!(
                matches!(conn.next(true), Next::Writable),
                "the answer went out at once"
            );

            if quarantined {
                wedge(&slot);
            } else {
                server.stop_accepting();
            }

            // Its job's look puts the rest out (its worker back, if it was
            // given up on), then ends the connection.
            let reader = std::thread::spawn(move || {
                let mut wire = Wire::of(&client);
                (wire.response().2.len(), wire.end())
            });
            let served = matches!(once_written(&mut conn), Next::Request(_));
            drop(conn);
            let (read, rest) = reader.join().expect("the client");
            let what = format!("{writes} writes, quarantined {quarantined}");
            assert!(!served, "{what}: served a request sent behind the answer");
            assert_eq!(
                (read, rest),
                (big.len(), Event::Closed),
                "{what}: cut short, reset or left open"
            );
            assert_eq!(cut_offs(&server), u64::from(quarantined), "{what}");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_crashed_workers_connections_are_answered_or_handed_to_hyper_though_no_job_looks_again(
    ) {
        let server = inline_loop_server(2, 8);
        let slots = server.slots.read().clone();
        let slot = &slots[0];
        // One serving the request the worker crashed in.
        let (serving_client, mut serving) = loopback(&server);
        send_get(&serving_client, "/crash", false);
        let crashed = next_request(&mut serving);
        crate::queue::enlist_inline(&server, slot, &crashed);
        // One answered, its job not back yet, holding one of the server's
        // connection permits.
        let permits = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = permits.clone().try_acquire_owned().expect("a permit");
        let (answered_client, mut answered) = loopback_holding(&server, permit);
        send_get(&answered_client, "/done", false);
        answer(&next_request(&mut answered), b"done");
        // One answered, its job stepping aside for the others, as its
        // client's next request comes.
        let (aside_client, mut aside) = loopback(&server);
        send_get(&aside_client, "/first", false);
        answer(&next_request(&mut aside), b"first");
        assert_eq!(Wire::of(&aside_client).response().2, b"first");
        assert!(
            aside.yield_turn(),
            "the answered connection did not step aside"
        );
        send_get(&aside_client, "/next", false);
        await_bytes(&aside);
        // One placed on it that it never took, its client's request sent.
        let placed_client = placed_on_first(&server).await;
        send_get(&placed_client, "/placed", false);
        assert!(
            slot.recycle().is_none(),
            "a slot its connections count on was recycled"
        );

        slot.crash(&server);

        let status = Wire::of(&serving_client).response().0;
        assert_eq!(status, "HTTP/1.1 500 Internal Server Error");
        assert_eq!(read_to_end(&serving_client), b"");
        assert_eq!(Wire::of(&answered_client).response().2, b"done");
        assert_eq!(read_to_end(&answered_client), b"");
        answer_dispatched(&server, &["/placed", "/next"]).await;
        assert_eq!(Wire::of(&placed_client).response().2, b"hyper:/placed");
        assert_eq!(Wire::of(&aside_client).response().2, b"hyper:/next");
        // Counted once, and nowhere once their worker is gone, nor again
        // as their jobs look or go with the dead worker's heap.
        let counts = || {
            (
                cut_offs(&server),
                handbacks(&server),
                slot.inline.conns.load(Ordering::Relaxed),
                slot.inline.answered.load(Ordering::Relaxed),
                server.counters.in_flight.load(Ordering::Relaxed),
            )
        };
        let settled = (2, 2, 0, 0, 0);
        assert_eq!(counts(), settled);
        assert_eq!(
            permits.available_permits(),
            1,
            "a dead worker's connection holds its permit"
        );
        assert!(matches!(answered.next(true), Next::Closed));
        assert!(matches!(aside.next(true), Next::Closed));
        drop((serving, answered, aside, crashed));
        assert_eq!(counts(), settled);
        assert!(slot.recycle().is_some(), "a quiet crashed slot is recycled");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_connection_being_placed_as_its_worker_crashes_goes_to_hyper_untouched() {
        let server = inline_loop_server(2, 8);
        let slot = server.slots.read()[0].clone();
        let (_client, stream, held, peer) = accepted(1).await.pop().expect("one");
        // As place has it: its seat taken and the connection made, not
        // sent to the worker yet, when the worker crashes.
        let stream = stream.into_std().expect("a std stream");
        let home = tokio::runtime::Handle::current();
        let conn = InlineConn::new(&server, take_seat(&slot), stream.into(), held, peer, home);
        slot.crash(&server);
        let Err(mut conn) = slot.send_placed(conn) else {
            panic!("a crashed worker took a connection");
        };
        assert!(
            conn.let_go_to_hyper().is_some(),
            "the crash cut a connection its worker never had"
        );
        assert_eq!(slot.inline.conns.load(Ordering::Relaxed), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_full_worker_takes_a_connection_placed_on_it_and_hands_its_request_to_hyper() {
        use crate::queue::Room;
        // Placed on the first while it has room, and a request sent to its
        // lane; then a request fills it before its loop takes either: its
        // loop takes events and the connection (Room::Events), or, its
        // backlog of events full too, the connection alone, an event left
        // in its inbox (Room::Connections).
        for room in [Room::Events, Room::Connections] {
            let server = inline_loop_server(2, 1);
            let slots = server.slots.read().clone();
            let client = placed_on_first(&server).await;
            let lane = crate::registry::LaneItem::Request(test_ctx());
            assert!(slots[0].send_lane(lane).is_ok());
            let running = test_ctx();
            crate::queue::enlist_inline(&server, &slots[0], &running);
            let inbox = slots[0].lane.ws_tx.as_ref().expect("an inbox");
            if room == Room::Connections {
                assert!(inbox.try_send(crate::ws::WsEvent::Close { id: 1 }).is_ok());
            }
            send_get(&client, "/placed", false);

            let taker = Taker::new(&server, &slots[0]);
            let Take::Item(Item::Conn(mut conn)) = taker.nonblock(room) else {
                panic!("{room:?}: a full worker left the connection placed on it waiting");
            };
            assert!(
                matches!(taker.nonblock(room), Take::Nothing),
                "{room:?}: took more"
            );
            let lane = slots[0].lane.rx.as_ref().expect("a lane").len();
            let inbox = slots[0].lane.ws_rx.as_ref().expect("an inbox").len();
            let event = usize::from(room == Room::Connections);
            assert_eq!(
                (lane, inbox),
                (1, event),
                "{room:?}: the request or the event was taken"
            );
            await_bytes(&conn);
            assert!(
                matches!(conn.next(false), Next::Closed),
                "{room:?}: kept a request"
            );
            assert_eq!(handbacks(&server), 1);
            answer_dispatched(&server, &["/placed"]).await;
            assert_eq!(Wire::of(&client).response().2, b"hyper:/placed");
        }
    }

    /// On a runtime of one thread, so that nothing reports the socket,
    /// registered anew, readable before hyper first reads it.
    #[test]
    fn a_connection_its_worker_refuses_as_it_is_sent_is_declined_and_served_by_hyper() {
        // Its worker retires as placement picks it; as the drain begins
        // too, hyper still answers the request already sent, and closes.
        for drain in [false, true] {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build();
            runtime.expect("a runtime").block_on(async {
                let server = inline_loop_server(2, 8);
                let _accepting = server.shutdown_tx.subscribe();
                let inline = server.inline.clone().expect("inline on");
                let draining = server.clone();
                *inline.on_pick.lock() = Some(Box::new(move |slot: &WorkerSlot| {
                    slot.retire();
                    if drain {
                        draining.stop_accepting();
                    }
                }));
                let (client, stream, held, peer) = accepted(1).await.pop().expect("one");
                send_get(&client, "/refused", false);
                stream.peek(&mut [0u8]).await.expect("its bytes in");
                let accepted = crate::server::Accepted {
                    conn: Either::Left(stream),
                    peer,
                    held,
                };
                tokio::spawn(crate::server::serve_conn(accepted, None, server.clone()));
                let answered = tokio::time::timeout(
                    std::time::Duration::from_secs(3),
                    answer_dispatched(&server, &["/refused"]),
                );
                assert!(answered.await.is_ok(), "drain {drain}: never read");
                let reading = tokio::task::spawn_blocking(move || Wire::of(&client).response());
                let (_, headers, body) = reading.await.expect("the client");
                assert_eq!(body, b"hyper:/refused", "drain {drain}");
                let closes = headers.contains(&"connection: close".to_string());
                assert_eq!(closes, drain, "drain {drain}: {headers:?}");
                assert!(
                    inline.on_pick.lock().is_none(),
                    "placement picked no worker"
                );
                let counts = (conns(&server), declined(&server), handbacks(&server));
                assert_eq!(counts, (vec![0, 0], 1, 0), "drain {drain}");
            });
        }
    }
}
