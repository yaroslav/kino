use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;

use super::*;
use crate::queue::testing::tick;
use crate::request::test_ctx;
use crate::slot::LANE_DEPTH;
use crate::testing::{assign, slot, TestServer, SERIAL};
use crate::ws::testing::QueuedSocket;

/// What a worker is doing when requests come.
#[derive(Clone, Copy)]
enum Was {
    Awake,
    /// Parked, the n-th to park: the higher, the hotter its caches.
    Parked(u64),
    /// Awake, 50 ms into a request.
    Deep,
    /// Awake, just into a request.
    Just,
    /// Awake and taking again, its last request a slow one.
    Back,
    Retired,
    Crashed,
    /// Retired, then reset for a new worker.
    Reset,
    /// Its lane closed, as the drain closes them.
    Closed,
    /// Parked n-th, owning a socket.
    Owning(u64),
    /// Parked n-th, its sockets being closed: it refuses handshakes.
    Closing(u64),
    /// Parked n-th, its lane full.
    Full(u64),
}
use Was::*;

#[derive(Clone, Copy)]
enum Kind {
    Request,
    Handshake,
}
use Kind::*;

/// Marks `slot`'s worker awake and in a request that started at
/// `started_us` on the mono clock.
fn busy_since(slot: &WorkerSlot, started_us: u64) {
    slot.park.parked.store(false, Relaxed);
    slot.load.in_flight.store(1, Relaxed);
    slot.load.last_started_us.store(started_us, Relaxed);
}

/// 50 ms into a request.
fn deep_in_a_request(slot: &WorkerSlot) {
    const DEEP: u64 = 50_000;
    while crate::mono::mono_us() < DEEP {
        std::thread::sleep(Duration::from_millis(1));
    }
    busy_since(slot, crate::mono::mono_us() - DEEP);
}

/// A lanes server whose workers are as `workers` says, and the sockets
/// they own.
fn server_where(workers: &[Was]) -> (Arc<ServerInner>, Vec<QueuedSocket>) {
    let server = TestServer::lanes().workers(workers.len()).build();
    let mut sockets = Vec::new();
    for (index, &was) in workers.iter().enumerate() {
        let slot = slot(&server, index);
        let park = |at| {
            slot.park.parked.store(true, Relaxed);
            slot.park.parked_at.store(at, Relaxed);
        };
        match was {
            Awake => {}
            Parked(at) => park(at),
            Deep => deep_in_a_request(&slot),
            // Stamped a second ahead, so a slow run still reads it as new.
            Just => busy_since(&slot, crate::mono::mono_us() + 1_000_000),
            Back => {
                deep_in_a_request(&slot);
                slot.load.in_flight.store(0, Relaxed);
            }
            Retired => slot.retire(),
            Crashed => {
                slot.crash();
            }
            Reset => {
                slot.retire();
                slot.reset();
            }
            Closed => drop(slot.lane.tx.lock().take()),
            Owning(at) => {
                park(at);
                sockets.push(QueuedSocket::with_limits(slot.clone(), Default::default()));
            }
            Closing(at) => {
                park(at);
                crate::ws::close_slot(&slot, crate::ws::SERVER_ERROR, crate::ws::Cause::Crash);
            }
            Full(at) => {
                park(at);
                (0..LANE_DEPTH).for_each(|_| assign(&slot));
            }
        }
    }
    (server, sockets)
}

/// How one attempt to dispatch `ctx` ends: "sent", "full" (every open
/// lane is) or "closed" (every lane is).
fn dispatch(server: &ServerInner, ctx: BoxedCtx) -> &'static str {
    match try_dispatch(server, ctx) {
        Attempt::Break(true) => "sent",
        Attempt::Continue(_) => "full",
        Attempt::Break(false) => "closed",
    }
}

/// Dispatching `requests` to workers that are as `workers` says sends each
/// one and leaves the lanes `depths` deep; a failure points at the caller's
/// line.
#[track_caller]
fn places(workers: &[Was], requests: &[Kind], depths: &[usize]) {
    let (server, _sockets) = server_where(workers);
    for kind in requests {
        let ctx = match kind {
            Request => test_ctx(),
            Handshake => crate::upgrade::test_handshake(&server),
        };
        assert_eq!(dispatch(&server, ctx), "sent");
    }
    assert_eq!(server.lane_depths().as_deref(), Some(depths));
}

#[test]
fn a_request_goes_to_an_awake_worker_with_room_or_the_idle_one_that_parked_last() {
    // Sockets count process-wide; the socket tests take this lock too.
    let _serial = SERIAL.blocking_lock();
    // Round robin over awake lanes.
    places(&[Awake, Awake], &[Request; 4], &[2, 2]);
    // An awake lane before a parked one.
    places(&[Parked(1), Awake], &[Request; 2], &[0, 2]);
    // The idle one parked last.
    places(&[Parked(1), Parked(3), Parked(2)], &[Request], &[0, 1, 0]);
    // A burst, hottest first.
    places(
        &[Parked(1), Parked(3), Parked(2)],
        &[Request; 2],
        &[0, 1, 1],
    );
    // Never a retired lane, nor the only awake one.
    places(&[Retired, Awake], &[Request; 2], &[0, 2]);
    places(&[Retired, Parked(1)], &[Request], &[0, 1]);
    // A reset lane again.
    places(&[Reset, Awake], &[Request; 4], &[2, 2]);
    // Past one deep in a slow request, but not one just into a request,
    // nor one back from a slow request.
    places(&[Deep, Parked(3), Parked(2)], &[Request], &[0, 1, 0]);
    places(&[Just, Parked(3), Parked(2)], &[Request], &[1, 0, 0]);
    places(&[Back, Parked(3), Parked(2)], &[Request], &[1, 0, 0]);
    // Busy workers alike with no idle sibling: a crashed worker is none,
    // nor a retired one.
    places(&[Deep, Just], &[Request; 2], &[1, 1]);
    places(&[Deep, Just, Crashed], &[Request; 2], &[1, 1, 0]);
    places(&[Deep, Just, Retired], &[Request; 2], &[1, 1, 0]);
    // A handshake: the hottest of those with the fewest sockets; then a
    // request: the hottest.
    places(&[Parked(1), Owning(3), Parked(2)], &[Handshake], &[0, 0, 1]);
    places(
        &[Parked(1), Owning(3), Parked(2)],
        &[Handshake, Request],
        &[0, 1, 1],
    );
    // A handshake past a full lane, and past a worker that would refuse it.
    places(&[Full(2), Owning(1)], &[Handshake], &[LANE_DEPTH, 1]);
    places(&[Closing(2), Owning(1)], &[Handshake], &[0, 1]);
}

#[test]
fn a_request_no_lane_takes_comes_back_for_a_retry_unless_every_lane_closed() {
    // A replacement may still take what a retired lane would not.
    let cases: [(&str, &[Was], bool); 5] = [
        ("no workers", &[], false),
        ("every lane full", &[Full(1)], false),
        ("only a retired lane", &[Retired], false),
        ("a closed lane and a retired one", &[Closed, Retired], false),
        ("every lane closed: the drain", &[Closed], true),
    ];
    for (what, workers, closed) in cases {
        let (server, _sockets) = server_where(workers);
        let outcome = dispatch(&server, test_ctx());
        assert_eq!(outcome, if closed { "closed" } else { "full" }, "{what}");
    }
}

#[test]
fn a_parked_workers_tick_takes_a_request_stuck_behind_a_slow_sibling() {
    let (server, _sockets) = server_where(&[Parked(1), Parked(2)]);
    // Queued while its worker had only just started what then ran long.
    assign(&slot(&server, 0));
    deep_in_a_request(&slot(&server, 0));

    assert_eq!(tick(&server, &slot(&server, 1)), "request");
    assert_eq!(server.lane_depths(), Some(vec![0, 0]));
}
