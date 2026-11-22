use std::sync::atomic::Ordering::{self, Relaxed};
use std::time::Duration;

use super::*;
use crate::queue::testing::{admit_next, boot, nonblock, rung, take_admitted, tick};
use crate::queue::Room;
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
                slot.crash(&server);
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
        Attempt::Break(Ok(())) => "sent",
        Attempt::Continue(_) => "full",
        Attempt::Break(Err(_)) => "closed",
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
fn a_request_no_lane_takes_comes_back_to_wait_for_lane_room_unless_every_lane_closed() {
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

#[test]
fn a_worker_loop_worker_is_passed_over_for_an_idle_sibling_only_while_it_holds_what_it_took() {
    use crate::queue::Worker;
    // Each leaves the worker as `busy` says, handing back a request
    // it keeps open.
    type Busy = fn(&ServerInner, &Arc<WorkerSlot>, &Worker) -> Option<BoxedCtx>;
    fn took(server: &ServerInner, slot: &Arc<WorkerSlot>) -> BoxedCtx {
        assign(slot);
        admit_next(server, slot)
    }
    // One row per case; rustfmt would split each row field by field.
    #[rustfmt::skip]
    let cases: [(&str, Busy, [usize; 2]); 6] = [
        ("a loop yet to take", |_, _, _| None, [1, 0]),
        ("its loop back after answering, as at one client",
            |server, slot, worker| { drop(took(server, slot)); Worker::woke(worker); None }, [1, 0]),
        ("its loop back, a long poll's fiber waiting",
            |server, slot, worker| { let poll = took(server, slot); Worker::woke(worker); Some(poll) }, [1, 0]),
        ("its thread away with a request it just took", |server, slot, _| Some(took(server, slot)), [0, 1]),
        ("its job past its answer, not back in its loop yet",
            |server, slot, _| { let job = took(server, slot); slot.answered(&job.responder, false); Some(job) }, [1, 0]),
        ("its thread away in its wake hook",
            |server, slot, _| { assert!(slot.wake()); assert_eq!(nonblock(server, slot, Room::All), "wake"); None }, [0, 1]),
    ];
    for (busy, make, depths) in cases {
        let server = TestServer::worker_loop().workers(2).build();
        let (looping, worker) = boot(&server, 0);
        let idle = slot(&server, 1);
        assert_eq!(nonblock(&server, &idle, Room::All), "nothing");
        let _open = make(&server, &looping, &worker);

        assert_eq!(dispatch(&server, test_ctx()), "sent");
        assert_eq!(
            server.lane_depths(),
            Some(depths.to_vec()),
            "a worker_loop worker with {busy}, its sibling idle"
        );
    }
}

#[test]
fn a_crashed_worker_loop_workers_slot_goes_to_its_replacement_doorbell_and_all() {
    let server = TestServer::worker_loop().workers(1).build();
    let open_fds = || std::fs::read_dir("/dev/fd").expect("/dev/fd").count();
    let before = open_fds();

    // Crash after crash, each replacement taking the slot over as the
    // pools do; a dead worker's handle goes with its heap.
    for _ in 0..200 {
        let (slot, worker) = boot(&server, 0);
        assert_eq!(nonblock(&server, &slot, Room::All), "nothing");
        slot.abort(crate::response::Cut::Crash);
        drop(worker);
        assert!(slot.recycle().is_some(), "a quiet crashed slot is recycled");
    }

    let grew = open_fds().saturating_sub(before);
    assert!(
        grew < 100,
        "200 crashed workers left {grew} more descriptors open: a crash loop would exhaust them"
    );
    let (slot, worker) = boot(&server, 0);
    assert!(
        !rung(&worker),
        "a replacement must not wake for a ring its predecessor never answered"
    );
    assert_eq!(nonblock(&server, &slot, Room::All), "nothing");
    assert_eq!(dispatch(&server, test_ctx()), "sent");
    assert!(
        rung(&worker),
        "a recycled slot's doorbell must ring for the worker that took it over"
    );
}

#[test]
fn a_worker_loop_worker_at_capacity_gets_no_request_and_all_at_capacity_is_overload() {
    let server = TestServer::worker_loop().workers(3).capacity(1).build();
    let (full, colder, warmer) = (slot(&server, 0), slot(&server, 1), slot(&server, 2));
    // Awake, the full worker is the one an awake lane is looked for in.
    assign(&full);
    let mut open = vec![admit_next(&server, &full)];
    for worker in [&colder, &warmer] {
        assert_eq!(nonblock(&server, worker, Room::All), "nothing", "idle");
    }
    assert_eq!(dispatch(&server, test_ctx()), "sent");
    assert_eq!(server.lane_depths(), Some(vec![0, 0, 1]), "awake");

    // Parked too, and last: the hottest, but for its capacity. (Its loop
    // would take events only; a full take would steal the sibling's request.)
    full.park.park(&server);
    assert_eq!(dispatch(&server, test_ctx()), "sent");
    assert_eq!(server.lane_depths(), Some(vec![0, 1, 1]), "idle");

    open.extend([admit_next(&server, &colder), admit_next(&server, &warmer)]);
    assert_eq!(
        dispatch(&server, test_ctx()),
        "full",
        "every worker at capacity is overload, waiting for room until queue_timeout, not a closed server"
    );
}

#[test]
fn idle_worker_loop_workers_take_requests_fewest_open_first_then_hottest_first() {
    let server = TestServer::worker_loop().workers(3).build();
    // Parked in slot order, so slot 2 parked last; slots 1 and 2 each
    // hold a request that waits, slot 0 none.
    let mut waiting = Vec::new();
    for index in 0..3 {
        let worker = slot(&server, index);
        if index > 0 {
            assign(&worker);
            waiting.push(admit_next(&server, &worker));
        }
        assert_eq!(nonblock(&server, &worker, Room::All), "nothing", "parked");
    }

    let expected = [
        (vec![1, 0, 0], "the worker with no request open"),
        (
            vec![1, 0, 1],
            "of those with one open, the one that parked last",
        ),
        (vec![1, 1, 1], "the last idle worker"),
    ];
    for (depths, taker) in expected {
        assert_eq!(dispatch(&server, test_ctx()), "sent");
        assert_eq!(server.lane_depths(), Some(depths), "{taker} takes it");
    }
}

/// Dispatches `count` requests, each of which must find a lane, and
/// stays in flight, as handle_request counts it.
fn dispatch_all(server: &ServerInner, count: usize) {
    for _ in 0..count {
        server.counters.in_flight.fetch_add(1, Ordering::Relaxed);
        assert_eq!(dispatch(server, test_ctx()), "sent");
    }
}

#[test]
fn a_worker_loop_lane_holds_as_many_requests_as_its_worker_has_room_for() {
    // Before its serve sets a capacity, and with less room than a
    // classic lane's depth, a lane holds as many as a classic one.
    let cases = [
        (0, 0, LANE_DEPTH),
        (2, 0, LANE_DEPTH),
        (16, 0, 16),
        (16, 6, 10),
        (16, 14, LANE_DEPTH),
    ];
    for (capacity, open, depth) in cases {
        let server = TestServer::worker_loop()
            .workers(1)
            .capacity(capacity)
            .build();
        slot(&server, 0)
            .load
            .in_flight
            .store(open, Ordering::Relaxed);

        dispatch_all(&server, depth);
        assert_eq!(server.lane_depths(), Some(vec![depth]));
        assert_eq!(
            dispatch(&server, test_ctx()),
            "full",
            "the lane of a worker with capacity {capacity}, {open} open, holds {depth} requests"
        );
    }
}

#[test]
fn a_hot_worker_loop_lane_takes_one_while_a_sibling_idles_and_fills_as_far_as_a_classic_one_once_none_does(
) {
    let server = TestServer::worker_loop().workers(2).capacity(16).build();
    assert_eq!(
        nonblock(&server, &slot(&server, 1), Room::All),
        "nothing",
        "idle"
    );

    let expected = [
        (1, vec![1, 0], "the awake worker keeps its hot lane"),
        (
            1,
            vec![1, 1],
            "the idle worker takes the next, rather than it waiting behind the first",
        ),
        (
            LANE_DEPTH - 1,
            vec![LANE_DEPTH, 1],
            "with no worker idle, the hot lane fills as far as a classic one",
        ),
    ];
    for (count, depths, why) in expected {
        dispatch_all(&server, count);
        assert_eq!(server.lane_depths(), Some(depths), "{why}");
    }
}

// --- Waiting for lane room. The test plays the worker on its own
// task, on a paused clock: a waiting request runs only when the test
// yields, and no timer fires meanwhile, so one that goes in went in
// because a worker made room.

/// A lanes server with one worker, a worker_loop one or a classic one,
/// whose requests wait for lane room up to `queue_timeout_ms`: in line
/// on a worker_loop server, retrying without one.
fn one_worker_waiting(worker_loop: bool, queue_timeout_ms: u64) -> Arc<ServerInner> {
    TestServer::lanes()
        .workers(1)
        .with(|c| {
            c.worker_loop = worker_loop;
            c.queue_timeout_ms = queue_timeout_ms;
        })
        .build()
}

/// A request for `/n`.
fn numbered(n: usize) -> BoxedCtx {
    let mut ctx = test_ctx();
    ctx.uri = format!("/{n}").parse().expect("a path");
    ctx
}

/// Lets the runtime's ready tasks run, a few rounds.
async fn settle() {
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
}

/// Fills the only lane of `server` with /0 to /3.
async fn fill_the_lane(server: &Arc<ServerInner>) {
    for n in 0..LANE_DEPTH {
        assert!(enqueue(server, numbered(n)).await);
    }
}

/// A dispatch of `/n`, once it lines up for lane room.
async fn waiting_dispatch(server: &Arc<ServerInner>, n: usize) -> tokio::task::JoinHandle<bool> {
    let before = server.lane_room.waiting();
    let task = tokio::spawn({
        let server = server.clone();
        async move { enqueue(&server, numbered(n)).await }
    });
    settle().await;
    assert_eq!(server.lane_room.waiting(), before + 1, "/{n} waits");
    task
}

/// The path of the next request the worker takes, the way its kind of
/// worker takes it.
fn take_path(server: &Arc<ServerInner>) -> String {
    let worker = slot(server, 0);
    let ctx = match server.config.worker_loop {
        true => admit_next(server, &worker),
        false => take_admitted(server, &worker),
    };
    ctx.uri.path().to_string()
}

fn paths(numbers: impl IntoIterator<Item = usize>) -> Vec<String> {
    numbers.into_iter().map(|n| format!("/{n}")).collect()
}

#[tokio::test(start_paused = true)]
async fn requests_that_found_every_lane_full_go_in_as_room_appears_in_the_order_they_came() {
    let server = one_worker_waiting(true, 60_000);
    fill_the_lane(&server).await;
    let mut waiting = Vec::new();
    for n in LANE_DEPTH..LANE_DEPTH + 3 {
        waiting.push(waiting_dispatch(&server, n).await);
    }
    assert_eq!(
        server.queued(),
        LANE_DEPTH + 3,
        "those waiting for lane room count as queued"
    );

    // Each take lets the next in line in before the take after it.
    let mut taken = Vec::new();
    for _ in 0..LANE_DEPTH + 3 {
        taken.push(take_path(&server));
        settle().await;
    }
    assert_eq!(taken, paths(0..LANE_DEPTH + 3), "in the order they came");
    for task in waiting {
        assert!(task.await.expect("the dispatch task"));
    }
}

#[tokio::test(start_paused = true)]
async fn without_a_worker_loop_a_request_that_found_every_lane_full_looks_again_at_its_next_retry()
{
    let server = one_worker_waiting(false, 60_000);
    fill_the_lane(&server).await;
    let waiting = tokio::spawn({
        let server = server.clone();
        async move { enqueue(&server, numbered(LANE_DEPTH)).await }
    });
    settle().await;
    assert_eq!(server.lane_room.waiting(), 0, "nobody lines up");
    assert_eq!(server.queued(), LANE_DEPTH, "nor counts as queued");

    assert_eq!(take_path(&server), "/0");
    settle().await;
    assert!(!waiting.is_finished(), "the room is not told of");
    // The retry, LANE_RETRY on, which the runtime's timer rounds up
    // to its next millisecond.
    tokio::time::advance(Duration::from_millis(1)).await;
    settle().await;

    assert!(waiting.is_finished(), "the next retry finds the room");
    assert!(waiting.await.expect("the dispatch task"));
    let taken: Vec<String> = (0..LANE_DEPTH).map(|_| take_path(&server)).collect();
    assert_eq!(taken, paths([1, 2, 3, LANE_DEPTH]));
}

#[tokio::test(start_paused = true)]
async fn a_newcomer_lines_up_behind_a_waiting_request_rather_than_take_the_room_it_was_told_of() {
    let server = one_worker_waiting(true, 60_000);
    fill_the_lane(&server).await;
    let waiting = waiting_dispatch(&server, LANE_DEPTH).await;

    assert_eq!(take_path(&server), "/0");
    // Comes in before the waiting request had its look.
    let newcomer = enqueue(&server, numbered(99));
    assert!(
        tokio::time::timeout(Duration::ZERO, newcomer)
            .await
            .is_err(),
        "the newcomer must wait its turn"
    );
    settle().await;

    assert!(waiting.is_finished(), "the waiting request goes in first");
    assert!(waiting.await.expect("the dispatch task"));
    let taken: Vec<String> = (0..LANE_DEPTH).map(|_| take_path(&server)).collect();
    assert_eq!(taken, paths(1..=LANE_DEPTH));
}

#[tokio::test(start_paused = true)]
async fn a_full_worker_loop_worker_lets_a_waiting_request_in_once_a_job_ends() {
    type Next = fn(&ServerInner, &Arc<WorkerSlot>);
    let cases: [(&str, Next); 2] = [
        ("parks", |server, worker| {
            assert_eq!(
                nonblock(server, worker, Room::All),
                "nothing",
                "nothing to take"
            );
        }),
        ("takes its wake hook's turn", |server, worker| {
            assert!(worker.wake());
            assert_eq!(nonblock(server, worker, Room::All), "wake");
        }),
    ];
    for (then, next) in cases {
        let server = one_worker_waiting(true, 60_000);
        let worker = slot(&server, 0);
        worker.load.set_capacity(1);
        assign(&worker);
        let job = admit_next(&server, &worker);
        let waiting = waiting_dispatch(&server, 1).await;

        drop(job); // answered, and its job ended
        next(&server, &worker);
        settle().await;

        assert!(
            waiting.is_finished(),
            "the waiting request goes in once the worker's loop is back with room and {then}"
        );
        assert!(waiting.await.expect("the dispatch task"));
        assert_eq!(server.lane_depths(), Some(vec![1]));
    }
}

#[tokio::test(start_paused = true)]
async fn a_worker_joining_the_pool_lets_a_waiting_request_in_as_it_parks() {
    let server = TestServer::worker_loop().workers(0).build();
    let waiting = tokio::spawn({
        let server = server.clone();
        async move { enqueue(&server, numbered(1)).await }
    });
    settle().await;
    assert_eq!(server.lane_room.waiting(), 1, "no worker yet");

    server.register_worker().expect("a worker slot");
    assert_eq!(nonblock(&server, &slot(&server, 0), Room::All), "nothing");
    settle().await;

    assert!(waiting.is_finished(), "the waiting request goes in");
    assert!(waiting.await.expect("the dispatch task"));
    assert_eq!(server.lane_depths(), Some(vec![1]));
}

#[tokio::test(start_paused = true)]
async fn a_waiting_request_whose_client_went_away_hands_its_turn_on() {
    let server = one_worker_waiting(true, 60_000);
    fill_the_lane(&server).await;
    let gone = waiting_dispatch(&server, LANE_DEPTH).await;
    let next = waiting_dispatch(&server, LANE_DEPTH + 1).await;

    // Room is made and the first in line told, but its client goes away
    // before it looks.
    assert_eq!(take_path(&server), "/0");
    gone.abort();
    settle().await;

    assert!(next.is_finished(), "the next in line goes in");
    assert!(next.await.expect("the dispatch task"));
    assert_eq!(server.lane_room.waiting(), 0);
    let taken: Vec<String> = (0..LANE_DEPTH).map(|_| take_path(&server)).collect();
    assert_eq!(taken, paths([1, 2, 3, LANE_DEPTH + 1]));
}

#[tokio::test(start_paused = true)]
async fn closing_the_lanes_turns_every_waiting_request_away() {
    let server = one_worker_waiting(true, 60_000);
    fill_the_lane(&server).await;
    let mut waiting = Vec::new();
    for n in LANE_DEPTH..LANE_DEPTH + 3 {
        waiting.push(waiting_dispatch(&server, n).await);
    }

    server.close_queues();
    settle().await;

    for task in waiting {
        assert!(task.is_finished(), "turned away without a timer");
        assert!(!task.await.expect("the dispatch task"));
    }
    assert_eq!(server.lane_room.waiting(), 0);
}

#[tokio::test(start_paused = true)]
async fn a_request_that_finds_no_lane_room_within_queue_timeout_gives_up() {
    for worker_loop in [false, true] {
        let server = one_worker_waiting(worker_loop, 200);
        fill_the_lane(&server).await;
        let started = tokio::time::Instant::now();

        assert!(!enqueue(&server, numbered(LANE_DEPTH)).await);
        assert_eq!(
            started.elapsed(),
            Duration::from_millis(200),
            "worker_loop {worker_loop}"
        );
        assert_eq!(server.lane_room.waiting(), 0);
    }
}

#[test]
fn a_dead_workers_late_end_never_touches_the_worker_that_took_its_slot_over() {
    let server = TestServer::worker_loop().workers(1).build();
    let (slot, dead) = boot(&server, 0);
    // Its Ractor ended, and the supervisor gave up on it, while its
    // thread still unwinds.
    slot.crash(&server);
    assert!(
        slot.recycle().is_none(),
        "the slot went to a new worker while its last one still held it"
    );

    crate::queue::Worker::crashed(&dead);
    crate::queue::Worker::release(&dead);
    assert!(slot.recycle().is_some(), "a slot let go of is recycled");
    let (_, taker) = boot(&server, 0);
    slot.load.set_capacity(8);
    let generation = slot.generation();
    // The successor took a request it has not answered yet.
    let (ctx, mut client) = crate::request::test_ctx_with_client();
    assert!(slot.send_lane(LaneItem::Request(ctx)).is_ok());
    let serving = admit_next(&server, &slot);

    // Its end, come again.
    crate::queue::Worker::crashed(&dead);
    crate::queue::Worker::release(&dead);
    drop(dead);
    assert!(
        matches!(
            client.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ),
        "a dead worker's handle answered a request its successor serves"
    );
    assert_eq!(
        slot.load.current.lock().len(),
        1,
        "the successor's request left its in-flight list"
    );
    assert_eq!(
        (
            slot.life.retired.load(Ordering::SeqCst),
            slot.load.capacity.load(Ordering::Relaxed),
            slot.generation()
        ),
        (false, 8, generation),
        "a dead worker's handle retired, emptied or moved on the slot its successor holds"
    );
    assert_eq!(dispatch(&server, test_ctx()), "sent");
    drop((serving, taker));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closing_the_queues_hands_connections_no_worker_took_to_hyper() {
    use crate::inline::testing::{inline_loop_server, placed, read_to_end, send_get};

    let server = inline_loop_server(1, 8);
    let client = placed(&server);

    // The drain, with a worker that never took its connection.
    server.stop_accepting();
    server.close_queues();

    let worker = slot(&server, 0);
    assert!(!worker.lane.placed_waiting());
    assert_eq!(worker.inline.conns.load(Ordering::Relaxed), 0);
    // Hyper serves it now: the drain's answer, then the end.
    send_get(&client, "/", true);
    let answer = String::from_utf8_lossy(&read_to_end(&client)).into_owned();
    assert!(answer.starts_with("HTTP/1.1 503 "), "{answer}");
}
