use std::sync::atomic::AtomicBool;
use std::time::Duration;

use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::take::OWN_RUN;
use super::testing::{admit_next, boot, kind, nonblock, ready, rung, socket_sends, tick};
use super::*;
use crate::registry::{BoxedCtx, ServerInner};
use crate::slot::{WorkerSlot, LANE_DEPTH};
use crate::testing::{assign, slot, InPlace, TestServer};
use crate::ws;

/// A classic worker's take as Worker::checkout makes it, where `release`
/// and `interrupts` stand for the VM (InPlace).
fn take(
    server: &ServerInner,
    slot: &Arc<WorkerSlot>,
    release: impl FnMut(&AtomicBool, &mut dyn FnMut()),
    interrupts: impl FnMut() -> Result<(), &'static str>,
) -> Result<Taken, &'static str> {
    let mut parker = InPlace {
        release,
        interrupts,
    };
    Taker::new(server, slot).take(&mut parker)
}

/// The number a message from `socket_sends` carries.
fn number(event: &ws::WsEvent) -> usize {
    match event {
        ws::WsEvent::Message { data, .. } => std::str::from_utf8(data)
            .expect("utf-8")
            .parse()
            .expect("a number"),
        other => panic!("took {other:?}, expected a message"),
    }
}

#[test]
fn a_take_keeps_waiting_after_a_timeout_unless_the_slot_is_retired() {
    let server = TestServer::shared_queue().workers(1).build();
    let slot = slot(&server, 0);
    assert_eq!(tick(&server, &slot), "nothing");

    slot.retire();
    let ended = tick(&server, &slot);
    assert_eq!(ended, "the end", "a retired shared-queue worker leaves");
}

#[test]
fn a_retired_lane_worker_drains_every_item_one_per_timeout() {
    let server = TestServer::lanes().workers(1).build();
    let slot = slot(&server, 0);
    for _ in 0..3 {
        assign(&slot);
    }
    slot.retire();

    let ticks = [(); 4].map(|()| tick(&server, &slot));
    assert_eq!(ticks, ["request", "request", "request", "the end"]);
}

#[test]
fn a_retired_worker_takes_no_request_from_a_sibling() {
    let server = TestServer::lanes().workers(2).build();
    let (retiring, sibling) = (slot(&server, 0), slot(&server, 1));
    retiring.retire();
    assign(&sibling);

    let taken = ready(&server, &retiring);
    assert_eq!(taken, "nothing", "a retired worker must not steal");
    assert_eq!(tick(&server, &retiring), "the end", "it leaves instead");
    let waiting = sibling.lane.rx.as_ref().expect("lane").len();
    assert_eq!(waiting, 1, "the request waits for its own worker");
}

#[test]
fn a_worker_a_tick_woke_leaves_a_request_to_its_idle_owner() {
    let classic = TestServer::lanes().workers(2).build();
    let (owner, sibling) = (slot(&classic, 0), slot(&classic, 1));
    owner.park.park(&classic);
    assign(&owner);
    let ticked = tick(&classic, &sibling);
    assert_eq!(ticked, "nothing", "the send woke the lane's own worker");
    let back_from_work = ready(&classic, &sibling);
    assert_eq!(back_from_work, "request", "a busy sibling still takes it");

    let looped = TestServer::worker_loop().workers(2).build();
    let (owner, sibling) = (slot(&looped, 0), slot(&looped, 1));
    for waiting in [&owner, &sibling] {
        assert_eq!(nonblock(&looped, waiting, Room::All), "nothing");
    }
    assign(&owner);
    let ticked = nonblock(&looped, &sibling, Room::All);
    assert_eq!(ticked, "nothing", "the send rang the lane's own worker");
    assert_eq!(nonblock(&looped, &owner, Room::All), "request");
}

#[test]
fn a_sibling_hands_a_doorbell_back_to_its_owner() {
    let server = TestServer::lanes().workers(2).build();
    let (owner, sibling) = (slot(&server, 0), slot(&server, 1));
    socket_sends(&owner, 0);
    owner.notify();

    let stolen = ready(&server, &sibling);
    assert_eq!(stolen, "nothing", "a socket's events stay with its worker");
    let bells = owner.lane.rx.as_ref().expect("lane").len();
    assert_eq!(bells, 1, "the doorbell is back in its owner's lane");
    assert_eq!(ready(&server, &owner), "event");
}

#[test]
fn wakes_until_its_worker_takes_one_ring_one_doorbell_and_count_once() {
    let server = TestServer::lanes().workers(1).build();
    let (slot, worker) = boot(&server, 0);

    assert!(slot.wake(), "a lane worker can be woken");
    assert!(slot.wake(), "so can one already due to wake");
    let bells = slot.lane.rx.as_ref().expect("lane").len();
    assert_eq!(bells, 1, "two wakes ring one doorbell");
    let taken = worker.waiting(&server).expect("take");
    assert_eq!(kind(&taken), "wake", "the worker takes :wake");
    let again = ready(&server, &slot);
    assert_eq!(again, "nothing", "the two wakes are taken as one");
    assert_eq!(
        crate::stats::Snapshot::take(&server).wakes,
        1,
        "and counted once"
    );
}

#[test]
fn a_parked_worker_takes_a_wake_whose_doorbell_met_a_full_lane() {
    let server = TestServer::lanes().workers(2).build();
    let (owner, sibling) = (slot(&server, 0), slot(&server, 1));
    for _ in 0..LANE_DEPTH {
        assign(&owner);
    }
    assert!(owner.wake());
    // The requests that kept the doorbell out go to a sibling.
    for _ in 0..LANE_DEPTH {
        assert_eq!(ready(&server, &sibling), "request");
    }

    let ticked = tick(&server, &owner);
    assert_eq!(
        ticked, "wake",
        "the wake must not wait for the owner's next request"
    );
}

#[test]
fn a_socket_flooding_its_worker_lets_a_waiting_request_then_a_due_wake_in_after_each_run() {
    let server = TestServer::lanes().workers(1).build();
    let worker = slot(&server, 0);
    (0..100).for_each(|n| socket_sends(&worker, n));
    let (mut sent, mut numbers) = (100, Vec::new());
    // The client keeps sending as fast as the worker takes.
    let mut takes = |count: usize| -> Vec<&'static str> {
        let mut take = || {
            socket_sends(&worker, sent);
            sent += 1;
            let taken = Taker::new(&server, &worker).ready();
            if let Take::Item(Item::Ws(event)) = &taken {
                numbers.push(number(event));
            }
            kind(&taken)
        };
        (0..count).map(|_| take()).collect()
    };
    let run = || vec!["event"; OWN_RUN as usize];

    assign(&worker);
    assert!(worker.wake());
    assert_eq!(
        takes(2 * OWN_RUN as usize + 2),
        [run(), vec!["request", "wake"], run()].concat(),
        "after a run of the socket's events, the lane gets a look, then a due wake its turn"
    );
    assert!(worker.wake());
    assert_eq!(
        takes(1 + 2 * OWN_RUN as usize),
        [vec!["wake"], run(), run()].concat(),
        "a due wake waits for no request, and alone the socket keeps its worker"
    );
    assert_eq!(
        numbers,
        (0..numbers.len()).collect::<Vec<_>>(),
        "the socket's messages arrive whole and in order"
    );
}

#[test]
fn a_parked_worker_takes_what_reached_its_inbox_as_its_lane_closed() {
    let server = TestServer::lanes().workers(1).build();
    let worker = slot(&server, 0);
    // While the worker waits, its socket sends and the queue closes:
    // the event's doorbell finds no lane.
    let release = |_: &AtomicBool, wait: &mut dyn FnMut()| {
        socket_sends(&worker, 0);
        worker.close_lane();
        wait();
    };

    let taken = take(&server, &worker, release, || Ok(()));
    assert!(
        matches!(&taken, Ok(Some(Item::Ws(event))) if number(event) == 0),
        "the event must be taken, not dropped with the lane"
    );
    assert_eq!(ready(&server, &worker), "the end");
}

#[test]
fn a_hook_that_wakes_its_own_worker_lets_a_waiting_request_go_first_and_never_keeps_it_closing() {
    let server = TestServer::lanes().workers(1).build();
    let worker = slot(&server, 0);
    let take = || ready(&server, &worker);
    worker.wake();
    assert_eq!(take(), "wake");

    // The hook wakes its worker again while a request waits.
    worker.wake();
    assign(&worker);
    assert_eq!(
        [take(), take()],
        ["request", "wake"],
        "a request waiting behind a wake goes first, and the wake still comes"
    );
    // It wakes it again as the lane closes.
    worker.wake();
    worker.close_lane();
    assert_eq!(take(), "the end", "a closing worker takes no more wakes");
}

#[test]
fn what_is_sent_while_a_worker_loop_worker_arms_its_doorbell_is_taken_not_missed() {
    let _serial = crate::testing::SERIAL.blocking_lock();
    let server = TestServer::worker_loop().workers(1).build();
    let (slot, worker) = boot(&server, 0);

    // The sends come between the worker's first look and its arming, so
    // they find the doorbell disarmed and stay quiet.
    let (taker, sender) = (Taker::new(&server, &slot), slot.clone());
    let doorbell = slot.lane.doorbell.as_ref().expect("a worker_loop lane");
    doorbell.before_arm(move || assign(&sender));
    let taken = taker.nonblock(Room::All);
    assert_eq!(
        kind(&taken),
        "request",
        "a request sent as an idle worker armed must be taken now, not at the next ring"
    );
    let sender = slot.clone();
    doorbell.before_arm(move || {
        socket_sends(&sender, 0);
        sender.notify();
    });
    let taken = taker.nonblock(Room::Events);
    assert_eq!(
        kind(&taken),
        "event",
        "an event sent as a full worker armed must be taken now"
    );

    assign(&slot);
    Worker::ring(&worker);
    assert!(
        !rung(&worker),
        "a worker that found work must not stay armed: each send would write the doorbell"
    );
}

#[test]
fn what_a_worker_loop_worker_waits_for_rings_its_doorbell() {
    let _serial = crate::testing::SERIAL.blocking_lock();
    type Arrive = fn(&ServerInner, &Arc<WorkerSlot>, &Worker);
    // How the worker waits (Room::All idle, Room::Events full, with as
    // many requests waiting in its lane), what arrives, and what its take
    // after the wait then takes.
    // One row per case; rustfmt would split each row field by field.
    #[rustfmt::skip]
    let rows: [(&str, Room, usize, Arrive, &str); 8] = [
        ("a request", Room::All, 0, |_, slot, _| assign(slot), "request"),
        ("a socket event", Room::All, 0, |_, slot, _| { socket_sends(slot, 0); slot.notify() }, "event"),
        ("a wake", Room::All, 0, |_, slot, _| assert!(slot.wake()), "wake"),
        ("a socket event, to a full worker", Room::Events, 0, |_, slot, _| { socket_sends(slot, 0); slot.notify() }, "event"),
        ("a socket event, its lane full", Room::Events, LANE_DEPTH, |_, slot, _| { socket_sends(slot, 0); slot.notify() }, "event"),
        ("a job's end, a request waiting", Room::Events, 1, |_, _, worker| Worker::ring(worker), "nothing"),
        ("the drain closing the lanes", Room::All, 0, |server, _, _| server.close_queues(), "the end"),
        ("its worker crashing", Room::All, 0, |_, slot, _| drop(slot.abort(crate::response::Cut::Crash)), "the end"),
    ];
    for (arrival, room, waiting, arrive, takes) in rows {
        let server = TestServer::worker_loop().workers(1).build();
        let (slot, worker) = boot(&server, 0);
        (0..waiting).for_each(|_| assign(&slot));
        let waits = nonblock(&server, &slot, room) == "nothing";
        assert!(waits && !rung(&worker), "{arrival}: nothing yet");

        arrive(&server, &slot, &worker);

        assert!(rung(&worker), "{arrival} must ring the doorbell");
        worker.off_doorbell();
        assert_eq!(nonblock(&server, &slot, room), takes, "{arrival}");
        if !matches!(takes, "nothing" | "the end") {
            slot.lane.ring_doorbell();
            assert!(
                !rung(&worker),
                "{arrival}: a worker that took what woke it is off its doorbell"
            );
        }
    }
}

#[test]
fn an_answer_leaves_a_full_worker_counted_full_until_its_take_with_room() {
    let server = TestServer::worker_loop().workers(1).build();
    let slot = slot(&server, 0);
    slot.load.capacity.store(1, Ordering::Relaxed);
    assign(&slot);
    let request = admit_next(&server, &slot);

    let responder = request.responder.clone();
    drop(request); // answered, its job maybe still running
    slot.answered(&responder);
    assert_eq!(
        slot.load.in_flight.load(Ordering::Relaxed),
        1,
        "only the take that tells a waiting request may count a full worker down"
    );
    assert_eq!(nonblock(&server, &slot, Room::All), "nothing");
    assert_eq!(slot.load.in_flight.load(Ordering::Relaxed), 0);
}

#[test]
fn a_full_worker_loop_worker_keeps_up_with_a_flooding_socket_and_lets_a_wake_in() {
    let _serial = crate::testing::SERIAL.blocking_lock();
    let server = TestServer::worker_loop().workers(1).build();
    let slot = slot(&server, 0);
    let count = 3 * OWN_RUN as usize;
    for n in 0..count {
        socket_sends(&slot, n);
    }
    assert!(slot.wake());

    let mut events = Vec::new();
    let mut wake = None;
    for take in 0..=count {
        match Taker::new(&server, &slot).nonblock(Room::Events) {
            Take::Item(Item::Ws(event)) => events.push(number(&event)),
            Take::Item(Item::Wake) => wake = Some(take),
            other => panic!("take {take} of a full worker took {}", kind(&other)),
        }
    }

    assert_eq!(events, (0..count).collect::<Vec<_>>());
    let wake = wake.expect("the wake never ran");
    assert!(
        wake <= OWN_RUN as usize,
        "the wake waited behind {wake} events"
    );
}

/// `take` on a thread of its own: a take still parked after five seconds
/// fails the test.
fn take_in_time(
    server: &Arc<ServerInner>,
    slot: &Arc<WorkerSlot>,
    release: impl FnMut(&AtomicBool, &mut dyn FnMut()) + Send + 'static,
    interrupts: impl FnMut() -> Result<(), &'static str> + Send + 'static,
) -> Result<Taken, &'static str> {
    let (taken_tx, taken) = flume::bounded(1);
    let (server, slot) = (server.clone(), slot.clone());
    std::thread::spawn(move || {
        let _ = taken_tx.send(take(&server, &slot, release, interrupts));
    });
    taken
        .recv_timeout(Duration::from_secs(5))
        .expect("the take still waits after 5 s")
}

#[test]
fn a_parked_classic_worker_the_vm_wakes_waits_on_and_takes_the_next_request() {
    let server = TestServer::lanes().workers(1).build();
    let slot = slot(&server, 0);
    // Thread#wakeup into the park, which raises the park's own flag; then
    // a request.
    let (mut wait, woken_slot) = (0, slot.clone());
    let woken = move |own: &AtomicBool, wait_on: &mut dyn FnMut()| {
        wait += 1;
        match wait {
            1 => own.store(true, Ordering::SeqCst),
            _ => assign(&woken_slot),
        }
        wait_on();
    };

    let taken = take_in_time(&server, &slot, woken, || Ok(()));

    assert!(
        matches!(taken, Ok(Some(Item::Ctx(_)))),
        "the worker must take on, not leave as if the server shut down"
    );
}

/// A take whose first wait finds what `send` sent it, `meanwhile` running
/// once it found it, as the VM's unblock function fires, with a
/// Thread#raise pending if `raise`, else a Thread#wakeup; later waits
/// find what there is.
fn take_as_the_vm_unblocks(
    server: &Arc<ServerInner>,
    slot: &Arc<WorkerSlot>,
    send: impl FnOnce() + Send + 'static,
    meanwhile: impl FnOnce() + Send + 'static,
    raise: bool,
) -> Result<Taken, &'static str> {
    let mut first = Some((send, meanwhile));
    let release = move |woken: &AtomicBool, wait: &mut dyn FnMut()| {
        let Some((send, meanwhile)) = first.take() else {
            return wait();
        };
        send();
        wait();
        meanwhile();
        woken.store(true, Ordering::SeqCst);
    };
    let interrupts = move || if raise { Err("Thread#raise") } else { Ok(()) };
    take_in_time(server, slot, release, interrupts)
}

#[test]
fn what_a_park_finds_as_the_vm_unblocks_it_goes_back_for_the_next_take_unless_it_must_not() {
    type Send = fn(&ServerInner, &WorkerSlot);
    let request: Send = |server, slot| match server.config.lanes {
        true => assign(slot),
        false => enqueue(server, crate::request::test_ctx()),
    };
    // Where it was sent (a lane, or the shared queue), the VM's pending
    // interrupt, what the take returns, and what the next take takes.
    // One row per case; rustfmt would split each row field by field.
    #[rustfmt::skip]
    let rows: [(bool, Send, bool, &str, &str); 6] = [
        (true, request, true, "raised", "request"),
        (true, request, false, "request", "nothing"),
        (true, |_, slot| assert!(slot.wake()), true, "raised", "wake"),
        // A socket's events must stay in order: handed over, the raise
        // left pending for Ruby, which fires it before the callback runs.
        (true, |_, slot| { socket_sends(slot, 0); socket_sends(slot, 1); slot.notify() }, true, "event 0", "event 1"),
        (false, request, true, "raised", "request"),
        (false, request, false, "request", "nothing"),
    ];
    for (lanes, send, raise, took, next) in rows {
        let server = match lanes {
            true => TestServer::lanes(),
            false => TestServer::shared_queue(),
        };
        let server = server.workers(1).build();
        let slot = slot(&server, 0);

        let sent = (server.clone(), slot.clone());
        let send = move || send(&sent.0, &sent.1);
        let taken = take_as_the_vm_unblocks(&server, &slot, send, || {}, raise);

        let case = format!("lanes {lanes}, raise {raise}, took {took}");
        let named = |taken: Taken| match taken {
            Some(Item::Ws(event)) => format!("event {}", number(&event)),
            Some(item) => kind(&Take::Item(item)).to_string(),
            None => "the end".to_string(),
        };
        assert_eq!(taken.map_or("raised".to_string(), named), took, "{case}");
        assert!(
            !slot.park.parked.load(Ordering::SeqCst),
            "{case}: still parked"
        );
        let again = match lanes {
            true => match Taker::new(&server, &slot).ready() {
                Take::Item(item) => named(Some(item)),
                other => kind(&other).to_string(),
            },
            false => server
                .req_rx
                .try_recv()
                .map_or("nothing", |_| "request")
                .to_string(),
        };
        assert_eq!(
            again, next,
            "{case}: taken once, else back for the next take"
        );
    }
}

/// The dispatcher's part on a shared-queue server: `ctx` joins the queue.
fn enqueue(server: &ServerInner, ctx: BoxedCtx) {
    let queue = server.req_tx.lock().clone().expect("queue open");
    assert!(queue.try_send(ctx).is_ok(), "queue has room");
}

#[test]
fn a_request_nothing_takes_back_is_answered_at_once_when_a_raise_comes_else_taken() {
    for raise in [true, false] {
        let server = TestServer::shared_queue().workers(1).build();
        let slot = slot(&server, 0);
        let (ctx, mut client) = crate::request::test_ctx_with_client();

        // The drain closes the queue as the take finds the request.
        let (sending, closing) = (server.clone(), server.clone());
        let send = move || enqueue(&sending, ctx);
        let close = move || drop(closing.req_tx.lock().take());
        let taken = take_as_the_vm_unblocks(&server, &slot, send, close, raise);

        if raise {
            assert!(matches!(taken, Err("Thread#raise")));
            let answer = client
                .try_recv()
                .expect("the client must get its answer at once, not when Ruby drops the request");
            assert_eq!(answer.status(), 500);
        } else {
            assert!(matches!(taken, Ok(Some(Item::Ctx(_)))));
            assert!(client.try_recv().is_err(), "the worker answers it");
        }
    }
}

#[test]
fn the_shutdowns_interrupt_ends_a_parked_take_whatever_the_vm_has_pending() {
    let server = TestServer::lanes().workers(1).build();
    let slot = slot(&server, 0);
    let thread_raise = || Err("Thread#raise");
    let halting = slot.clone();
    let halted = move |_: &AtomicBool, wait: &mut dyn FnMut()| {
        halting.halt();
        wait();
    };
    assert!(
        matches!(take_in_time(&server, &slot, halted, thread_raise), Ok(None)),
        "past the shutdown deadline the take ends as a shutdown"
    );

    // A later wait on the slot ends at once too, as a body read the app
    // tries again must: the halt left the flag up.
    let taken = take_in_time(&server, &slot, |_, wait| wait(), thread_raise);
    assert!(
        matches!(taken, Ok(None)),
        "a wait after the shutdown's interrupt must end as a shutdown"
    );
}

/// What a worker's checkout took: the request, admitted as take_one admits it (the env aside),
/// its handle kept as Ruby would keep it.
fn checked_out(server: &ServerInner, worker: &Worker, slot: &Arc<WorkerSlot>) -> BoxedCtx {
    let taken = worker.waiting(server).expect("take");
    let mut ctx = super::testing::expect_request(taken);
    super::worker::enlist(server, slot, &mut ctx);
    ctx
}

#[test]
fn a_worker_coming_back_for_more_answers_what_it_took_and_left_unanswered() {
    let server = TestServer::lanes().workers(1).build();
    let slot = slot(&server, 0);
    let worker = Worker::new(&server, 0, slot.clone());
    let (left, mut left_client) = crate::request::test_ctx_with_client();
    let (answered, mut answered_client) = crate::request::test_ctx_with_client();
    for ctx in [left, answered, crate::request::test_ctx()] {
        assert!(slot
            .send_lane(crate::registry::LaneItem::Request(ctx))
            .is_ok());
    }

    let left = checked_out(&server, &worker, &slot);
    let answered = checked_out(&server, &worker, &slot);
    assert_eq!(
        left_client
            .try_recv()
            .expect("an answer as the worker came back, its handle still alive")
            .status(),
        500
    );
    assert!(answered
        .responder
        .send_response(crate::response::plain_response(200, "ok")));
    let _last = checked_out(&server, &worker, &slot);

    assert_eq!(
        answered_client.try_recv().expect("its answer").status(),
        200,
        "an answered request is left alone"
    );
    drop((left, answered));
}
