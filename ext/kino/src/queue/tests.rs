use super::take::{Attempt, OWN_RUN};
use super::testing::{boot, kind, ready, socket_sends, tick};
use super::*;
use crate::slot::LANE_DEPTH;
use crate::testing::{assign, slot, TestServer};
use crate::ws;

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
    let taken = worker.checkout().expect("take");
    assert!(
        matches!(taken, Some((_, Item::Wake))),
        "the worker takes :wake"
    );
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
    let wait = |attempt: Attempt<'_>| {
        socket_sends(&worker, 0);
        worker.close_lane();
        Ok(attempt())
    };

    let taken = Taker::new(&server, &worker).take(wait).expect("take");
    assert!(
        matches!(&taken, Some(Item::Ws(event)) if number(event) == 0),
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
