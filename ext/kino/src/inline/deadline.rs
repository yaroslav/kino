//! hyper's deadlines for inline connections: a next head not whole HEADER_READ_TIMEOUT after the
//! accept or the last answer, or a body stalled for BODY_READ_TIMEOUT, is cut off unanswered;
//! with request_timeout set, an answer not begun by then gets hyper's 504. Each connection keeps
//! one deadline on the inline clock, which the ticker moves on once a TICK.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::{InlineIo, Listed};
use crate::registry::ServerInner;
/// The ticker's look: the inline clock moves on (never back), and the connections due, found by
/// their deadlines alone, are cut off (InlineIo::expire) once the lists and the pool's lock are
/// let go, as that makes socket calls no pool resize or I/O thread's dispatch may wait on.
pub fn tick(server: &ServerInner) {
    let Some(inline) = &server.inline else {
        return;
    };
    let real = super::real_now(server);
    let now = inline.clock.0.fetch_max(real, Ordering::Relaxed).max(real);
    let mut due: smallvec::SmallVec<[Arc<InlineIo>; 4]> = smallvec::SmallVec::new();
    for slot in server.slots.read().iter() {
        if slot.inline.conns.load(Ordering::Relaxed) == 0 {
            continue;
        }
        let listed = slot.inline.ios.lock();
        let ios = listed.iter().filter(|listed| listed.deadline.due(now));
        due.extend(ios.filter_map(Listed::io));
    }
    for io in due {
        io.expire(now);
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use crate::inline::testing::{
        advance, answer, await_bytes, body_of, configured_server, cut_offs, elapse,
        inline_loop_server, look, loopback, narrowed, next_request, read_to_end, send_get, server,
        wedge, with_body, Event, Wire,
    };
    use crate::inline::{shut_unserved, tick, InlineConn, Next, Wrote};
    use crate::registry::ServerInner;
    use crate::request::RequestCtx;

    fn secs(seconds: f64) -> Duration {
        Duration::from_secs_f64(seconds)
    }

    /// What `conn`'s job finds as it looks again, with what its client
    /// sent by now in hand: still waiting on it (Readable), unless it was
    /// cut off.
    fn waiting(conn: &mut InlineConn) -> bool {
        match look(conn, true) {
            Next::Readable => true,
            Next::Closed => false,
            Next::Writable => panic!("an answer is still going out"),
            Next::Request(ctx) => panic!("{} ran", ctx.uri),
        }
    }

    /// `conn` was cut off: its job, looking again, finds it over, its
    /// client reads the end and nothing before it, and its worker counts it
    /// no more: it ended where it was, neither run nor handed back.
    fn cut_off(server: &ServerInner, client: &TcpStream, conn: &mut InlineConn) {
        assert!(!waiting(conn), "its job did not see the end");
        assert_eq!(read_to_end(client), b"", "answered before it ended");
        let slot = server.slots.read()[0].clone();
        assert_eq!(slot.inline.conns.load(Ordering::Relaxed), 0);
        let handed = crate::inline::testing::handbacks(server);
        assert_eq!(handed, 0, "a connection cut off went to hyper");
    }

    fn inline_requests(server: &ServerInner) -> u64 {
        crate::inline::testing::inline_stats(server).requests
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_half_sent_head_or_a_stalled_body_is_cut_off_at_its_timeout_and_a_steady_one_is_not()
    {
        // A head has the header timeout; once whole, its body has the
        // body timeout from its last byte.
        let half_head = b"GET / HTTP/1.1\r\nhost:".to_vec();
        for (what, sent, timeout) in [
            ("a half-sent head", half_head, 15.0),
            ("a stalled body", with_body("/up", 100, 10), 30.0),
        ] {
            let server = server();
            let (client, mut conn) = loopback(&server);
            (&client).write_all(&sent).unwrap();
            await_bytes(&conn);
            assert!(waiting(&mut conn));

            elapse(&server, secs(timeout - 0.1));
            assert!(waiting(&mut conn), "{what}: cut off before its timeout");
            elapse(&server, secs(0.2));
            cut_off(&server, &client, &mut conn);
            assert_eq!(inline_requests(&server), 0, "{what}: the app ran");
            assert_eq!(cut_offs(&server), 1, "{what}");
        }

        // A body a byte a second: never 30 s without one, 100 s in all.
        let server = server();
        let (client, mut conn) = loopback(&server);
        let mut head = with_body("/up", 100, 100);
        let body = head.split_off(head.len() - 100);
        (&client).write_all(&head).unwrap();
        await_bytes(&conn);
        for (sent, byte) in body.iter().enumerate() {
            assert!(waiting(&mut conn), "cut off at byte {sent}");
            elapse(&server, secs(1.0));
            (&client).write_all(&[*byte]).unwrap();
            await_bytes(&conn);
        }
        let ctx = next_request(&mut conn);
        assert!(body_of(&ctx) == body, "the body read is not the body sent");
        answer(&ctx, b"stored");
        assert_eq!(Wire::of(&client).response().2, b"stored");
    }

    /// The app's answer to `ctx`, streamed: a head of `status` and
    /// `headers`, then "done", then its end.
    fn streamed(ctx: &RequestCtx, status: u16, headers: &[(&'static str, &'static str)]) {
        let io = ctx.inline.clone().expect("an inline request");
        let mut map = http::HeaderMap::new();
        for (name, value) in headers {
            let name = http::HeaderName::from_static(name);
            map.insert(name, http::HeaderValue::from_static(value));
        }
        let status = http::StatusCode::from_u16(status).expect("a status");
        assert!(io.start_stream(&ctx.responder, status, &map));
        io.write_chunk(&ctx.responder, b"done");
        io.finish_stream(&ctx.responder);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_next_head_is_due_a_header_timeout_after_the_answer_however_long_the_request_ran() {
        type Answers = fn(&RequestCtx);
        // How the app answers, the answer's last bytes, and how long the
        // app ran: 10 s, or 20 s, past the header timeout from the accept.
        #[rustfmt::skip] // One row per line: a table of answers.
        let kinds: [(&str, Answers, &[u8], f64); 5] = [
            ("whole", |ctx| answer(ctx, b"done"), b"done", 10.0),
            ("chunked", |ctx| streamed(ctx, 200, &[]), b"0\r\n\r\n", 10.0),
            ("streamed with its length", |ctx| streamed(ctx, 200, &[("content-length", "4")]), b"done", 10.0),
            ("a head alone", |ctx| streamed(ctx, 204, &[]), b"\r\n\r\n", 10.0),
            ("whole, the app running past the deadline", |ctx| answer(ctx, b"done"), b"done", 20.0),
        ];
        for (kind, answers, end, app) in kinds {
            let server = server();
            let (client, mut conn) = loopback(&server);
            let accepted = crate::inline::now(&server);
            send_get(&client, "/", false);
            let ctx = next_request(&mut conn);
            elapse(&server, secs(app));
            answers(&ctx);
            drop(ctx);
            let mut read = Vec::new();
            while !read.ends_with(end) {
                let mut chunk = [0; 4096];
                let n = (&client).read(&mut chunk).expect("the answer");
                assert!(n > 0, "{kind}: the connection ended mid-answer: {read:?}");
                read.extend_from_slice(&chunk[..n]);
            }
            assert!(waiting(&mut conn), "{kind}: the connection ended");
            // A ticker's look that found it due by its accept's deadline,
            // 16 s on, takes its lock only after the answer moved it on.
            conn.io.expire(accepted + 16_000_000);
            assert!(
                waiting(&mut conn),
                "{kind}: cut off by the deadline its answer moved on"
            );

            elapse(&server, secs(14.9));
            assert!(
                waiting(&mut conn),
                "{kind}: cut off before the header timeout"
            );
            elapse(&server, secs(0.2));
            cut_off(&server, &client, &mut conn);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_request_cut_off_once_it_was_read_never_runs() {
        type Cut = fn(&ServerInner);
        // Between its job's read of a whole request and the request's
        // admission, the ticker finds it past its deadline, or shutdown
        // passes its own.
        let cuts: [(&str, Cut); 2] = [
            ("deadline", |server| tick(server)),
            ("shutdown", |server| shut_unserved(&server.slots.read()[0])),
        ];
        for (why, cut) in cuts {
            let server = server();
            let (client, mut conn) = loopback(&server);
            send_get(&client, "/cut", false);
            await_bytes(&conn);
            advance(&server, secs(16.0));
            let cutter = server.clone();
            conn.on_whole = Some(Box::new(move || cut(&cutter)));
            cut_off(&server, &client, &mut conn);
            assert_eq!(inline_requests(&server), 0, "{why}: the request ran");
            assert_eq!(cut_offs(&server), 1, "{why}");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_head_sent_in_time_waits_for_its_worker_unless_the_worker_was_given_up_on() {
        // Its worker held up past the deadline, a whole head its client
        // sent in time waits unread, or read with the request before it,
        // in its connection's buffer: each is served, its next head due a
        // header timeout after its answer.
        let pipelined = b"GET /1 HTTP/1.1\r\nhost: x\r\n\r\nGET /2 HTTP/1.1\r\nhost: x\r\n\r\n";
        for given_up in [false, true] {
            let server = inline_loop_server(1, 8);
            let slot = server.slots.read()[0].clone();
            let (unread_client, mut unread) = loopback(&server);
            send_get(&unread_client, "/2", false);
            await_bytes(&unread);
            let (buffered_client, mut buffered) = loopback(&server);
            (&buffered_client).write_all(pipelined).unwrap();
            answer(&next_request(&mut buffered), b"one");
            assert_eq!(Wire::of(&buffered_client).response().2, b"one");
            assert!(
                !buffered.readable(),
                "the second request is still in the socket"
            );
            // Given up on as wedged, its worker leaves them to their deadline.
            if given_up {
                wedge(&slot);
            }
            elapse(&server, secs(16.0));
            for (client, conn) in [
                (&unread_client, &mut unread),
                (&buffered_client, &mut buffered),
            ] {
                if given_up {
                    assert_eq!(read_to_end(client), b"", "answered before it ended");
                    continue;
                }
                let ctx = next_request(conn);
                assert_eq!(ctx.uri.path(), "/2");
                answer(&ctx, b"two");
                drop(ctx);
                assert_eq!(Wire::of(client).response().2, b"two");
            }
            if !given_up {
                elapse(&server, secs(15.1));
                assert_eq!(read_to_end(&unread_client), b"");
                assert_eq!(read_to_end(&buffered_client), b"");
                assert_eq!(cut_offs(&server), 2);
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn request_timeout_answers_504_only_to_a_request_whose_answer_has_not_begun() {
        // The app's answer, buffered or a stream's head, half a second
        // before the deadline or half a second past it, to a GET or a
        // HEAD with a request behind it.
        for (method, streams, late) in [
            ("GET", true, false),
            ("GET", false, true),
            ("GET", true, true),
            ("HEAD", false, true),
        ] {
            let what = format!("{method}, streamed {streams}, late {late}");
            let server = configured_server(|c| c.request_timeout_ms = 1000);
            let (client, mut conn) = loopback(&server);
            let sent = format!(
                "{method} /slow HTTP/1.1\r\nhost: x\r\n\r\nGET /next HTTP/1.1\r\nhost: x\r\n\r\n"
            );
            (&client).write_all(sent.as_bytes()).unwrap();
            let ctx = next_request(&mut conn);
            // The clock from its admission on.
            tick(&server);
            elapse(&server, secs(if late { 1.5 } else { 0.5 }));
            let io = ctx.inline.clone().expect("an inline request");
            let (ok, none) = (http::StatusCode::OK, http::HeaderMap::new());
            let began = if streams {
                io.start_stream(&ctx.responder, ok, &none)
            } else {
                io.respond(&ctx.responder, ok, &none, b"app")
            };
            assert_eq!(began, !late, "{what}: the app's answer went out");
            // A stream begun in time goes on as long as it takes.
            elapse(&server, secs(20.0));
            if began && streams {
                assert_eq!(io.write_chunk(&ctx.responder, b"app"), Wrote::Done);
                io.finish_stream(&ctx.responder);
            }
            let (status, headers, read) = match Wire::of(&client).answer(method == "HEAD") {
                Event::Answer {
                    status,
                    headers,
                    body,
                    ..
                } => (status, headers, body),
                other => panic!("{what}: {other:?}"),
            };
            let body: &[u8] = match (late, method) {
                (true, "HEAD") => b"",
                (true, _) => b"Gateway Timeout\n",
                (false, _) => b"3\r\napp\r\n0\r\n\r\n",
            };
            assert_eq!(read, body, "{what}");
            if late {
                assert_eq!(status, "HTTP/1.1 504 Gateway Timeout", "{what}");
                let close = headers.contains(&"connection: close".to_string());
                assert!(close, "{what}: {headers:?}");
                // Its 504 settled it, though its app still holds it, and
                // left the request behind it unrun.
                assert_eq!(server.counters.in_flight.load(Ordering::Relaxed), 0);
                assert!(matches!(conn.next(true), Next::Closed), "{what}");
                // A timeout, not a cut-off, whatever looks at it after.
                elapse(&server, secs(16.0));
                assert_eq!(cut_offs(&server), 0, "{what}");
            } else {
                assert_eq!(status, "HTTP/1.1 200 OK", "{what}");
            }
            drop(ctx);
            let timeouts = server.counters.timeouts.load(Ordering::Relaxed);
            assert_eq!(timeouts, u64::from(late), "{what}");
        }
    }

    /// Whether the worker writing an answer out stays, retired, or was
    /// given up on as wedged.
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Leaving {
        Staying,
        Retired,
        Quarantined,
    }

    /// Whether `conn`'s socket takes more now.
    fn takes_more(conn: &InlineConn) -> bool {
        let fd = conn.fd().expect("an open connection");
        crate::inline::io::poll_for(fd, libc::POLLOUT, 0).is_some()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_leaving_workers_answer_to_a_client_that_stops_reading_ends_a_header_timeout_on() {
        // A worker that stays keeps writing the answer out for as long as
        // its client may read it, as hyper does. One that retires, or is
        // given up on as wedged, keeps a client that reads on, slowly, and
        // cuts off one that took nothing more for a header timeout, so it
        // can leave.
        for leaving in [Leaving::Staying, Leaving::Retired, Leaving::Quarantined] {
            let server = inline_loop_server(2, 8);
            let slot = server.slots.read()[0].clone();
            let (mut client, mut conn) = narrowed(&server);
            send_get(&client, "/big", false);
            let ctx = next_request(&mut conn);
            answer(&ctx, &vec![b'x'; 1 << 20]);
            drop(ctx);
            assert!(matches!(conn.next(true), Next::Writable));
            match leaving {
                Leaving::Staying => {}
                Leaving::Retired => slot.retire(),
                Leaving::Quarantined => wedge(&slot),
            }
            for step in 0..3 {
                elapse(&server, secs(10.0));
                let left = conn.unsent();
                client
                    .read_exact(&mut [0; 4096])
                    .expect("more of the answer");
                crate::testing::eventually_blocking("the socket taking more", || {
                    let next = conn.next(true);
                    assert!(
                        matches!(next, Next::Writable),
                        "{leaving:?}: cut off at step {step}"
                    );
                    (conn.unsent() < left).then_some(())
                });
            }
            // Its client reads no more, and a header timeout passes over
            // a socket that takes no more: full after the ticker's look,
            // it was full at that look, as only a write fills it. While
            // its client's kernel still takes more, its job writes on, as
            // a job its scheduler wakes would: each such look sends some
            // of the answer, so the looks end.
            let cut = loop {
                assert!(
                    matches!(conn.next(true), Next::Writable),
                    "{leaving:?}: the answer ended"
                );
                elapse(&server, secs(16.0));
                if cut_offs(&server) > 0 {
                    break true;
                }
                if !takes_more(&conn) {
                    break false;
                }
            };
            if leaving == Leaving::Staying {
                assert!(!cut, "a worker that stays cut off its client");
                let next = conn.next(true);
                assert!(
                    matches!(next, Next::Writable),
                    "a worker that stays ended its answer"
                );
            } else {
                assert!(cut, "{leaving:?}: kept a client that took nothing more");
                let read = read_to_end(&client).len();
                assert!(
                    read < 1 << 20,
                    "{leaving:?}: the whole answer went out: {read} bytes"
                );
                assert!(matches!(conn.next(true), Next::Closed));
                assert_eq!(cut_offs(&server), 1, "{leaving:?}");
            }
        }
    }
}
