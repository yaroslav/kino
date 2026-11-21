//! HTTP/1.1 and HTTP/2 through a served connection: a test plays the
//! client and the worker, taking requests from the shared queue.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::Frame;
use hyper::client::conn::http2::SendRequest;
use hyper_util::rt::{TokioExecutor, TokioIo};

use crate::queue::testing::{admit_next, rings};
use crate::registry::{BoxedCtx, ServerInner};
use crate::request::{RequestCtx, Step, WaitError};
use crate::response::{full_body, plain_response, Cut};
use crate::testing::{
    connect, eventually, get, h2_get, response_to, slot, take_ctx, until, Pending, TestServer,
};

/// A prior-knowledge HTTP/2 client connection to `server`.
async fn h2(server: &Arc<ServerInner>) -> SendRequest<Full<Bytes>> {
    crate::testing::h2_client(super::testing::spawn_conn(server.clone())).await
}

async fn body_of(response: hyper::Response<hyper::body::Incoming>) -> Bytes {
    response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes()
}

/// Read the request body the way a worker does: leftover first, then
/// the forwarder channel until the sender drops (EOF).
async fn drain_body(ctx: &mut RequestCtx) -> Vec<u8> {
    let mut out = ctx.leftover.take().map(Vec::from).unwrap_or_default();
    if let Some(rx) = &ctx.body_rx {
        while let Ok(chunk) = rx.recv_async().await {
            out.extend_from_slice(&chunk);
        }
    }
    out
}

#[tokio::test]
async fn h2c_prior_knowledge_reaches_the_queue_as_http2() {
    let server = TestServer::shared_queue().build();
    let mut sender = h2(&server).await;
    let response = tokio::spawn(async move { sender.send_request(h2_get("/")).await });

    let ctx = take_ctx(&server).await;
    assert_eq!(ctx.version, http::Version::HTTP_2);
    // The :authority pseudo-header arrives in the URI, where build_env
    // picks it up; no Host header exists on h2.
    assert_eq!(
        ctx.uri.authority().map(|a| a.as_str()),
        Some("kino.test:8443")
    );
    assert!(ctx.headers.get(http::header::HOST).is_none());
    assert!(ctx.responder.send_response(plain_response(200, "ok\n")));

    let response = response.await.expect("join").expect("h2 response");
    assert_eq!(
        response.headers()[http::header::SERVER],
        "Kino",
        "responses stay branded over h2"
    );
    assert_eq!(body_of(response).await, "ok\n");
}

#[tokio::test]
async fn h1_is_still_served_by_the_auto_builder() {
    let server = TestServer::shared_queue().build();
    let mut sender = connect(&server).await;
    let response = tokio::spawn(async move { sender.send_request(get("/")).await });

    let ctx = take_ctx(&server).await;
    assert_eq!(ctx.version, http::Version::HTTP_11);
    assert!(ctx.responder.send_response(plain_response(200, "h1\n")));
    let response = response.await.expect("join").expect("h1 response");
    assert_eq!(body_of(response).await, "h1\n");
}

#[tokio::test]
async fn h2_upload_forwards_data_and_drops_trailers() {
    let server = TestServer::shared_queue().build();
    let worker = tokio::spawn({
        let server = server.clone();
        async move {
            let mut ctx = take_ctx(&server).await;
            let body = drain_body(&mut ctx).await;
            assert!(
                ctx.responder.body_abandoned().is_none(),
                "a trailer frame must not abort the body read"
            );
            let length = Bytes::from(body.len().to_string());
            let response = hyper::Response::new(full_body(length));
            assert!(ctx.responder.send_response(response));
            body
        }
    });

    let mut trailers = http::HeaderMap::new();
    trailers.insert("x-checksum", "ignored".parse().expect("value"));
    let frames = [
        Frame::data(Bytes::from_static(b"hello ")),
        Frame::data(Bytes::from_static(b"world")),
        Frame::trailers(trailers),
    ];
    let (frames_tx, frames_rx) = flume::bounded::<Result<_, std::io::Error>>(4);
    for frame in frames {
        frames_tx.send(Ok(frame)).expect("frame");
    }
    drop(frames_tx); // EOS
    let (mut sender, conn) = hyper::client::conn::http2::handshake(
        TokioExecutor::new(),
        TokioIo::new(super::testing::spawn_conn(server.clone())),
    )
    .await
    .expect("h2 handshake");
    tokio::spawn(conn);
    let request = hyper::Request::post("http://kino.test/upload")
        .body(StreamBody::new(frames_rx.into_stream()))
        .expect("request");
    let response = sender.send_request(request).await.expect("h2 response");
    assert_eq!(body_of(response).await, "11");
    assert_eq!(worker.await.expect("worker"), b"hello world");
}

#[tokio::test]
async fn h2_streaming_response_arrives_chunked() {
    let server = TestServer::shared_queue().build();
    let worker = tokio::spawn({
        let server = server.clone();
        async move {
            let ctx = take_ctx(&server).await;
            let head = hyper::Response::builder().status(200);
            assert!(ctx.responder.send_stream_head(head).expect("head"));
            let frames = ctx.responder.body_sender().expect("open stream");
            for chunk in ["alpha ", "beta ", "gamma"] {
                let frame = Frame::data(Bytes::from_static(chunk.as_bytes()));
                frames.send_async(Ok(frame)).await.expect("chunk accepted");
            }
            ctx.responder.finish_stream();
        }
    });

    let response = h2(&server).await.send_request(h2_get("/stream")).await;
    assert_eq!(
        body_of(response.expect("h2 response")).await,
        "alpha beta gamma"
    );
    worker.await.expect("worker");
}

#[tokio::test]
async fn what_a_stream_sent_before_it_was_cut_reaches_the_client_ahead_of_the_cut() {
    // Its cuts count in the process-wide counts another test reads.
    let _serial = crate::testing::SERIAL.lock().await;
    // How many chunks the app's stream holds when it is cut: one, none
    // past the head, or as many as the channel takes, so the cut finds
    // no room behind them. All of it, the cut included, waits when
    // hyper first looks at the response.
    for h2_client in [false, true] {
        for (written, chunks) in [
            ("a chunk", 1),
            ("nothing", 0),
            ("a full channel", usize::MAX),
        ] {
            let what = format!("h2 {h2_client}, {written}");
            let server = TestServer::shared_queue().build();
            let pending = if h2_client {
                let mut sender = h2(&server).await;
                tokio::spawn(async move { sender.send_request(h2_get("/stream")).await })
            } else {
                crate::testing::send(&server, get("/stream")).await
            };
            let ctx = take_ctx(&server).await;
            let head = hyper::Response::builder().status(200);
            assert!(ctx.responder.send_stream_head(head).expect("head"));
            let stream = ctx.responder.body_sender().expect("an open stream");
            let mut sent = Vec::new();
            while sent.len() < chunks.saturating_mul(8) {
                let chunk = Frame::data(Bytes::from_static(b"streamed"));
                if stream.try_send(Ok(chunk)).is_err() {
                    break;
                }
                sent.extend_from_slice(b"streamed");
            }
            drop(stream);
            // Giving up on a full channel never waits for room. The thread
            // owns the context: a failed test unwinds without dropping it,
            // which would give up again and wait on the same stuck call.
            crate::testing::finishes_in_time(&format!("{what}: giving up"), move || {
                ctx.responder.give_up(Cut::Crash)
            });

            let mut body = crate::testing::response_to(pending).await.into_body();
            let mut got = Vec::new();
            let ended = loop {
                match body.frame().await {
                    Some(Ok(frame)) => got.extend_from_slice(&frame.into_data().expect("data")),
                    Some(Err(_)) => break "a cut",
                    None => break "a clean end",
                }
            };
            assert_eq!(got, sent, "{what}: what came before the end");
            assert_eq!(ended, "a cut", "{what}: then the cut, not a clean end");
        }
    }
}

#[tokio::test]
async fn hyper_drops_the_body_of_a_reset_h2_stream_so_the_next_send_fails() {
    // Its cut counts in the process-wide counts another test reads.
    let _serial = crate::testing::SERIAL.lock().await;
    let server = TestServer::shared_queue().build();
    let mut sender = h2(&server).await;
    let response = tokio::spawn(async move { sender.send_request(h2_get("/events")).await });

    let ctx = take_ctx(&server).await;
    let head = hyper::Response::builder().status(200);
    ctx.responder.send_stream_head(head).expect("head");
    let frames = ctx.responder.body_sender().expect("open stream");
    // Dropping the response body resets the stream.
    drop(response.await.expect("joined").expect("h2 response"));

    let event = || Ok(Frame::data(Bytes::from_static(b"data: x\n\n")));
    let gone = tokio::time::timeout(Duration::from_secs(5), async {
        while frames.send_async(event()).await.is_ok() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });
    assert!(
        gone.await.is_ok(),
        "writes kept succeeding after the client reset its stream"
    );
}

#[tokio::test]
async fn h2_multiplexes_concurrent_streams_into_the_queue() {
    let server = TestServer::shared_queue().build();
    let mut sender = h2(&server).await;
    let (one, two) = (
        sender.send_request(h2_get("/one")),
        sender.send_request(h2_get("/two")),
    );
    let client = tokio::spawn(async move { (one.await, two.await) });

    // Both streams are queued before either is answered, and answering
    // them in reverse order proves stream completion is not FIFO.
    let first = take_ctx(&server).await;
    let second = take_ctx(&server).await;
    assert_eq!([first.uri.path(), second.uri.path()], ["/one", "/two"]);
    assert!(second.responder.send_response(plain_response(200, "two\n")));
    assert!(first.responder.send_response(plain_response(200, "one\n")));

    let (one, two) = client.await.expect("join");
    assert_eq!(body_of(one.expect("first stream")).await, "one\n");
    assert_eq!(body_of(two.expect("second stream")).await, "two\n");
}

#[tokio::test]
async fn drain_finishes_the_in_flight_h2_stream_then_goaways() {
    let server = TestServer::shared_queue().build();
    let mut sender = h2(&server).await;
    let in_flight = sender.send_request(h2_get("/inflight"));

    // The request is with the worker when the drain fires; its response
    // must still reach the client through the shutdown.
    let ctx = take_ctx(&server).await;
    let _ = server.shutdown_tx.send(true);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(ctx.responder.send_response(plain_response(200, "late\n")));
    let response = in_flight.await.expect("in-flight served");
    assert_eq!(body_of(response).await, "late\n");

    assert!(
        sender.send_request(h2_get("/after")).await.is_err(),
        "a drained connection must not accept new streams"
    );
}

#[tokio::test]
async fn drain_closes_an_h1_connection_after_its_response() {
    let server = TestServer::shared_queue().build();
    let mut sender = connect(&server).await;
    // h1's SendRequest is not Clone: one task sends the in-flight
    // request, then, once it completed and so the drain was seen, the
    // keep-alive follow-up that must be refused.
    let client = tokio::spawn(async move {
        let first = sender.send_request(get("/inflight")).await;
        (first, sender.send_request(get("/after")).await.is_err())
    });

    let ctx = take_ctx(&server).await;
    let _ = server.shutdown_tx.send(true);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(ctx.responder.send_response(plain_response(200, "late\n")));

    let (first, refused) = client.await.expect("join");
    assert_eq!(first.expect("in-flight served").status(), 200);
    assert!(
        refused,
        "a drained keep-alive connection must close after its response"
    );
}

#[tokio::test]
async fn rapid_stream_resets_do_not_wedge_the_pipeline() {
    let server = TestServer::shared_queue()
        .with(|config| config.queue_depth = 64)
        .build();
    // A worker that answers everything it sees; answers to streams
    // already reset just vanish, as in production.
    let worker = tokio::spawn({
        let server = server.clone();
        async move {
            while let Ok(ctx) = server.req_rx.recv_async().await {
                let done = ctx.uri.path() == "/done";
                ctx.responder.send_response(plain_response(200, "ok\n"));
                if done {
                    break;
                }
            }
        }
    });

    // Fire and cancel: dropping the response future resets the stream.
    // The codec bounds reset churn; the server must keep serving after.
    let mut sender = h2(&server).await;
    for i in 0..40 {
        drop(sender.send_request(h2_get(&format!("/cancel/{i}"))));
    }
    let response = sender.send_request(h2_get("/done")).await;
    assert_eq!(body_of(response.expect("still served")).await, "ok\n");
    worker.await.expect("worker loop");
}

type Chunk = Result<hyper::body::Frame<bytes::Bytes>, std::io::Error>;
type Trickled = http_body_util::StreamBody<flume::r#async::RecvStream<'static, Chunk>>;

/// A POST through a real connection, as slot 0's worker_loop worker
/// admits it. Its body comes chunk by chunk through the returned
/// sender, and ends when that drops; the client goes away when its
/// connection task (returned too) is aborted.
async fn admitted(
    server: &Arc<ServerInner>,
) -> (
    flume::Sender<Chunk>,
    Pending,
    tokio::task::JoinHandle<()>,
    BoxedCtx,
) {
    let io = TokioIo::new(super::testing::spawn_conn(server.clone()));
    let (mut sender, conn) = hyper::client::conn::http1::handshake::<_, Trickled>(io)
        .await
        .expect("h1 handshake");
    let conn = tokio::spawn(async move {
        let _ = conn.await;
    });
    let (chunks, body) = flume::bounded(16);
    let request = hyper::Request::post("/upload")
        .header("host", "kino.test")
        .body(http_body_util::StreamBody::new(body.into_stream()))
        .expect("request");
    let pending = tokio::spawn(async move { sender.send_request(request).await });
    let slot = slot(server, 0);
    until("the request in the worker's lane", || {
        slot.lane.holds_items()
    })
    .await;
    let ctx = admit_next(server, &slot);
    (chunks, pending, conn, ctx)
}

fn chunk(text: &'static str) -> Chunk {
    Ok(hyper::body::Frame::data(bytes::Bytes::from_static(
        text.as_bytes(),
    )))
}

/// The descriptor a read that has to wait hands out.
fn waiting_read(ctx: &RequestCtx) -> std::os::fd::RawFd {
    match ctx.read_nonblock().expect("a read") {
        Step::Wait(fd) => fd,
        Step::Done(read) => panic!("expected to wait, read {read:?}"),
    }
}

/// Writes 16 KiB frames until one has to wait and the wait holds
/// while the connection runs, the client reading nothing: the
/// descriptor it waits on.
async fn stalled_write(ctx: &RequestCtx) -> std::os::fd::RawFd {
    for _ in 0..1000 {
        match ctx
            .write_nonblock(|_| bytes::Bytes::from(vec![b'x'; 16 * 1024]))
            .expect("an open stream")
        {
            Step::Done(()) => tokio::task::yield_now().await,
            Step::Wait(fd) => {
                tokio::time::sleep(Duration::from_millis(20)).await;
                if rings(fd) == 0 {
                    return fd;
                }
                ctx.waited();
            }
        }
    }
    panic!("the stream never filled");
}

#[tokio::test]
async fn reads_and_writes_that_never_wait_open_no_descriptor() {
    let server = TestServer::worker_loop().workers(1).build();
    let (chunks, pending, _conn, ctx) = admitted(&server).await;
    chunks.send(chunk("all of it")).expect("client body open");
    drop(chunks);
    let body_rx = ctx.body_rx.clone().expect("a body channel");
    eventually("the whole body forwarded", || {
        body_rx.is_disconnected().then_some(())
    })
    .await;

    assert!(matches!(ctx.read_nonblock(), Ok(Step::Done(Some(_)))));
    assert_eq!(ctx.read_nonblock().expect("a read"), Step::Done(None));
    assert!(ctx
        .responder
        .send_stream_head(hyper::Response::builder().status(200))
        .expect("head"));
    for _ in 0..4 {
        let written = ctx.write_nonblock(|_| bytes::Bytes::from_static(b"event\n"));
        assert_eq!(written.expect("an open stream"), Step::Done(()));
    }
    ctx.responder.finish_stream();
    let body = response_to(pending).await.into_body().collect();
    let body = tokio::time::timeout(Duration::from_secs(5), body)
        .await
        .expect("the stream ended within 5 s")
        .expect("body")
        .to_bytes();
    assert_eq!(&body[..], b"event\n".repeat(4).as_slice());

    assert!(ctx.responder.body_bell.sockets().is_none());
    assert!(ctx.responder.room_bell.sockets().is_none());
}

#[tokio::test]
async fn ending_a_stream_rings_a_write_still_waiting_on_it() {
    // Given up (a crash, an error mid-stream), or finished by another
    // fiber of the request.
    type End = fn(&RequestCtx);
    let ends: [(End, &str); 2] = [
        (|ctx| ctx.responder.give_up(Cut::Crash), "Gone"),
        (|ctx| ctx.responder.finish_stream(), "Closed"),
    ];
    for (end, fails) in ends {
        let server = TestServer::worker_loop().workers(1).build();
        let (chunks, pending, _conn, ctx) = admitted(&server).await;
        drop(chunks);
        assert!(ctx
            .responder
            .send_stream_head(hyper::Response::builder().status(200))
            .expect("head"));
        let _body = response_to(pending).await;

        let fd = stalled_write(&ctx).await;
        end(&ctx);
        assert_eq!(
            rings(fd),
            1,
            "the waiting write is rung, then fails as {fails}"
        );
        ctx.waited();
        let late = ctx.write_nonblock(|_| bytes::Bytes::from_static(b"late"));
        assert!(
            matches!(
                (fails, late),
                ("Gone", Err(WaitError::Gone)) | ("Closed", Err(WaitError::Closed))
            ),
            "{fails}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_body_that_stalls_past_its_timeout_rings_the_waiting_read() {
    let server = TestServer::worker_loop().workers(1).build();
    let (chunks, _pending, _conn, ctx) = admitted(&server).await;

    let fd = waiting_read(&ctx);
    tokio::time::sleep(super::handle::BODY_READ_TIMEOUT + Duration::from_secs(1)).await;
    assert_eq!(rings(fd), 1);
    ctx.waited();
    assert_eq!(ctx.read_nonblock().expect("a read"), Step::Done(None));
    assert_eq!(
        ctx.responder.body_abandoned(),
        Some(crate::response::BodyAbandon::TimedOut)
    );
    drop(chunks);
}

#[test]
fn a_body_cut_by_the_runtime_going_away_reads_as_interrupted() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("runtime");
    let server = TestServer::worker_loop().workers(1).build();
    let (chunks, ctx) = runtime.block_on(async {
        let (chunks, _pending, _conn, ctx) = admitted(&server).await;
        chunks.send(chunk("half")).expect("client body open");
        (chunks, ctx)
    });
    let body_rx = ctx.body_rx.clone().expect("a body channel");
    crate::testing::eventually_blocking("the chunk in the body channel", || {
        (!body_rx.is_empty()).then_some(())
    });
    assert!(matches!(ctx.read_nonblock(), Ok(Step::Done(Some(_)))));
    let fd = waiting_read(&ctx);

    runtime.shutdown_timeout(Duration::from_secs(1));
    assert_eq!(rings(fd), 1);
    ctx.waited();
    assert_eq!(ctx.read_nonblock().expect("a read"), Step::Done(None));
    assert_eq!(
        ctx.responder.body_abandoned(),
        Some(crate::response::BodyAbandon::Interrupted)
    );
    drop(chunks);
}

#[tokio::test]
async fn a_crashed_workers_requests_close_their_bells() {
    let server = TestServer::worker_loop().workers(1).build();
    let (_chunks, _pending, _conn, ctx) = admitted(&server).await;
    waiting_read(&ctx);

    slot(&server, 0).abort(crate::response::Cut::Crash);
    assert!(ctx.responder.body_bell.sockets().is_none());
}
