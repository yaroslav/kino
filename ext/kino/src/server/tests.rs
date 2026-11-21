//! HTTP/1.1 and HTTP/2 through a served connection: a test plays the
//! client and the worker, taking requests from the shared queue.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::Frame;
use hyper::client::conn::http2::SendRequest;
use hyper_util::rt::{TokioExecutor, TokioIo};

use crate::registry::ServerInner;
use crate::request::RequestCtx;
use crate::response::{full_body, plain_response, Cut};
use crate::testing::{connect, get, h2_get, take_ctx, TestServer};

/// A prior-knowledge HTTP/2 client connection to `server`.
async fn h2(server: &Arc<ServerInner>) -> SendRequest<Full<Bytes>> {
    let io = TokioIo::new(super::testing::spawn_conn(server.clone()));
    let (sender, conn) = hyper::client::conn::http2::handshake(TokioExecutor::new(), io)
        .await
        .expect("h2 handshake");
    tokio::spawn(conn);
    sender
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
            assert!(ctx.responder.send_stream_head(head, false).expect("head"));
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
            assert!(ctx.responder.send_stream_head(head, false).expect("head"));
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
    ctx.responder.send_stream_head(head, false).expect("head");
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
