//! One request through hyper: body forwarding, the queue, the answer and the access log.

use std::ops::Deref;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use http_body_util::BodyExt;

use super::accept::Peer;
use crate::access_log::{Logged, Timing};
use crate::framing::content_length;
use crate::registry::ServerInner;
use crate::request::RequestCtx;
use crate::response::{plain_response, HyperResponse, Responder};

fn branded(mut response: HyperResponse) -> HyperResponse {
    response
        .headers_mut()
        .entry(http::header::SERVER)
        .or_insert(http::HeaderValue::from_static("Kino"));
    response
}

/// Idle deadline between body frames, so a silent client cannot pin a worker in read_body.
pub(crate) const BODY_READ_TIMEOUT: Duration = Duration::from_secs(30);

/// A request counted in Counters::in_flight while this lives. hyper's borrows its connection's
/// server, keeping the server's Arc count, which every worker take moves, off the I/O threads;
/// an inline request's holds an Arc.
/// A Drop guard: hyper drops this future at an await when the client aborts, and a
/// plain decrement after it would never run.
pub(crate) struct InFlight<S: Deref<Target = ServerInner> = Arc<ServerInner>> {
    server: S,
    /// A switching request's responder, which leaves the count when the app takes it over.
    switching: Option<Arc<Responder>>,
}

impl<S: Deref<Target = ServerInner>> InFlight<S> {
    pub(crate) fn enter(server: S, switching: Option<Arc<Responder>>) -> Self {
        server.counters.in_flight.fetch_add(1, Ordering::Relaxed);
        InFlight { server, switching }
    }
}

impl<S: Deref<Target = ServerInner>> Drop for InFlight<S> {
    fn drop(&mut self) {
        // A hijacked request left the count when the app took it over.
        if self.switching.as_ref().is_none_or(|r| r.leave_in_flight()) {
            self.server
                .counters
                .in_flight
                .fetch_sub(1, Ordering::Relaxed);
        }
    }
}

pub(super) async fn handle_request(
    server: &Arc<ServerInner>,
    peer: Peer,
    mut req: hyper::Request<hyper::body::Incoming>,
) -> Result<HyperResponse, std::convert::Infallible> {
    // A task holds a switching request's connection until the app claims it.
    let upgrade = crate::upgrade::capture(&mut req, server);
    let (parts, body) = req.into_parts();
    let head = parts.method == http::Method::HEAD;
    // Only when logging.
    let logged = server.access_log.as_ref().map(|log| {
        let logged = Logged::arrival(log, &parts.method, &parts.uri, peer.remote.ip());
        (log, logged)
    });

    // A declared oversize body gets a 413 before any worker runs; the forwarder caps
    // chunked or lying clients, and read_body raises.
    let max_body = server.config.max_body_size as u64;
    let oversize = max_body > 0 && content_length(&parts.headers).is_some_and(|len| len > max_body);

    let (head_tx, mut head_rx) = tokio::sync::oneshot::channel();
    let responder = Arc::new(Responder::new(head_tx));
    // A request that asks to switch can be hijacked: it keeps its responder for the request
    // timeout and the in-flight count.
    let switching = upgrade.is_some().then(|| responder.clone());
    // No channel is a bodyless EOF.
    let bodyless = oversize || hyper::body::Body::is_end_stream(&body);
    let body_rx = (!bodyless).then(|| forward_body(body, &responder, max_body));
    let ctx = RequestCtx::new(parts, peer, server, responder, body_rx, upgrade);

    let in_flight = InFlight::enter(&**server, switching);

    // One exit, so the access log sees every outcome, 503s included.
    let response = if oversize {
        plain_response(413, "Payload Too Large\n")
    } else if !crate::dispatch::enqueue(server, ctx).await {
        server.counters.rejected.fetch_add(1, Ordering::Relaxed);
        plain_response(503, "Service Unavailable\n")
    } else {
        await_head(server, in_flight.switching.as_deref(), &mut head_rx).await
    };
    if let Some((log, logged)) = logged {
        // A 503 or 504 never reached a worker and carries no timing.
        let timing = response.extensions().get::<Timing>().copied();
        let total = logged.elapsed();
        logged.completion(log, response.status().as_u16(), total, timing);
    }
    Ok(branded(crate::response::without_forbidden_body(
        response, head,
    )))
}

/// The request's body, forwarded by a task of its own through a bounded channel, so hyper is
/// polled only as fast as Ruby reads.
fn forward_body(
    mut body: hyper::body::Incoming,
    responder: &Arc<Responder>,
    max_body: u64,
) -> flume::Receiver<bytes::Bytes> {
    let (body_tx, body_rx) = flume::bounded::<bytes::Bytes>(8);
    let feed = BodyFeed {
        tx: Some(body_tx),
        responder: responder.clone(),
        ended: false,
    };
    tokio::spawn(async move {
        let mut total: u64 = 0;
        loop {
            // Only the client's silence trips this; a slow app blocks send_async, untimed.
            let frame = match tokio::time::timeout(BODY_READ_TIMEOUT, body.frame()).await {
                Ok(Some(Ok(frame))) => frame,
                Ok(Some(Err(_))) | Ok(None) => break, // body error or clean EOF
                Err(_) => {
                    feed.responder
                        .abandon_body(crate::response::BodyAbandon::TimedOut);
                    break;
                }
            };
            // Non-data frames (h2 trailers) are dropped: Rack has no trailer surface.
            if let Ok(data) = frame.into_data() {
                total += data.len() as u64;
                if max_body > 0 && total > max_body {
                    // Past the cap: dropping the sender unblocks read_body, which raises.
                    feed.responder
                        .abandon_body(crate::response::BodyAbandon::Oversize);
                    break;
                }
                if !feed.send(data).await {
                    break; // request handle dropped; stop pulling
                }
            }
        }
        feed.end();
    });
    body_rx
}

/// The head must arrive in time or the client gets a 504; the worker runs on, and its late
/// answer goes nowhere (the first claim won). A CPU-stuck handler keeps its slot.
async fn await_head(
    server: &ServerInner,
    switching: Option<&Responder>,
    head_rx: &mut tokio::sync::oneshot::Receiver<HyperResponse>,
) -> HyperResponse {
    let dropped = |_| plain_response(500, "Internal Server Error\n");
    if server.config.request_timeout_ms == 0 {
        return head_rx.await.unwrap_or_else(dropped);
    }
    let deadline = Duration::from_millis(server.config.request_timeout_ms);
    match tokio::time::timeout(deadline, &mut *head_rx).await {
        Ok(result) => result.unwrap_or_else(dropped),
        Err(_elapsed) => timed_out(server, switching, head_rx),
    }
}

/// The forwarder's end of a request body: each chunk, and the body's end however it comes,
/// rings a reader waiting on the request's body bell.
struct BodyFeed {
    tx: Option<flume::Sender<bytes::Bytes>>,
    responder: Arc<Responder>,
    /// Dropped before its loop ended, the forwarder went with the runtime: no end at all.
    ended: bool,
}

impl BodyFeed {
    /// False once the request's handle is gone.
    async fn send(&self, data: bytes::Bytes) -> bool {
        let Some(tx) = &self.tx else {
            return false;
        };
        let sent = tx.send_async(data).await.is_ok();
        self.responder.body_bell.ring();
        sent
    }

    fn end(mut self) {
        self.ended = true;
    }
}

impl Drop for BodyFeed {
    /// The channel closes before the ring, so the reader it wakes finds
    /// the body's end; a body cut by the shutdown reads as interrupted.
    fn drop(&mut self) {
        if !self.ended {
            self.responder
                .abandon_body(crate::response::BodyAbandon::Interrupted);
        }
        self.tx.take();
        self.responder.body_bell.ring();
    }
}

/// A 504. For a request that asked to switch (`switching`, its responder) it closes the
/// connection, and a held 101 is given up unless it went out as the deadline passed.
fn timed_out(
    server: &ServerInner,
    switching: Option<&Responder>,
    head_rx: &mut tokio::sync::oneshot::Receiver<HyperResponse>,
) -> HyperResponse {
    if let Some(responder) = switching {
        if !responder.cancel_switch() {
            if let Ok(response) = head_rx.try_recv() {
                return response;
            }
        }
    }
    server.counters.timeouts.fetch_add(1, Ordering::Relaxed);
    let mut response = plain_response(504, "Gateway Timeout\n");
    if switching.is_some() {
        crate::response::close_connection(&mut response);
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn branded_adds_the_server_header_unless_the_app_set_one() {
        let response = branded(plain_response(200, "x"));
        assert_eq!(response.headers()[http::header::SERVER], "Kino");

        let mut custom = plain_response(200, "x");
        let server = http::HeaderValue::from_static("custom");
        custom.headers_mut().insert(http::header::SERVER, server);
        assert_eq!(branded(custom).headers()[http::header::SERVER], "custom");
    }
}
