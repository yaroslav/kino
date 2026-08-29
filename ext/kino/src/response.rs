//! The response half of a request's lifecycle. A `Responder` is shared
//! between the Ruby-held `Request` handle and (via `Weak`) the worker slot,
//! so exactly one of three parties can answer the client: the app (normal
//! path), the supervisor (`abort_inflight` after a ractor crash), or the
//! `RequestCtx` Drop backstop. The `responded` flag makes the race benign.
//!
//! Two response shapes: `send_response` (complete, one shot) and
//! `send_stream_head` + a bounded frame channel (streaming bodies). The
//! channel being bounded(8) is the client-side backpressure: a slow client
//! makes `write_chunk` block in Ruby, with the GVL released.

use std::io;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use http_body_util::{combinators::BoxBody, BodyExt, Full, StreamBody};
use parking_lot::Mutex;

pub type BodyFrame = hyper::body::Frame<Bytes>;
pub type FrameResult = Result<BodyFrame, io::Error>;
pub type ResponseBody = BoxBody<Bytes, io::Error>;
pub type HyperResponse = hyper::Response<ResponseBody>;

const STREAM_BUFFER: usize = 8;

pub struct Responder {
    responded: AtomicBool,
    head_tx: Mutex<Option<tokio::sync::oneshot::Sender<HyperResponse>>>,
    body_tx: Mutex<Option<flume::Sender<FrameResult>>>,
}

impl Responder {
    pub fn new(head_tx: tokio::sync::oneshot::Sender<HyperResponse>) -> Self {
        Responder {
            responded: AtomicBool::new(false),
            head_tx: Mutex::new(Some(head_tx)),
            body_tx: Mutex::new(None),
        }
    }

    /// Claim the right to respond. First caller wins; everyone else gets None.
    fn claim(&self) -> Option<tokio::sync::oneshot::Sender<HyperResponse>> {
        if self.responded.swap(true, Ordering::SeqCst) {
            return None;
        }
        self.head_tx.lock().take()
    }

    /// Complete response in one shot.
    pub fn send_response(&self, response: HyperResponse) -> bool {
        match self.claim() {
            Some(tx) => tx.send(response).is_ok(),
            None => false,
        }
    }

    /// Start a streaming response from a prepared head: send it now, return
    /// chunks later through the frame channel. Returns false if someone
    /// already responded.
    pub fn send_stream_head(
        &self,
        builder: hyper::http::response::Builder,
    ) -> Result<bool, http::Error> {
        let Some(tx) = self.claim() else {
            return Ok(false);
        };
        let (body_tx, body_rx) = flume::bounded::<FrameResult>(STREAM_BUFFER);
        let response = builder.body(StreamBody::new(body_rx.into_stream()).boxed())?;
        *self.body_tx.lock() = Some(body_tx);
        let _ = tx.send(response);
        Ok(true)
    }

    /// Clone of the live frame sender, if a stream is open. The clone lets
    /// `write_chunk` block on a full channel without holding the lock.
    pub fn body_sender(&self) -> Option<flume::Sender<FrameResult>> {
        self.body_tx.lock().clone()
    }

    /// Clean end of stream: dropping the sender ends the hyper body.
    pub fn finish_stream(&self) {
        self.body_tx.lock().take();
    }

    /// The "the app will never answer" path. Before the head: canned 500.
    /// Mid-stream: error frame, which makes hyper abort the connection
    /// rather than fake a clean end. Never touches the Ruby API: callable
    /// from Drop, tokio threads, and the supervisor.
    pub fn respond_500_if_unsent(&self) {
        if let Some(tx) = self.claim() {
            let _ = tx.send(plain_response(500, "Internal Server Error\n"));
            return;
        }
        if let Some(body_tx) = self.body_tx.lock().take() {
            let _ = body_tx.send(Err(io::Error::other("Kino: response abandoned mid-stream")));
        }
    }
}

pub fn full_body(bytes: Bytes) -> ResponseBody {
    Full::new(bytes).map_err(|never| match never {}).boxed()
}

/// Canned plain-text response, built entirely on the Rust side (used for
/// the 500/503/504 paths that never reach Ruby).
pub fn plain_response(status: u16, message: &'static str) -> HyperResponse {
    hyper::Response::builder()
        .status(status)
        .header("content-type", "text/plain")
        .body(full_body(Bytes::from_static(message.as_bytes())))
        .expect("static response must build")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pair() -> (Responder, tokio::sync::oneshot::Receiver<HyperResponse>) {
        let (head_tx, head_rx) = tokio::sync::oneshot::channel();
        (Responder::new(head_tx), head_rx)
    }

    #[test]
    fn first_claimant_wins() {
        let (responder, mut head_rx) = pair();

        assert!(responder.send_response(plain_response(200, "first")));
        assert!(!responder.send_response(plain_response(201, "second")));
        assert_eq!(head_rx.try_recv().expect("head sent").status(), 200);
    }

    #[test]
    fn backstop_sends_500_when_unsent_and_is_idempotent() {
        let (responder, mut head_rx) = pair();

        responder.respond_500_if_unsent();
        responder.respond_500_if_unsent(); // second call must be a no-op

        assert_eq!(head_rx.try_recv().expect("head sent").status(), 500);
        assert!(!responder.send_response(plain_response(200, "late")));
    }

    #[test]
    fn stream_head_claims_and_opens_the_frame_channel() {
        let (responder, mut head_rx) = pair();

        let started = responder
            .send_stream_head(hyper::Response::builder().status(200))
            .expect("valid head");
        assert!(started);
        assert_eq!(head_rx.try_recv().expect("head sent").status(), 200);

        // A second stream start loses the claim.
        let again = responder
            .send_stream_head(hyper::Response::builder().status(201))
            .expect("valid head");
        assert!(!again);

        // The frame channel is open until finish_stream closes it.
        assert!(responder.body_sender().is_some());
        responder.finish_stream();
        assert!(responder.body_sender().is_none());
    }

    #[test]
    fn backstop_mid_stream_closes_the_frame_channel() {
        let (responder, _head_rx) = pair();

        responder
            .send_stream_head(hyper::Response::builder().status(200))
            .expect("valid head");
        assert!(responder.body_sender().is_some());

        // Mid-stream abandonment: the error frame goes to hyper (which
        // aborts the connection) and the channel is closed for the app.
        responder.respond_500_if_unsent();
        assert!(responder.body_sender().is_none());
    }

    #[test]
    fn concurrent_claimants_yield_exactly_one_winner() {
        let (responder, mut head_rx) = pair();
        let responder = std::sync::Arc::new(responder);

        let winners: usize = std::thread::scope(|scope| {
            (0..16)
                .map(|i| {
                    let responder = responder.clone();
                    scope.spawn(move || responder.send_response(plain_response(200 + i, "race")))
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(|handle| usize::from(handle.join().expect("no panic")))
                .sum()
        });

        assert_eq!(winners, 1);
        assert!(head_rx.try_recv().is_ok());
    }

    #[test]
    fn plain_response_sets_status_and_content_type() {
        let response = plain_response(503, "Service Unavailable\n");

        assert_eq!(response.status(), 503);
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "text/plain"
        );
    }
}
