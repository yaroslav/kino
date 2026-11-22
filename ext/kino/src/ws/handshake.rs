//! Taking a handshake natively: the offer, the app's accept, registration and the held 101.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use http::{HeaderName, HeaderValue};
use tokio::sync::mpsc;

use super::codec::accept_key;
use super::socket::{Abort, Out};
use super::stats::{Cause, Refusal, WsCount, CLOSED, COUNTS, HANDSHAKES_REFUSED, OPEN};
use super::{unregister, Conn, Limits, CONNS, NEXT_ID};
use crate::framing;
use crate::registry::ServerInner;
use crate::request::RequestCtx;
use crate::slot::WorkerSlot;
use crate::upgrade::Claim;

/// What ws::accept needs, held with the request's upgrade until the app claims it.
#[derive(Clone)]
pub struct Offer {
    limits: Arc<Limits>,
    /// A draining server opens no sockets.
    draining: tokio::sync::watch::Receiver<bool>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Accept {
    Opened(u64),
    /// Draining, the worker's sockets closed, or `websocket_max` open: a 503 that closes.
    Refused,
    /// Not a handshake Kino can take (or answered, or hijacked): the app's response goes out.
    Declined,
}

/// What the connection task needs once the app accepted.
pub(crate) struct Accepted {
    pub(super) registration: Registration,
    pub(super) out_rx: mpsc::Receiver<Out>,
    /// For pongs and close echoes.
    pub(super) reply: mpsc::Sender<Out>,
    /// Resolves once the 101 went out; fails when answered otherwise (hold_for_switch).
    pub(super) switched: tokio::sync::oneshot::Receiver<()>,
}

/// A socket's entry in CONNS and its place in the open counts (server, worker, process):
/// dropping it gives up what is left, so every end, a runtime teardown included, forgets it.
pub(crate) struct Registration {
    pub(super) id: u64,
    pub(super) owner: Arc<WorkerSlot>,
    pub(super) limits: Arc<Limits>,
    pub(super) abort: Arc<Abort>,
    registered: AtomicBool,
    counted: AtomicBool,
    opened: bool,
}

impl Registration {
    pub(super) fn open(&mut self) {
        self.opened = true;
        OPEN.fetch_add(1, Ordering::Relaxed);
        COUNTS.add(WsCount::Opened);
    }

    /// Out of CONNS and its topics, and the open counts. An opened socket counts as closed under
    /// its cause; one with none went with its task: GoingAway once switched, else HandshakeFailed.
    pub(super) fn unregister(&self) {
        if self.registered.swap(false, Ordering::SeqCst) {
            unregister(self.id);
            self.limits.open.fetch_sub(1, Ordering::SeqCst);
            if self.opened {
                // Closed first: a snapshot that reads none open reads every close.
                let cause = self.abort.cause().unwrap_or_else(|| {
                    if self.abort.switched.load(Ordering::SeqCst) {
                        Cause::GoingAway
                    } else {
                        Cause::HandshakeFailed
                    }
                });
                CLOSED.add(cause);
                OPEN.fetch_sub(1, Ordering::Release);
            }
        }
    }

    /// Its close is in the worker's inbox, or no worker will take it: no on_close is owed.
    pub(super) fn uncount(&self) {
        if self.counted.swap(false, Ordering::SeqCst) {
            self.owner.sockets.open.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.unregister();
        self.uncount();
    }
}

/// RFC 6455 4.2.1: an HTTP/1.1 GET upgrading to version 13, with a key.
fn is_upgrade<B>(req: &hyper::Request<B>) -> bool {
    let has_token = |name, token: &[u8]| {
        let mut values = req.headers().get_all(name).iter();
        values.any(|v| v.to_str().is_ok() && framing::has_token(v.as_bytes(), token))
    };
    req.method() == http::Method::GET
        && req.version() == http::Version::HTTP_11
        && has_token(http::header::UPGRADE, b"websocket")
        && has_token(http::header::CONNECTION, b"upgrade")
        && req.headers().contains_key(http::header::SEC_WEBSOCKET_KEY)
        && req
            .headers()
            .get(http::header::SEC_WEBSOCKET_VERSION)
            .is_some_and(|v| v == "13")
}

/// Only on a lanes server: without lanes, a socket's events have no route to its worker.
pub(crate) fn offer<B>(req: &hyper::Request<B>, server: &ServerInner) -> Option<Offer> {
    (server.config.lanes && is_upgrade(req)).then(|| Offer {
        limits: server.config.ws.clone(),
        draining: server.shutdown_tx.subscribe(),
    })
}

/// Give the socket to the slot that took the request, and hold its 101 with the app's `extra`
/// headers for `switch`: on_open runs in between, so what it writes and subscribes is in place
/// when the client sees the socket. With `on_shutdown`, the drain's 1001 waits for it.
pub fn accept(
    ctx: &mut RequestCtx,
    extra: &[(HeaderName, HeaderValue)],
    on_shutdown: bool,
) -> Accept {
    open(ctx, extra, on_shutdown).unwrap_or(Accept::Declined)
}

fn open(
    ctx: &mut RequestCtx,
    extra: &[(HeaderName, HeaderValue)],
    on_shutdown: bool,
) -> Option<Accept> {
    let owner = ctx.slot.clone().filter(|slot| slot.lane.ws_tx.is_some())?;
    let key = ctx.headers.get(http::header::SEC_WEBSOCKET_KEY)?;
    let accept = HeaderValue::try_from(accept_key(key.as_bytes())).ok()?;
    let upgrade = ctx
        .upgrade
        .take_if(|upgrade| upgrade.websocket().is_some())?;
    let offer = upgrade.websocket()?.clone();

    let (switch_tx, switched) = tokio::sync::oneshot::channel();
    let Some((id, mut accepted)) = register(owner.clone(), &offer.limits, switched) else {
        // A drain or a closing worker would have refused it anyway.
        let why = refusal(&owner, &offer.draining).unwrap_or(Refusal::WebsocketMax);
        return Some(refuse(ctx, why));
    };
    // Checked once registered: a close that begins later finds the socket.
    if let Some(why) = refusal(&owner, &offer.draining) {
        drop(accepted);
        return Some(refuse(ctx, why));
    }
    accepted.registration.open();
    let abort = &accepted.registration.abort;
    abort.on_shutdown.store(on_shutdown, Ordering::SeqCst);
    // The connection task is gone (runtime shutdown); the claim dropped the registration.
    if !upgrade.claim(Claim::WebSocket(accepted)) {
        return None;
    }
    ctx.responder
        .hold_for_switch(switch_tx, switching_protocols(accept, extra));
    Some(Accept::Opened(id))
}

/// A 503 for `why` that closes the connection.
fn refuse(ctx: &mut RequestCtx, why: Refusal) -> Accept {
    let mut refusal = crate::response::plain_response(503, "Service Unavailable\n");
    crate::response::close_connection(&mut refusal);
    ctx.responder.send_response(refusal);
    HANDSHAKES_REFUSED.add(why);
    Accept::Refused
}

fn switching_protocols(
    accept: HeaderValue,
    extra: &[(HeaderName, HeaderValue)],
) -> crate::response::HyperResponse {
    let mut response = hyper::Response::new(crate::response::full_body(Bytes::new()));
    *response.status_mut() = http::StatusCode::SWITCHING_PROTOCOLS;
    let headers = response.headers_mut();
    for (name, value) in extra {
        headers.append(name.clone(), value.clone());
    }
    headers.insert(http::header::UPGRADE, HeaderValue::from_static("websocket"));
    headers.insert(
        http::header::CONNECTION,
        HeaderValue::from_static("Upgrade"),
    );
    headers.insert(http::header::SEC_WEBSOCKET_ACCEPT, accept);
    response
}

/// The server drains, or the slot's sockets are being closed.
fn refusal(owner: &WorkerSlot, draining: &tokio::sync::watch::Receiver<bool>) -> Option<Refusal> {
    if *draining.borrow() {
        return Some(Refusal::Draining);
    }
    (owner.sockets.close_code.load(Ordering::SeqCst) != 0).then_some(Refusal::WorkerClosing)
}

/// None when `websocket_max` sockets are open.
pub(super) fn register(
    owner: Arc<WorkerSlot>,
    limits: &Arc<Limits>,
    switched: tokio::sync::oneshot::Receiver<()>,
) -> Option<(u64, Accepted)> {
    if !limits.open_one() {
        return None;
    }
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let (tx, out_rx) = mpsc::channel(limits.queue);
    let abort = Arc::new(Abort {
        queue_bytes: limits.queue_bytes,
        inbound: limits.inbound,
        ..Abort::default()
    });
    owner.sockets.open.fetch_add(1, Ordering::SeqCst);
    CONNS.write().insert(
        id,
        Conn {
            tx: tx.clone(),
            abort: abort.clone(),
            owner: owner.clone(),
        },
    );
    let accepted = Accepted {
        registration: Registration {
            id,
            owner,
            limits: limits.clone(),
            abort,
            registered: AtomicBool::new(true),
            counted: AtomicBool::new(true),
            opened: false,
        },
        out_rx,
        reply: tx,
        switched,
    };
    Some((id, accepted))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_protocol_version_13_is_a_handshake() {
        let mut request = crate::ws::testing::handshake().body(()).expect("request");
        assert!(is_upgrade(&request));

        request.headers_mut().insert(
            http::header::SEC_WEBSOCKET_VERSION,
            http::HeaderValue::from_static("8"),
        );
        assert!(!is_upgrade(&request), "version 8 must not be upgraded");
    }
}
