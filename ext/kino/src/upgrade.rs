//! Who gets a connection that asks to switch protocols: hyper hands out one upgrade per request,
//! so a task holds it until the app claims it (ws::accept, hijack::take) or answers plainly.

use std::sync::Arc;

use hyper::upgrade::OnUpgrade;
use tokio::sync::oneshot;

use crate::hijack::Bridge;
use crate::registry::ServerInner;
use crate::ws;

/// Claiming it hands the connection over; dropping it (a plain answer) ends the holding task.
pub struct Upgrade {
    claim: oneshot::Sender<Claim>,
    websocket: Option<ws::Offer>,
}

impl Upgrade {
    pub fn websocket(&self) -> Option<&ws::Offer> {
        self.websocket.as_ref()
    }

    /// False when nothing holds it any more (the runtime is shutting down).
    pub(crate) fn claim(self, claim: Claim) -> bool {
        self.claim.send(claim).is_ok()
    }
}

pub(crate) enum Claim {
    WebSocket(ws::Accepted),
    Hijack(Bridge),
}

pub fn capture(
    req: &mut hyper::Request<hyper::body::Incoming>,
    server: &Arc<ServerInner>,
) -> Option<Upgrade> {
    if req.version() != http::Version::HTTP_11 || !req.headers().contains_key(http::header::UPGRADE)
    {
        return None;
    }
    let websocket = ws::offer(req, server);
    let (claim, claimed) = oneshot::channel();
    tokio::spawn(settle(hyper::upgrade::on(req), claimed, server.clone()));
    Some(Upgrade { claim, websocket })
}

/// A WebSocket handshake as `server` hands it to dispatch, with no
/// connection behind it.
#[cfg(test)]
pub fn test_handshake(server: &ServerInner) -> crate::registry::BoxedCtx {
    let request = ws::testing::handshake()
        .body(())
        .expect("handshake request");
    let (claim, _claimed) = oneshot::channel();
    let mut ctx = crate::request::test_ctx();
    ctx.headers = request.headers().clone();
    ctx.upgrade = Some(Upgrade {
        claim,
        websocket: ws::offer(&request, server),
    });
    assert!(ctx.offers_websocket(), "only a lanes server offers sockets");
    ctx
}

async fn settle(
    on_upgrade: OnUpgrade,
    claimed: oneshot::Receiver<Claim>,
    server: Arc<ServerInner>,
) {
    match claimed.await {
        Ok(Claim::WebSocket(accepted)) => ws::drive(on_upgrade, accepted).await,
        Ok(Claim::Hijack(bridge)) => bridge.run(on_upgrade, &server).await,
        Err(_) => {} // the app answered plainly
    }
}
