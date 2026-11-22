//! Serving an accepted connection: the TLS handshake, then hyper, until it ends or drains.

use std::sync::Arc;
use std::time::Duration;

use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto;
use tokio_util::either::Either;

use super::accept::{Accepted, Conn, ConnIo, Peer};
use super::handle::handle_request;
use crate::registry::ServerInner;

/// Slowloris guard for TLS: the header deadline starts only once hyper serves.
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// TLS is boxed: an upgrade boxes the stream, and a plain one must not carry a TLS session's room.
type Io = Either<Box<tokio_rustls::server::TlsStream<tokio::net::TcpStream>>, Conn>;

pub(crate) async fn serve_conn(
    Accepted { conn, peer, held }: Accepted,
    acceptor: Option<tokio_rustls::TlsAcceptor>,
    server: Arc<ServerInner>,
) {
    let io: Io = match (conn, acceptor) {
        (Either::Left(stream), Some(acceptor)) => {
            // Failed or stalled handshakes (scans, plain HTTP, slowloris) just drop the connection.
            let handshake = tokio::time::timeout(TLS_HANDSHAKE_TIMEOUT, acceptor.accept(stream));
            let Ok(Ok(tls)) = handshake.await else { return };
            Either::Left(Box::new(tls))
        }
        // TLS over a unix socket is refused at bind time.
        (conn, _) => Either::Right(conn),
    };
    serve_connection(ConnIo { io, _held: held }, server, peer).await;
}

/// Slowloris guard for HTTP/1 heads; fixed, as tuning intake is the fronting proxy's job.
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(15);

/// SETTINGS_MAX_CONCURRENT_STREAMS from slot capacity, not hyper's flat 200 (zeros keep that).
fn advertised_streams(workers: usize, threads: usize) -> u32 {
    let slots = workers.saturating_mul(threads);
    if slots == 0 {
        return 200;
    }
    slots.clamp(8, 1024) as u32
}

fn stream_capacity(topology: &super::Topology) -> u32 {
    advertised_streams(topology.max_workers, topology.threads)
}

/// h2 by ALPN or preface sniff, else HTTP/1. No Date header: a clock read per response for a
/// SHOULD. The h1 timer is what makes header_read_timeout fire.
fn conn_builder(max_streams: u32) -> auto::Builder<TokioExecutor> {
    let mut builder = auto::Builder::new(TokioExecutor::new());
    builder
        .http1()
        .timer(TokioTimer::new())
        .header_read_timeout(HEADER_READ_TIMEOUT)
        .auto_date_header(false);
    builder
        .http2()
        .timer(TokioTimer::new())
        .auto_date_header(false)
        .max_concurrent_streams(max_streams);
    builder
}

async fn serve_connection<I>(io: I, server: Arc<ServerInner>, peer: Peer)
where
    I: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let http2 = server.config.http2;
    let max_streams = stream_capacity(&server.config.topology);
    let mut drain = server.shutdown_tx.subscribe();
    let service = service_fn(move |req| handle_request(server.clone(), peer, req));
    // On the drain signal: finish the request, then close (GOAWAY on h2).
    // A macro: hyper's two upgradeable connection types share no trait.
    macro_rules! serve_until_drained {
        ($conn:expr) => {{
            let mut conn = std::pin::pin!($conn);
            tokio::select! {
                _ = conn.as_mut() => {}
                _ = drain_signal(&mut drain) => {
                    conn.as_mut().graceful_shutdown();
                    let _ = conn.as_mut().await;
                }
            }
        }};
    }
    // Upgrades need upgradeable connections, where http1_only is ignored: pin HTTP/1 directly.
    if http2 {
        let builder = conn_builder(max_streams);
        serve_until_drained!(builder.serve_connection_with_upgrades(TokioIo::new(io), service));
    } else {
        serve_until_drained!(http1_builder()
            .serve_connection(TokioIo::new(io), service)
            .with_upgrades());
    }
}

fn http1_builder() -> hyper::server::conn::http1::Builder {
    let mut builder = hyper::server::conn::http1::Builder::new();
    builder
        .timer(TokioTimer::new())
        .header_read_timeout(HEADER_READ_TIMEOUT)
        .auto_date_header(false);
    builder
}

/// The value first: a connection may subscribe just after the signal. A dropped sender counts.
async fn drain_signal(rx: &mut tokio::sync::watch::Receiver<bool>) {
    if !*rx.borrow_and_update() {
        let _ = rx.changed().await;
    }
}

#[cfg(test)]
pub(crate) mod testing {
    use std::sync::Arc;

    use super::{serve_connection, Peer};
    use crate::registry::ServerInner;

    /// Serve one connection the production way, over an in-memory pipe:
    /// the client's end.
    pub fn spawn_conn(server: Arc<ServerInner>) -> tokio::io::DuplexStream {
        let (client, conn) = tokio::io::duplex(64 * 1024);
        let peer = Peer {
            remote: ([127, 0, 0, 1], 40000).into(),
            local: ([127, 0, 0, 1], 9292).into(),
        };
        tokio::spawn(serve_connection(conn, server, peer));
        client
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use tokio::io::AsyncWriteExt;

    use super::*;
    use crate::response::{plain_response, HyperResponse};
    use crate::testing::h2_get;

    async fn ok(_: hyper::Request<hyper::body::Incoming>) -> Result<HyperResponse, Infallible> {
        Ok(plain_response(200, "ok\n"))
    }

    #[test]
    fn a_connection_advertises_the_slots_of_its_pools_ceiling_floored_and_capped() {
        assert_eq!(advertised_streams(8, 3), 24);
        assert_eq!(advertised_streams(2, 1), 8, "floor");
        assert_eq!(advertised_streams(64, 32), 1024, "cap");
        assert_eq!(
            advertised_streams(8, 0),
            200,
            "an embedder's unknown topology"
        );
        // An elastic pool grows to meet what an h2 balancer sends.
        let elastic = super::super::Topology {
            workers: 2,
            max_workers: 8,
            threads: 3,
            ..Default::default()
        };
        assert_eq!(stream_capacity(&elastic), 24, "the ceiling, not the floor");
    }

    #[tokio::test]
    async fn http2_off_refuses_the_preface_but_serves_h1() {
        // What a server with http2 off serves every connection with.
        let pinned = || {
            let (client, conn) = tokio::io::duplex(64 * 1024);
            let served = http1_builder().serve_connection(TokioIo::new(conn), service_fn(ok));
            tokio::spawn(served.with_upgrades());
            TokioIo::new(client)
        };
        // The pinned h1 codec reads the preface as a malformed request line.
        let h2 = async {
            let (mut sender, conn) =
                hyper::client::conn::http2::handshake(TokioExecutor::new(), pinned()).await?;
            tokio::spawn(conn);
            sender.send_request(h2_get("/")).await
        };
        assert!(h2.await.is_err(), "h2 must not be served when pinned to h1");

        let (mut sender, conn) = hyper::client::conn::http1::handshake(pinned())
            .await
            .expect("h1 handshake");
        tokio::spawn(conn);
        let response = sender.send_request(crate::testing::get("/")).await;
        assert_eq!(response.expect("h1 response").status(), 200);
    }

    #[tokio::test]
    async fn streams_beyond_the_advertised_cap_queue_instead_of_failing() {
        // Six requests at a cap of 2: the h2 client holds the excess until
        // the server frees a stream, and nothing is refused or reset.
        let (client, conn) = tokio::io::duplex(64 * 1024);
        tokio::spawn(async move {
            let _ = conn_builder(2)
                .serve_connection(TokioIo::new(conn), service_fn(ok))
                .await;
        });
        let (mut sender, conn) =
            hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(client))
                .await
                .expect("h2 handshake");
        tokio::spawn(conn);
        let requests: Vec<_> = (0..6)
            .map(|i| sender.send_request(h2_get(&format!("/{i}"))))
            .collect();
        for request in requests {
            assert_eq!(request.await.expect("queued stream served").status(), 200);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn header_read_timeout_still_fires_through_the_auto_builder() {
        let (mut client, conn) = tokio::io::duplex(64 * 1024);
        let served = tokio::spawn(async move {
            let _ = conn_builder(200)
                .serve_connection(TokioIo::new(conn), service_fn(ok))
                .await;
        });
        // A partial request line, then silence: the slow-header guard reaps
        // the connection (the paused clock auto-advances past 15 s).
        client.write_all(b"GET / HT").await.expect("partial write");
        tokio::time::timeout(Duration::from_secs(60), served)
            .await
            .expect("connection reaped by header_read_timeout")
            .expect("serve task join");
    }

    #[test]
    fn an_upgraded_plain_connection_carries_no_room_for_a_tls_session() {
        // hyper boxes the stream it hands over on an upgrade.
        let served = size_of::<ConnIo<Io>>();
        let plain = size_of::<ConnIo<tokio::net::TcpStream>>();
        assert!(
            served <= plain + 16,
            "{served} bytes, {plain} for plain TCP"
        );
    }
}
