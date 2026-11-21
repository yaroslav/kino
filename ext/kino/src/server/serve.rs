//! Serving an accepted connection: the TLS handshake, the client's first bytes in time
//! (intake.rs), then hyper until it ends or drains.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_util::either::Either;

use super::accept::{Accepted, Conn, ConnIo, Peer};
use super::handle::handle_request;
use super::intake::{
    drain_signal, first_bytes, preface, Began, Intake, H2_PING_EVERY, H2_PING_TIMEOUT,
    HEADER_READ_TIMEOUT,
};
use crate::registry::ServerInner;

/// TLS is boxed: an upgrade boxes the stream, and a plain one must not carry a TLS session's room.
type Io = Either<Box<tokio_rustls::server::TlsStream<TcpStream>>, Conn>;

pub(crate) async fn serve_conn(
    accepted: Accepted,
    acceptor: Option<tokio_rustls::TlsAcceptor>,
    server: Arc<ServerInner>,
) {
    serve_conn_by(Intake::KINO, accepted, acceptor, server).await;
}

async fn serve_conn_by(
    intake: Intake,
    Accepted { conn, peer, held }: Accepted,
    acceptor: Option<tokio_rustls::TlsAcceptor>,
    server: Arc<ServerInner>,
) {
    let (stream, acceptor) = match (conn, acceptor) {
        (Either::Left(stream), Some(acceptor)) => (stream, acceptor),
        // TLS over a unix socket is refused at bind time.
        (conn, _) => {
            let accepted = Accepted { conn, peer, held };
            return serve(accepted, intake.first_byte, server).await;
        }
    };
    // Failed or stalled handshakes (scans, plain HTTP, slowloris) just drop the connection.
    // The timeout bounds a stall, and the drain closes one whose handshake has not begun.
    let accepted = tokio::time::Instant::now();
    let began = first_bytes(Either::Left(&stream), intake.handshake, &server);
    if began.await.is_none() {
        return;
    }
    let handshake = tokio::time::timeout_at(accepted + intake.handshake, acceptor.accept(stream));
    let Ok(Ok(mut tls)) = handshake.await else {
        return;
    };
    let due = accepted + intake.first_byte;
    let Some((protocol, prefix)) = tls_protocol(&mut tls, due, &server).await else {
        return;
    };
    let io = ConnIo::<Io>::new(Either::Left(Box::new(tls)), held, prefix);
    serve_connection(io, protocol, server, peer).await;
}

/// The protocol a TLS client chose by ALPN, with no sniff, and what was read of its start to
/// tell, for hyper to read again. Only HTTP/1 has a header timer, so h2's preface, or what tells
/// the two apart without ALPN, is read here; None, and it closes, when that is not in by `due`.
async fn tls_protocol(
    tls: &mut tokio_rustls::server::TlsStream<TcpStream>,
    due: tokio::time::Instant,
    server: &ServerInner,
) -> Option<(Protocol, Bytes)> {
    let left = || due.saturating_duration_since(tokio::time::Instant::now());
    let (tcp, session) = tls.get_ref();
    let alpn = session.alpn_protocol();
    if !server.config.http2 || alpn == Some(b"http/1.1") {
        // What came with the handshake's last flight is read already. Else the peek sees
        // records, whose first byte (a content type, 0x14 to 0x18) never starts the h2 preface.
        if session.wants_read() {
            first_bytes(Either::Left(tcp), left(), server).await?;
        }
        return Some((Protocol::Http1, Bytes::new()));
    }
    let alpn_h2 = alpn == Some(b"h2");
    let (began, prefix) = preface(tls, left(), server).await?;
    let protocol = match alpn_h2 || began == Began::H2 {
        true => Protocol::Http2,
        false => Protocol::Http1,
    };
    Some((protocol, prefix))
}

/// A plain connection served by hyper once its client began, by `within` (first_bytes).
async fn serve(accepted: Accepted, within: Duration, server: Arc<ServerInner>) {
    let plain = match &accepted.conn {
        Either::Left(stream) => Either::Left(stream),
        Either::Right(stream) => Either::Right(stream),
    };
    if first_bytes(plain, within, &server).await.is_none() {
        return;
    }
    let Accepted { conn, peer, held } = accepted;
    let io = ConnIo::<Io>::new(Either::Right(conn), held, Bytes::new());
    serve_connection(io, Protocol::of(&server), server, peer).await;
}

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

/// Kino's HTTP/2 settings, on either of hyper's h2 builders (they share no trait). No Date header:
/// a clock read per response for a SHOULD. A client silent for H2_PING_EVERY gets a PING, and one
/// that leaves it unanswered for H2_PING_TIMEOUT is closed, so a peer that is gone holds no
/// connection; one that answers keeps its connection, idle or not.
macro_rules! h2_settings {
    ($builder:expr, $max_streams:expr) => {
        $builder
            .timer(TokioTimer::new())
            .auto_date_header(false)
            .max_concurrent_streams($max_streams)
            .keep_alive_interval(H2_PING_EVERY)
            .keep_alive_timeout(H2_PING_TIMEOUT)
    };
}

/// Plaintext: h2 by preface sniff, else HTTP/1 (TLS: by ALPN, http1_builder and http2_builder).
/// The h1 timer is what makes header_read_timeout fire.
fn conn_builder(max_streams: u32) -> auto::Builder<TokioExecutor> {
    let mut builder = auto::Builder::new(TokioExecutor::new());
    builder
        .http1()
        .timer(TokioTimer::new())
        .header_read_timeout(HEADER_READ_TIMEOUT)
        .auto_date_header(false);
    h2_settings!(builder.http2(), max_streams);
    builder
}

/// How hyper serves a connection: HTTP/1, h2 (as a TLS client chose), or either by sniff.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Protocol {
    Http1,
    Http2,
    Auto,
}

impl Protocol {
    /// The sniff, or HTTP/1 alone where `server` has HTTP/2 off.
    fn of(server: &ServerInner) -> Protocol {
        if server.config.http2 {
            Protocol::Auto
        } else {
            Protocol::Http1
        }
    }
}

/// Serves `io` by `protocol` until it ends or drains.
async fn serve_connection<I>(
    io: ConnIo<I>,
    protocol: Protocol,
    server: Arc<ServerInner>,
    peer: Peer,
) where
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let max_streams = stream_capacity(&server.config.topology);
    let mut drain = server.shutdown_tx.subscribe();
    // A count of the connection's own, moved per request: the server's, which every worker take
    // moves, stays off the I/O threads.
    let server = Arc::new(server);
    let service = service_fn(move |req| {
        let server = server.clone();
        async move { handle_request(&server, peer, req).await }
    });
    // On the drain signal: finish the request, then close (GOAWAY on h2).
    // A macro: hyper's connection types share no trait.
    macro_rules! serve_until_drained {
        ($conn:expr) => {{
            let mut conn = std::pin::pin!($conn);
            // The connection first: one with a request to read is served before a drain that
            // began meanwhile closes it.
            tokio::select! {
                biased;
                _ = conn.as_mut() => {}
                _ = drain_signal(&mut drain) => {
                    conn.as_mut().graceful_shutdown();
                    let _ = conn.as_mut().await;
                }
            }
        }};
    }
    let io = TokioIo::new(io);
    // Upgrades need upgradeable connections, where http1_only is ignored: pin HTTP/1 directly.
    match protocol {
        Protocol::Http1 => {
            let conn = http1_builder().serve_connection(io, service);
            serve_until_drained!(conn.with_upgrades())
        }
        Protocol::Http2 => {
            serve_until_drained!(http2_builder(max_streams).serve_connection(io, service))
        }
        Protocol::Auto => {
            let builder = conn_builder(max_streams);
            serve_until_drained!(builder.serve_connection_with_upgrades(io, service))
        }
    }
}

/// For a TLS client that chose h2.
fn http2_builder(max_streams: u32) -> hyper::server::conn::http2::Builder<TokioExecutor> {
    let mut builder = hyper::server::conn::http2::Builder::new(TokioExecutor::new());
    h2_settings!(builder, max_streams);
    builder
}

fn http1_builder() -> hyper::server::conn::http1::Builder {
    let mut builder = hyper::server::conn::http1::Builder::new();
    builder
        .timer(TokioTimer::new())
        .header_read_timeout(HEADER_READ_TIMEOUT)
        .auto_date_header(false);
    builder
}

#[cfg(test)]
pub(crate) mod testing {
    use std::sync::Arc;

    use super::{serve_connection, ConnIo, Peer, Protocol};
    use crate::registry::ServerInner;
    use crate::server::accept::Held;

    /// `io` as hyper serves a connection, holding a permit of its own.
    pub(super) fn conn_io<I>(io: I) -> ConnIo<I> {
        let held = Held::new(crate::testing::permit(), None);
        ConnIo::new(io, held, bytes::Bytes::new())
    }

    /// Serve one connection the production way, over an in-memory pipe:
    /// the client's end.
    pub fn spawn_conn(server: Arc<ServerInner>) -> tokio::io::DuplexStream {
        let protocol = Protocol::of(&server);
        spawn_conn_by(server, protocol).0
    }

    /// The same by `protocol`, with the connection's task.
    pub(super) fn spawn_conn_by(
        server: Arc<ServerInner>,
        protocol: Protocol,
    ) -> (tokio::io::DuplexStream, tokio::task::JoinHandle<()>) {
        let (client, conn) = tokio::io::duplex(64 * 1024);
        let peer = Peer {
            remote: ([127, 0, 0, 1], 40000).into(),
            local: ([127, 0, 0, 1], 9292).into(),
        };
        let served = serve_connection(conn_io(conn), protocol, server, peer);
        (client, tokio::spawn(served))
    }
}

#[cfg(test)]
mod intake_tests;

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
