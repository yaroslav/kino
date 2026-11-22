//! The intake through serve_conn on every listener (TCP, unix, TLS with and without ALPN), one
//! table per rule: a client's first bytes in time (or a drain before them), and its end before
//! hyper has a request; then the PINGs that close an h2 peer that is gone.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_util::either::Either;

use super::super::accept::{Accepted, Held, Peer, UNIX_PEER};
use super::super::intake::H2_PREFACE;
use super::testing::spawn_conn_by;
use super::*;
use crate::registry::ServerInner;
use crate::response::plain_response;
use crate::testing::{h2_client, h2_get, on_own_runtime, response_to, take_ctx, TestServer};

/// The listener a test client connects through, and how far a TLS
/// client got: `Tls` has its handshake done, having offered h2 by
/// ALPN, `TlsH1` http/1.1, `TlsBare` nothing; `TlsSilent` sent
/// nothing.
#[derive(Clone, Copy, Debug)]
enum Bind {
    Tcp,
    Unix,
    Tls,
    TlsH1,
    TlsBare,
    TlsSilent,
}

trait ClientIo: AsyncRead + AsyncWrite + Unpin + Send + std::any::Any {}

impl<T: AsyncRead + AsyncWrite + Unpin + Send + std::any::Any> ClientIo for T {}

/// Intake deadlines short enough to wait out, and far enough apart to
/// tell which one closed a connection.
const SHORT: Intake = Intake {
    handshake: Duration::from_millis(200),
    first_byte: Duration::from_millis(1500),
};

/// SHORT, with a handshake that has as long as the first byte: for a
/// TLS client whose handshake's deadline is not what is under test.
const SHORT_FIRST_BYTE: Intake = Intake {
    handshake: SHORT.first_byte,
    ..SHORT
};

/// A client of `server` on a `bind` listener, its connection accepted
/// and served as an accept loop does, by `intake`, on a task of its
/// own.
async fn client_on(
    server: &Arc<ServerInner>,
    bind: Bind,
    intake: Intake,
) -> (Box<dyn ClientIo>, tokio::task::JoinHandle<()>) {
    static SOCKETS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let held = Held::new(crate::testing::permit(), None);
    if let Bind::Unix = bind {
        let n = SOCKETS.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("kino-{}-in{n}.sock", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let listener = tokio::net::UnixListener::bind(&path).expect("bind");
        let client = tokio::net::UnixStream::connect(&path)
            .await
            .expect("connect");
        let (conn, _) = listener.accept().await.expect("accept");
        let _ = std::fs::remove_file(&path);
        let accepted = Accepted {
            conn: Either::Right(conn),
            peer: UNIX_PEER,
            held,
        };
        let served = serve_conn_by(intake, accepted, None, server.clone());
        return (Box::new(client), tokio::spawn(served));
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let client = tokio::net::TcpStream::connect(listener.local_addr().expect("addr"))
        .await
        .expect("connect");
    let (conn, remote) = listener.accept().await.expect("accept");
    let peer = Peer {
        remote,
        local: conn.local_addr().expect("local addr"),
    };
    let acceptor = match bind {
        Bind::Tcp | Bind::Unix => None,
        _ => Some(crate::tls::testing::acceptor(server.config.http2)),
    };
    let accepted = Accepted {
        conn: Either::Left(conn),
        peer,
        held,
    };
    let served = tokio::spawn(serve_conn_by(intake, accepted, acceptor, server.clone()));
    let alpn: &[&[u8]] = match bind {
        Bind::Tls => &[b"h2"],
        Bind::TlsH1 => &[b"http/1.1"],
        _ => &[],
    };
    let client: Box<dyn ClientIo> = match bind {
        Bind::Tls | Bind::TlsH1 | Bind::TlsBare => {
            let tls = crate::tls::testing::connector(alpn)
                .connect(crate::tls::testing::localhost(), client)
                .await
                .expect("a TLS handshake");
            Box::new(tls)
        }
        _ => Box::new(client),
    };
    (client, served)
}

/// What `client` reads up to its connection's end (an error ends it
/// too: a TLS client's, at an end with no close_notify), within a
/// few seconds.
async fn read_to_end(client: &mut Box<dyn ClientIo>, what: impl std::fmt::Display) -> Vec<u8> {
    let mut read = Vec::new();
    let mut chunk = [0u8; 4096];
    let ended = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match client.read(&mut chunk).await {
                Ok(0) | Err(_) => return,
                Ok(n) => read.extend_from_slice(&chunk[..n]),
            }
        }
    });
    ended
        .await
        .unwrap_or_else(|_| panic!("{what}: still open 5 s on"));
    read
}

/// A request whose first byte is also the h2 preface's.
static IN_TIME: &[u8] =
    b"POST /in-time HTTP/1.1\r\nhost: x\r\ncontent-length: 0\r\nconnection: close\r\n\r\n";

/// A first-bytes row's end: served, closed at its deadline, or at once by a drain, with HTTP/2 off
/// too (where hyper's HTTP/1 alone would keep the connection to its header timeout).
#[derive(Clone, Copy, Debug)]
enum Then {
    Served,
    Closed,
    Drained { http2: bool },
}

/// Each row on a thread of its own, all at once, on a runtime of one
/// thread: the server looks at its connection only while the client
/// waits.
#[test]
fn a_client_is_served_if_it_begins_by_its_first_byte_deadline_and_closed_if_not_or_by_a_drain() {
    use Then::{Closed, Drained, Served};

    let preface = &H2_PREFACE[..];
    // What the client sends at once, then half the deadline on; what
    // comes of it.
    #[rustfmt::skip] // One row per line: a table of clients.
    let rows: [(Bind, &'static [u8], &'static [u8], Then); 14] = [
        (Bind::Tcp, IN_TIME, b"", Served),
        (Bind::Tcp, &IN_TIME[..1], &IN_TIME[1..], Served),
        (Bind::Unix, &IN_TIME[..1], &IN_TIME[1..], Served),
        (Bind::TlsH1, b"", IN_TIME, Served),
        (Bind::Tcp, b"", b"", Closed),
        (Bind::Unix, b"", b"", Closed),
        (Bind::Tls, b"", b"", Closed),
        (Bind::Tcp, &preface[..23], b"", Closed),
        (Bind::TlsBare, &preface[..1], b"", Closed),
        // No handshake: closed at the handshake's deadline.
        (Bind::TlsSilent, b"", b"", Closed),
        (Bind::Tcp, b"", b"", Drained { http2: true }),
        (Bind::Tcp, &preface[..1], b"", Drained { http2: true }),
        (Bind::TlsSilent, b"", b"", Drained { http2: true }),
        (Bind::Tcp, b"", b"", Drained { http2: false }),
    ];
    let rows: Vec<_> = rows
        .into_iter()
        .map(|(bind, now, later, then)| {
            std::thread::spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("a runtime")
                    .block_on(first_bytes_row(bind, now, later, then))
            })
        })
        .collect();
    for row in rows {
        if let Err(why) = row.join() {
            std::panic::resume_unwind(why);
        }
    }
}

async fn first_bytes_row(bind: Bind, now: &[u8], later: &[u8], then: Then) {
    let what = format!("{bind:?} {then:?}, {} then {}", now.len(), later.len());
    let drained = matches!(then, Then::Drained { .. });
    let handshake_only = !drained && matches!(bind, Bind::TlsSilent);
    let intake = if handshake_only {
        SHORT
    } else {
        SHORT_FIRST_BYTE
    };
    let http2 = !matches!(then, Then::Drained { http2: false });
    let server = TestServer::shared_queue().with(|c| c.http2 = http2).build();
    // As the accept loop's: the drain signal is sent to no one without it.
    let _accepting = server.shutdown_tx.subscribe();
    let accepted = std::time::Instant::now();
    let (mut client, serving) = client_on(&server, bind, intake).await;
    client.write_all(now).await.expect("a start");
    // In before the server looks: a TLS client's with the last flight
    // of its handshake.
    std::thread::sleep(Duration::from_millis(20));
    let mut signalled = None;
    if drained {
        // As the server waits for the client's first bytes.
        tokio::time::sleep(Duration::from_millis(100)).await;
        signalled = Some(std::time::Instant::now());
        let _ = server.shutdown_tx.send(true);
    } else if !later.is_empty() {
        tokio::time::sleep(SHORT.first_byte / 2).await;
        client.write_all(later).await.expect("the rest");
    }
    let served = matches!(then, Then::Served);
    if served {
        let ctx = tokio::time::timeout(Duration::from_secs(5), take_ctx(&server))
            .await
            .unwrap_or_else(|_| panic!("{what}: never queued"));
        assert_eq!(ctx.uri.path(), "/in-time");
        assert!(ctx
            .responder
            .send_response(plain_response(200, "in time\n")));
    }
    let read = String::from_utf8(read_to_end(&mut client, &what).await).expect("text");
    let closed = accepted.elapsed();
    if served {
        assert!(
            read.starts_with("HTTP/1.1 200 OK\r\n") && read.ends_with("\r\n\r\nin time\n"),
            "{what}: {read:?}"
        );
        return;
    }
    assert_eq!(read, "", "{what}: answered");
    if let Some(signalled) = signalled {
        // From the signal, however long the setup took: without the drain the connection
        // would stay until its first-byte deadline. The Closed rows show none closes early.
        let closed = signalled.elapsed();
        assert!(
            closed < Duration::from_secs(1),
            "{what}: closed {closed:?} after the drain began, not within a second"
        );
        serving.await.expect("serve_conn");
        return;
    }
    let (from, to) = if handshake_only {
        (SHORT.handshake, SHORT.first_byte)
    } else {
        (SHORT.first_byte, SHORT.first_byte + Duration::from_secs(2))
    };
    assert!(
        closed >= from && closed < to,
        "{what}: closed {closed:?} after its accept, not within {from:?}..{to:?}"
    );
    serving.await.expect("serve_conn");
}

/// Each row on a runtime of its own, as a spinning task never lets one
/// go.
#[test]
fn a_client_that_ends_its_side_before_a_request_is_whole_is_closed_at_once_unanswered() {
    let p = &H2_PREFACE[..1];
    let part: &[u8] = b"GET /hello HT";
    // The listener, whether h2 is on, what the client sends, and
    // whether it then resets rather than ends its side.
    #[rustfmt::skip] // One row per line: a table of clients.
    let rows: [(Bind, bool, &'static [u8], bool); 7] = [
        (Bind::Tcp, true, b"", false),
        (Bind::Tcp, true, p, false),
        (Bind::Tcp, true, p, true),
        (Bind::Tcp, false, part, false),
        (Bind::TlsSilent, true, p, false),
        (Bind::TlsSilent, true, p, true),
        (Bind::TlsBare, true, part, false),
    ];
    let mut open = Vec::new();
    for (bind, http2, sent, reset) in rows {
        let end = if reset { "a RST" } else { "a FIN" };
        let what = format!(
            "{bind:?}, http2 {http2}, \"{}\" then {end}",
            sent.escape_ascii()
        );
        let scenario = {
            let what = what.clone();
            async move {
                let server = TestServer::shared_queue().with(|c| c.http2 = http2).build();
                let (mut client, served) = client_on(&server, bind, Intake::KINO).await;
                client.write_all(sent).await.expect("a start");
                let ended = std::time::Instant::now();
                if reset {
                    // A TCP client, dropped with its linger off: it resets.
                    let tcp =
                        (client as Box<dyn std::any::Any>).downcast::<tokio::net::TcpStream>();
                    let tcp = tcp.expect("a TCP client");
                    let socket = socket2::SockRef::from(&*tcp);
                    socket.set_linger(Some(Duration::ZERO)).expect("linger off");
                } else {
                    client.shutdown().await.expect("a FIN");
                    let read = read_to_end(&mut client, &what).await;
                    // A raw client of a TLS listener may read an alert.
                    let raw_tls = matches!(bind, Bind::TlsSilent);
                    assert!(read.is_empty() || raw_tls, "{what}: answered {read:?}");
                    let closed = ended.elapsed();
                    assert!(
                        closed < Duration::from_secs(1),
                        "{what}: closed {closed:?} on"
                    );
                    drop(client);
                }
                tokio::time::timeout(Duration::from_secs(1), served)
                    .await
                    .unwrap_or_else(|_| panic!("{what}: still served a second on"))
                    .expect("serve_conn");
                assert!(server.req_rx.is_empty(), "{what}: the app saw a request");
            }
        };
        let ran = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            on_own_runtime(&what, Duration::from_secs(3), scenario)
        }));
        if ran.is_err() {
            open.push(what);
        }
    }
    assert!(open.is_empty(), "not closed at once: {open:?}");
}

/// The protocols hyper serves h2 by: the auto builder's, which sniffs a plaintext connection, and
/// the h2 builder a TLS client's ALPN chose.
const H2_PROTOCOLS: [Protocol; 2] = [Protocol::Auto, Protocol::Http2];

/// By when a client that never answers its PING is closed, after its last frame.
const REAPED: Duration = H2_PING_EVERY.saturating_add(H2_PING_TIMEOUT);

/// A timer's rounding, and the turns a close takes.
const SLACK: Duration = Duration::from_secs(1);

/// A client that is gone: it sent what opens an h2 connection (the preface and its SETTINGS),
/// and nothing since; nothing reads what the server sends.
async fn gone_h2(io: &mut tokio::io::DuplexStream) {
    let mut opening = H2_PREFACE.to_vec();
    opening.extend_from_slice(&[0, 0, 0, 0x4, 0, 0, 0, 0, 0]);
    io.write_all(&opening).await.expect("the opening");
}

/// When `served`, a connection's task, ended, after `opened`; it must by `by`.
async fn ended(
    served: tokio::task::JoinHandle<()>,
    opened: tokio::time::Instant,
    by: Duration,
    what: impl std::fmt::Display,
) -> Duration {
    tokio::time::timeout_at(opened + by, served)
        .await
        .unwrap_or_else(|_| panic!("{what}: still served {by:?} on"))
        .expect("serve_connection");
    opened.elapsed()
}

#[tokio::test(start_paused = true)]
async fn an_h2_peer_that_is_gone_is_closed_at_its_unanswered_ping_and_an_idle_one_that_answers_is_kept(
) {
    for protocol in H2_PROTOCOLS {
        let server = TestServer::shared_queue().build();
        let opened = tokio::time::Instant::now();
        let (mut gone, served) = spawn_conn_by(server.clone(), protocol);
        gone_h2(&mut gone).await;
        let at = ended(served, opened, 2 * REAPED, format!("{protocol:?}, gone")).await;
        assert!(
            at >= REAPED && at < REAPED + SLACK,
            "{protocol:?}: a gone peer was closed {at:?} on"
        );

        let (io, served) = spawn_conn_by(server.clone(), protocol);
        let mut idle = h2_client(io).await;
        tokio::time::sleep(5 * REAPED).await;
        assert!(
            !served.is_finished(),
            "{protocol:?}: an idle client that answers its PINGs was closed"
        );
        let kept = tokio::spawn(async move { idle.send_request(h2_get("/kept")).await });
        let ctx = take_ctx(&server).await;
        assert!(ctx.responder.send_response(plain_response(200, "kept\n")));
        assert_eq!(response_to(kept).await.status(), 200, "{protocol:?}");
    }
}

/// What a drain finds on an h2 connection.
#[derive(Clone, Copy, Debug)]
enum AtDrain {
    /// A stream whose answer comes long after any PING timed out.
    OpenStream,
    /// A client that is there, with no stream open.
    Idle,
    /// A client that is gone.
    Gone,
}

#[tokio::test(start_paused = true)]
async fn a_drain_waits_for_an_h2_stream_through_the_pings_and_they_still_close_a_gone_peer() {
    use http_body_util::BodyExt;

    let late = 3 * REAPED;
    for protocol in H2_PROTOCOLS {
        for at_drain in [AtDrain::OpenStream, AtDrain::Idle, AtDrain::Gone] {
            let what = format!("{protocol:?}, {at_drain:?}");
            let server = TestServer::shared_queue().build();
            let opened = tokio::time::Instant::now();
            let (mut io, served) = spawn_conn_by(server.clone(), protocol);
            // The drain begins a second on, once the connection is set up; as the accept loop's,
            // without which the signal is sent to no one.
            let _accepting = server.shutdown_tx.subscribe();
            let drain = || async {
                tokio::time::sleep(Duration::from_secs(1)).await;
                server.shutdown_tx.send(true).expect("the signal");
            };
            match at_drain {
                AtDrain::OpenStream => {
                    let mut client = h2_client(io).await;
                    let answer =
                        tokio::spawn(async move { client.send_request(h2_get("/late")).await });
                    let ctx = take_ctx(&server).await;
                    drain().await;
                    tokio::time::sleep(late).await;
                    assert!(ctx.responder.send_response(plain_response(200, "late\n")));
                    let answer = answer.await.expect("the client").expect(&what);
                    let body = answer.into_body().collect().await.expect(&what);
                    assert_eq!(&body.to_bytes()[..], b"late\n", "{what}");
                    let at = ended(served, opened, late + 2 * SLACK, &what).await;
                    assert!(at >= late, "{what}: closed {at:?} on, its stream open");
                }
                AtDrain::Idle => {
                    let _client = h2_client(io).await;
                    drain().await;
                    ended(served, opened, 2 * SLACK, &what).await;
                }
                AtDrain::Gone => {
                    gone_h2(&mut io).await;
                    drain().await;
                    let at = ended(served, opened, REAPED + SLACK, &what).await;
                    assert!(at >= REAPED, "{what}: closed {at:?} on");
                }
            }
        }
    }
}
