//! A connection's intake before hyper serves it: by when its client must have begun (Intake), and
//! how it began, told as hyper-util's sniff tells HTTP/1 from h2 (Sniff).

use std::io::ErrorKind;
use std::time::Duration;

use bytes::Bytes;
use socket2::{SockRef, Socket};
use tokio::io::{AsyncReadExt, Interest};
use tokio::net::{TcpStream, UnixStream};
use tokio_util::either::Either;

use crate::registry::ServerInner;

/// Slowloris guard for HTTP/1 heads; fixed, as tuning intake is the fronting proxy's job.
/// hyper times the head once it reads; first_bytes (or preface, over TLS) what comes before.
pub(super) const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(15);

/// hyper's HTTP/2 keep-alive: a client silent this long gets a PING, and is closed unless it
/// answers within H2_PING_TIMEOUT. It reaps a peer that is gone; an idle client that answers
/// keeps its connection (max_connections, or a fronting proxy, bounds those).
pub(super) const H2_PING_EVERY: Duration = Duration::from_secs(30);
pub(super) const H2_PING_TIMEOUT: Duration = Duration::from_secs(20);

/// What a client opening HTTP/2 without ALPN sends first (RFC 9113 3.4).
pub(super) const H2_PREFACE: &[u8; 24] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

/// By when a client must have begun, from its accept: its TLS handshake done and its first byte
/// sent (first_bytes).
#[derive(Clone, Copy)]
pub(super) struct Intake {
    pub(super) handshake: Duration,
    pub(super) first_byte: Duration,
}

impl Intake {
    pub(super) const KINO: Intake = Intake {
        // Slowloris guard for TLS: the header deadline starts only once hyper serves.
        handshake: Duration::from_secs(10),
        first_byte: HEADER_READ_TIMEOUT,
    };
}

/// How a client began, as hyper's sniff tells: the h2 preface, or anything else (its end too).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Began {
    H2,
    Other,
}

pub(super) type Plain<'a> = Either<&'a TcpStream, &'a UnixStream>;

fn socket<'a>(plain: &Plain<'a>) -> SockRef<'a> {
    match *plain {
        Either::Left(stream) => SockRef::from(stream),
        Either::Right(stream) => SockRef::from(stream),
    }
}

/// What a client sent first, up to what tells HTTP/1 from h2 (a byte off the preface, or all of
/// it); the sniff waits for it with no timer.
#[derive(Clone, Copy)]
struct Sniff {
    first: [u8; H2_PREFACE.len()],
    have: usize,
}

impl Sniff {
    fn new() -> Sniff {
        Sniff {
            first: [0; H2_PREFACE.len()],
            have: 0,
        }
    }

    fn began(&self) -> Option<Began> {
        let first = &self.first[..self.have];
        if first == H2_PREFACE {
            Some(Began::H2)
        } else if !H2_PREFACE.starts_with(first) {
            Some(Began::Other)
        } else {
            None
        }
    }

    fn recv(&mut self, socket: &Socket, flags: libc::c_int) -> std::io::Result<usize> {
        let n = recv_now(socket, &mut self.first[self.have..], flags)?;
        self.have += n;
        Ok(n)
    }

    /// How `socket`'s client began, by what it sent and what waits unread (a peek that waits for
    /// nothing); Other once its connection ended or failed, all of it hyper's to read.
    fn peek(mut self, socket: &Socket) -> Option<Began> {
        if let Some(began) = self.began() {
            return Some(began);
        }
        match self.recv(socket, libc::MSG_PEEK) {
            Ok(0) => Some(Began::Other),
            Ok(_) => self.began(),
            Err(e) => (e.kind() != ErrorKind::WouldBlock).then_some(Began::Other),
        }
    }
}

/// How a connection whose client sent too little for hyper's sniff began, once it
/// has: the sniff never times out a client that sends nothing or stops inside the h2 preface.
/// None, and it closes unanswered, when nothing more comes `within`, or once the drain began with
/// nothing there, as hyper closes an idle connection. Mostly the bytes are in by the accept: one
/// peek.
pub(super) async fn first_bytes(
    plain: Plain<'_>,
    within: Duration,
    server: &ServerInner,
) -> Option<Began> {
    let (sniff, sock) = (Sniff::new(), socket(&plain));
    if let Some(began) = sniff.peek(&sock) {
        return Some(began);
    }
    let mut drain = server.shutdown_tx.subscribe();
    let sent = async {
        // tokio assumes a just-accepted socket ready; the peek tells, and forgets a readiness
        // that was not. One whose client ended its side is hyper's to read: tokio never forgets
        // that readiness, so waiting on would spin.
        loop {
            let ready = match &plain {
                Either::Left(stream) => stream.ready(Interest::READABLE).await,
                Either::Right(stream) => stream.ready(Interest::READABLE).await,
            };
            match ready {
                Ok(ready) if ready.is_read_closed() => return Some(Began::Other),
                Ok(_) => {}
                Err(_) => return None,
            }
            let peek = || sniff.peek(&sock).ok_or(ErrorKind::WouldBlock.into());
            let looked = match &plain {
                Either::Left(stream) => stream.try_io(Interest::READABLE, peek),
                Either::Right(stream) => stream.try_io(Interest::READABLE, peek),
            };
            if let Ok(began) = looked {
                return Some(began);
            }
        }
    };
    tokio::select! {
        biased;
        sent = tokio::time::timeout(within, sent) => sent.ok().flatten(),
        () = drain_signal(&mut drain) => None,
    }
}

/// How a TLS client began, read off `tls` until that tells within `within`, and those bytes for
/// hyper to read again; None, and it closes, past that, at a drain with too little there, or at
/// its end.
pub(super) async fn preface(
    tls: &mut tokio_rustls::server::TlsStream<TcpStream>,
    within: Duration,
    server: &ServerInner,
) -> Option<(Began, Bytes)> {
    let mut sniff = Sniff::new();
    let mut drain = server.shutdown_tx.subscribe();
    let read = async {
        loop {
            if let Some(began) = sniff.began() {
                return Some(began);
            }
            match tls.read(&mut sniff.first[sniff.have..]).await {
                Ok(0) | Err(_) => return None,
                Ok(n) => sniff.have += n,
            }
        }
    };
    let began = tokio::select! {
        biased;
        began = tokio::time::timeout(within, read) => began.ok().flatten(),
        () = drain_signal(&mut drain) => None,
    };
    Some((began?, Bytes::copy_from_slice(&sniff.first[..sniff.have])))
}

/// recv(2) on `socket` into `buf` with `flags`, waiting for nothing; again if interrupted.
pub(crate) fn recv_now(
    socket: &Socket,
    buf: &mut [u8],
    flags: libc::c_int,
) -> std::io::Result<usize> {
    // SAFETY: recv(2) writes only initialised bytes, as socket2's own Read for &Socket relies on.
    let buf = unsafe { &mut *(buf as *mut [u8] as *mut [std::mem::MaybeUninit<u8>]) };
    loop {
        match socket.recv_with_flags(buf, flags | libc::MSG_DONTWAIT) {
            Err(e) if e.kind() == ErrorKind::Interrupted => {}
            got => return got,
        }
    }
}

/// The value first: a connection may subscribe just after the signal. A dropped sender counts.
pub(super) async fn drain_signal(rx: &mut tokio::sync::watch::Receiver<bool>) {
    if !*rx.borrow_and_update() {
        let _ = rx.changed().await;
    }
}
