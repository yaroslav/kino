//! A socket's connection task: it reads the client's messages into the
//! worker's inbox as credit allows, writes what is queued, pings a silent
//! client, and says the drain's goodbye.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::sync::mpsc;
use tungstenite::Message;

use super::codec::{close_frame, encode, Opcode, Reader};
use super::handshake::Accepted;
use super::socket::{Abort, Out};
use super::stats::{Cause, WsCount, COUNTS};
use super::{handled, shutdown_ran, WsEvent, CLOSE_TIMEOUT, GOING_AWAY};
use crate::slot::WorkerSlot;

/// Hand `event` to the owner's worker, waiting while its inbox is full; whether a worker will
/// take it. An abandoned slot drops it once it lands, which frees room for the next reader.
async fn emit(owner: &WorkerSlot, event: WsEvent) -> bool {
    let Some(tx) = owner.lane.ws_tx.as_ref() else {
        return false;
    };
    let sent = tx.send_async(event).await.is_ok();
    landed(owner, sent)
}

/// emit, but never waiting: the event back when `owner`'s inbox is full,
/// else whether a worker will take it.
fn try_emit(owner: &WorkerSlot, event: WsEvent) -> Result<bool, WsEvent> {
    let Some(tx) = owner.lane.ws_tx.as_ref() else {
        return Ok(false);
    };
    match tx.try_send(event) {
        Err(flume::TrySendError::Full(event)) => Err(event),
        sent => Ok(landed(owner, sent.is_ok())),
    }
}

/// After an event went to `owner`'s inbox, or failed to: ring the worker, or on an abandoned
/// slot drop what landed. Whether a worker will take it.
fn landed(owner: &WorkerSlot, sent: bool) -> bool {
    if owner.sockets.abandoned() {
        drain_inbox(owner);
        return false;
    }
    if sent {
        owner.notify();
    }
    sent
}

/// Drop the events waiting in `slot`'s inbox: their sockets get the
/// credit back, and the drain does not wait for their on_shutdown.
pub(super) fn drain_inbox(slot: &WorkerSlot) {
    if let Some(rx) = slot.lane.ws_rx.as_ref() {
        while let Ok(event) = rx.try_recv() {
            match event {
                WsEvent::Message { id, .. } => handled(id),
                WsEvent::Shutdown { id } => shutdown_ran(id),
                WsEvent::Drained { .. } | WsEvent::Close { .. } => {}
            }
        }
    }
}

/// When a socket's client was last heard from: any bytes it sent, or a write that got through
/// after the connection made it wait (room only the client's reading frees). A reader waiting
/// on its worker reads nothing, but then the worker is behind, not the client. Also the last ping.
pub(super) struct Liveness {
    start: tokio::time::Instant,
    heard_us: AtomicU64,
    waiting: AtomicBool,
    /// When the last ping was written, plus one; 0: never.
    ping_written_us: AtomicU64,
}

impl Liveness {
    fn new() -> Self {
        Liveness {
            start: tokio::time::Instant::now(),
            heard_us: AtomicU64::new(0),
            waiting: AtomicBool::new(false),
            ping_written_us: AtomicU64::new(0),
        }
    }

    fn now_us(&self) -> u64 {
        self.start.elapsed().as_micros() as u64
    }

    fn heard(&self) {
        self.heard_us.store(self.now_us(), Ordering::SeqCst);
    }

    fn ping_written(&self) {
        self.ping_written_us
            .store(self.now_us() + 1, Ordering::SeqCst);
    }

    /// How long the socket has been open, when its client was last heard from (now, while the
    /// reader waits on its worker), and when the last ping was written.
    fn state(&self) -> (Duration, Duration, Option<Duration>) {
        let now = self.start.elapsed();
        let heard = if self.waiting.load(Ordering::SeqCst) {
            now
        } else {
            Duration::from_micros(self.heard_us.load(Ordering::SeqCst))
        };
        let ping = match self.ping_written_us.load(Ordering::SeqCst) {
            0 => None,
            written => Some(Duration::from_micros(written - 1)),
        };
        (now, heard, ping)
    }

    /// The reader waits on its worker for `wait`; the client's silence
    /// counts again from its end.
    pub(super) async fn on_worker<T>(&self, wait: impl Future<Output = T>) -> T {
        self.waiting.store(true, Ordering::SeqCst);
        let out = wait.await;
        self.heard();
        self.waiting.store(false, Ordering::SeqCst);
        out
    }
}

/// A socket's stream, telling its Liveness of a read that brings bytes, or a write that gets
/// through after the stream made it wait (the client read, freeing room).
struct Tap<S> {
    io: S,
    liveness: Arc<Liveness>,
    waited: bool,
}

impl<S> Tap<S> {
    fn read(&mut self, n: usize) {
        if n > 0 {
            self.liveness.heard();
            COUNTS.add_n(WsCount::ReceivedBytes, n as u64);
        }
    }

    /// A write or flush polled; `progress` says whether a done one moved
    /// anything.
    fn wrote<T>(
        &mut self,
        polled: Poll<io::Result<T>>,
        progress: impl Fn(&T) -> bool,
    ) -> Poll<io::Result<T>> {
        match &polled {
            Poll::Pending => self.waited = true,
            Poll::Ready(Ok(done)) if self.waited && progress(done) => {
                self.waited = false;
                self.liveness.heard();
            }
            Poll::Ready(_) => {}
        }
        polled
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for Tap<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let before = buf.filled().len();
        let polled = Pin::new(&mut this.io).poll_read(cx, buf);
        this.read(buf.filled().len() - before);
        polled
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for Tap<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let polled = Pin::new(&mut this.io).poll_write(cx, buf);
        this.wrote(polled, |written| *written > 0)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let polled = Pin::new(&mut this.io).poll_write_vectored(cx, bufs);
        this.wrote(polled, |written| *written > 0)
    }

    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let polled = Pin::new(&mut this.io).poll_flush(cx);
        this.wrote(polled, |_| true)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_shutdown(cx)
    }
}

/// Pings a client silent for `interval`, once per silence, and resolves
/// once it has been silent for two, and for one since the ping was
/// written: a dead peer. Zero: never.
async fn keepalive(liveness: &Liveness, interval: Duration, abort: &Abort, out: mpsc::Sender<Out>) {
    if interval.is_zero() {
        return std::future::pending().await;
    }
    // When the client was last heard from as of the last ping.
    let mut pinged = None;
    loop {
        let (now, heard, ping_written) = liveness.state();
        let silence = now.saturating_sub(heard);
        if silence < interval {
            tokio::time::sleep(interval - silence).await;
            continue;
        }
        let mut deadline = heard + interval * 2;
        if let Some(written) = ping_written.filter(|written| *written >= heard) {
            deadline = deadline.max(written + interval);
        }
        if now >= deadline {
            return;
        }
        let mut check_in = deadline - now;
        // Nothing may follow a close frame; a full queue takes the ping a
        // little later.
        if pinged != Some(heard) && !abort.closing.load(Ordering::SeqCst) {
            if out.try_send(Out::Ping(encode(Opcode::Ping, b""))).is_ok() {
                pinged = Some(heard);
            } else {
                check_in = check_in.min(interval / 8);
            }
        }
        tokio::time::sleep(check_in).await;
    }
}

/// The drain's goodbye (leave_all): once told to leave, the worker runs its on_shutdown, if
/// any, and the 1001 follows that and the socket's turn in the spread. Resolves only when the
/// 1001 cannot queue (a full queue): the socket is dropped instead.
async fn going_away(id: u64, owner: &WorkerSlot, abort: &Abort, out: mpsc::Sender<Out>) {
    abort.leave.notified().await;
    let turn = Duration::from_micros(abort.leave_after_us.load(Ordering::SeqCst));
    let goodbye = async {
        if abort.on_shutdown.load(Ordering::SeqCst) && emit(owner, WsEvent::Shutdown { id }).await {
            abort.goodbye.notified().await;
        }
    };
    tokio::join!(tokio::time::sleep(turn), goodbye);
    if abort.close(&out, GOING_AWAY, Cause::GoingAway) {
        std::future::pending::<()>().await;
    }
}

/// The connection task once the app accepted (upgrade::settle): waits for
/// hyper to hand over the upgraded connection, then frames it.
pub(crate) async fn drive(on_upgrade: hyper::upgrade::OnUpgrade, accepted: Accepted) {
    let Accepted {
        registration,
        out_rx,
        reply,
        switched,
    } = accepted;
    let (id, owner) = (registration.id, &registration.owner);
    let (limits, abort) = (registration.limits.clone(), registration.abort.clone());
    let mut flushing = None;
    // Only a 101 hands the connection over: after any other answer hyper
    // keeps the upgrade pending until the client hangs up.
    let upgraded = match switched.await {
        Ok(()) => {
            abort.switched.store(true, Ordering::SeqCst);
            let upgraded = on_upgrade.await.ok();
            if upgraded.is_none() {
                abort.end_as(Cause::ClientGone);
            }
            upgraded
        }
        Err(_) => {
            drop(on_upgrade);
            // Whatever ended it first, the client never got the socket.
            abort
                .cause
                .store(Cause::HandshakeFailed as u8, Ordering::SeqCst);
            None
        }
    };
    if let Some(upgraded) = upgraded {
        let liveness = Arc::new(Liveness::new());
        let (rd, wr) = tokio::io::split(Tap {
            io: TokioIo::new(upgraded),
            liveness: liveness.clone(),
            waited: false,
        });
        let socket = Socket {
            id,
            owner: owner.clone(),
            abort: abort.clone(),
            liveness: liveness.clone(),
        };
        let writer = tokio::spawn(write_loop(wr, out_rx, socket));
        let messages = Reader::new(rd, limits.max_message);
        let (pinger, leaver) = (reply.clone(), reply.clone());
        // An abort wins even over a reader parked on a full inbox or out of credit; what the
        // client already sent (a pong) is read before its silence is judged. The arms' senders
        // go with them, so the writer ends once unregister drops the socket's own. An abort or
        // a close recorded its cause when asked for.
        let (aborted, cause) = tokio::select! {
            biased;
            _ = abort.notify.notified() => (true, None),
            _ = abort.close_expired() => (true, None),
            cause = read_loop(messages, id, owner, reply, &abort, &liveness) => (false, Some(cause)),
            () = keepalive(&liveness, limits.ping_interval, &abort, pinger) => (true, Some(Cause::DeadPeer)),
            () = going_away(id, owner, &abort, leaver) => (true, Some(Cause::GoingAway)),
        };
        if let Some(cause) = cause {
            abort.end_as(cause);
        }
        if aborted {
            // The writer may be stuck on a client that stopped reading.
            writer.abort();
        } else {
            // It flushes what is queued (a close echo included) and ends
            // once unregister drops the last senders.
            flushing = Some(writer);
        }
    }
    registration.unregister();
    emit(owner, WsEvent::Close { id }).await;
    registration.uncount();
    if let Some(mut writer) = flushing {
        // A client that stopped reading would keep it waiting forever.
        if tokio::time::timeout(CLOSE_TIMEOUT, &mut writer)
            .await
            .is_err()
        {
            writer.abort();
        }
    }
}

/// Frames a socket's writer gathers into one vectored write.
const WRITE_BATCH: usize = 64;

/// What a socket's writer needs besides its queue.
struct Socket {
    id: u64,
    owner: Arc<WorkerSlot>,
    abort: Arc<Abort>,
    liveness: Arc<Liveness>,
}

/// Drains a socket's queue: what is queued goes out together, in vectored writes straight from
/// the frames, so a socket holds no write buffer. Data frames leave the budgets once written;
/// when that empties a queue that refused something, the worker hears, without the writer waiting.
async fn write_loop<W: AsyncWrite + Unpin>(mut wr: W, mut rx: mpsc::Receiver<Out>, socket: Socket) {
    let Socket {
        id,
        owner,
        abort: budget,
        liveness,
    } = socket;
    while let Some(first) = rx.recv().await {
        let mut frames = Vec::new();
        let (mut charged, mut data) = (0, 0);
        let mut ping = false;
        let mut close = None;
        let mut next = Some(first);
        while let Some(out) = next.take() {
            match out {
                // An empty write would read as a closed stream.
                Out::Frame(bytes) if bytes.is_empty() => data += 1,
                Out::Control(bytes) if bytes.is_empty() => {}
                Out::Frame(bytes) => {
                    charged += bytes.len();
                    data += 1;
                    frames.push(bytes);
                }
                Out::Control(bytes) => frames.push(bytes),
                Out::Ping(bytes) => {
                    ping = true;
                    frames.push(bytes);
                }
                Out::Close(bytes) => {
                    close = Some(bytes);
                    break;
                }
            }
            if frames.len() < WRITE_BATCH {
                next = rx.try_recv().ok();
            }
        }
        let bytes: usize = frames.iter().map(Bytes::len).sum();
        if write_frames(&mut wr, frames).await.is_err() {
            return;
        }
        budget.queued_bytes.fetch_sub(charged, Ordering::SeqCst);
        budget.queued_frames.fetch_sub(data, Ordering::SeqCst);
        COUNTS.add_n(WsCount::MessagesSent, data as u64);
        COUNTS.add_n(WsCount::SentBytes, bytes as u64);
        if ping {
            liveness.ping_written();
        }
        if let Some(bytes) = close {
            if wr.write_all(&bytes).await.is_ok() {
                COUNTS.add_n(WsCount::SentBytes, bytes.len() as u64);
            }
            let _ = wr.flush().await;
            let _ = wr.shutdown().await;
            return;
        }
        if wr.flush().await.is_err() {
            return;
        }
        // A socket on its way out has nothing left to resume.
        if !budget.closing.load(Ordering::SeqCst) && budget.drained() {
            hand_off(&owner, WsEvent::Drained { id });
        }
    }
    let _ = wr.shutdown().await;
}

/// emit for a task that must not wait on the worker: a full inbox gets
/// the event from a task of its own.
fn hand_off(owner: &Arc<WorkerSlot>, event: WsEvent) {
    if let Err(event) = try_emit(owner, event) {
        let owner = owner.clone();
        tokio::spawn(async move { emit(&owner, event).await });
    }
}

/// Write `frames` whole and in order, as few writes as the stream takes.
async fn write_frames<W: AsyncWrite + Unpin>(wr: &mut W, mut frames: Vec<Bytes>) -> io::Result<()> {
    let mut first = 0;
    while first < frames.len() {
        let slices: Vec<_> = frames[first..]
            .iter()
            .map(|frame| io::IoSlice::new(frame))
            .collect();
        let mut written = wr.write_vectored(&slices).await?;
        drop(slices);
        if written == 0 {
            return Err(io::ErrorKind::WriteZero.into());
        }
        while written > 0 {
            let frame = &mut frames[first];
            if written < frame.len() {
                bytes::Buf::advance(frame, written);
                break;
            }
            written -= frame.len();
            first += 1;
        }
    }
    Ok(())
}

/// Reads a socket's messages into its worker's inbox, each once it has
/// the credit for it: with `Limits::inbound` unhandled, nothing more
/// is read, and TCP pushes back on the client. Returns why reading
/// ended.
async fn read_loop<R: AsyncRead + Unpin>(
    mut messages: Reader<R>,
    id: u64,
    owner: &WorkerSlot,
    reply: mpsc::Sender<Out>,
    credit: &Abort,
    liveness: &Liveness,
) -> Cause {
    loop {
        credit.take_credit(owner, liveness).await;
        let (data, text) = loop {
            match messages.next().await {
                Ok(Message::Text(data)) => break (data.into(), true),
                Ok(Message::Binary(data)) => break (data, false),
                Ok(Message::Ping(payload)) => {
                    let _ = reply.try_send(Out::Control(encode(Opcode::Pong, &payload)));
                }
                Ok(_) => {}
                Err(ended) => {
                    if let Some(code) = ended.code {
                        let _ = reply.try_send(Out::Close(close_frame(code)));
                    }
                    return ended.cause;
                }
            }
        };
        COUNTS.add(WsCount::MessagesReceived);
        let message = WsEvent::Message { id, data, text };
        if let Err(message) = try_emit(owner, message) {
            liveness.on_worker(emit(owner, message)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncReadExt;

    use super::*;
    use crate::ws::QUEUE_FRAMES;

    #[tokio::test]
    async fn queued_frames_reach_a_slow_client_whole_and_in_order() {
        // Its writes count in the process-wide counters socket tests read.
        let _serial = crate::testing::SERIAL.lock().await;
        let (client, conn) = tokio::io::duplex(1000);
        let (tx, rx) = mpsc::channel(QUEUE_FRAMES);
        let budget = Arc::new(Abort::default());
        let socket = Socket {
            id: 1,
            owner: Arc::new(WorkerSlot::new(true, 1, None)),
            abort: budget.clone(),
            liveness: Arc::new(Liveness::new()),
        };
        let writer = tokio::spawn(write_loop(conn, rx, socket));
        let small = |n: usize| encode(Opcode::Text, n.to_string().as_bytes());
        let mut frames: Vec<Bytes> = (0..50).map(small).collect();
        frames.push(encode(Opcode::Binary, &vec![7; 300_000]));
        frames.extend((50..100).map(small));
        let reading = tokio::spawn(async move {
            let mut received = Vec::new();
            let mut client = client;
            client.read_to_end(&mut received).await.map(|_| received)
        });

        for frame in &frames {
            budget.queued_bytes.fetch_add(frame.len(), Ordering::SeqCst);
            budget.queued_frames.fetch_add(1, Ordering::SeqCst);
            tx.send(Out::Frame(frame.clone()))
                .await
                .expect("the writer stopped taking frames");
        }
        drop(tx);
        writer.await.expect("writer");
        assert_eq!(
            budget.queued_bytes.load(Ordering::SeqCst),
            0,
            "written frames must leave the byte budget"
        );
        assert_eq!(budget.pending(), 0, "written frames are no longer pending");

        let received = reading.await.expect("join").expect("client read");
        assert!(
            received == frames.concat(),
            "the client got {} bytes, {} were queued, or they came out of order",
            received.len(),
            frames.concat().len()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_client_has_an_interval_to_answer_from_when_its_ping_went_out_and_none_with_pings_off(
    ) {
        let interval = Duration::from_secs(2);
        let (liveness, abort) = (Liveness::new(), Abort::default());
        let (tx, mut queued) = mpsc::channel(4);
        let dead = keepalive(&liveness, interval, &abort, tx);
        tokio::pin!(dead);
        // Silent since it opened: a ping is queued after an interval, and
        // the writer gets it out only at 3.5 s, behind other frames.
        let early = tokio::time::timeout(Duration::from_millis(3500), &mut dead);
        assert!(early.await.is_err());
        assert!(matches!(queued.try_recv(), Ok(Out::Ping(_))));
        liveness.ping_written();

        let late = tokio::time::timeout(Duration::from_millis(1999), &mut dead);
        assert!(
            late.await.is_err(),
            "two intervals of silence, but not one since the ping went out"
        );
        let due = tokio::time::timeout(Duration::from_millis(2), &mut dead);
        assert!(due.await.is_ok());

        // Zero: no pings, and no client is ever dropped for its silence.
        let (tx, mut queued) = mpsc::channel(4);
        let off = keepalive(&liveness, Duration::ZERO, &abort, tx);
        assert!(tokio::time::timeout(interval * 100, off).await.is_err());
        assert!(queued.try_recv().is_err(), "no ping");
    }
}
