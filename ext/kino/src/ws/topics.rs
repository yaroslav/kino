//! WebSocket topics: a socket subscribes with an envelope (a prefix and a suffix). A publish,
//! from any Ractor, only queues for one fan-out thread, which builds one frame per envelope
//! and queues the same bytes to every socket that shares it.

use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, LazyLock};

use bytes::Bytes;
use magnus::{Error, RString, Ruby};
use parking_lot::{RwLock, RwLockWriteGuard};

use super::codec::{encode_parts, Opcode};
use super::socket::charge;
use super::stats::{WsCount, COUNTS};
use super::Conn;
use crate::counters::Counted;

/// Publishes waiting for the fan-out thread, and their bytes: past either, publish fails
/// rather than block. Anything fits an empty queue.
const FANOUT_QUEUE: usize = 65_536;
const FANOUT_BYTES: usize = 64 << 20;

/// What a publish that finds a subscriber's queue full does to it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WhenFull {
    /// Drop the socket as too slow.
    Close,
    /// Leave this message out for that socket only.
    Skip,
}

struct Subscriber {
    conn: Conn,
    when_full: WhenFull,
}

struct Envelope {
    prefix: Bytes,
    suffix: Bytes,
    /// Both parts are UTF-8, so text publishes can use it.
    text: bool,
    sockets: HashMap<u64, Subscriber>,
}

impl Envelope {
    fn new(prefix: Bytes, suffix: Bytes) -> Self {
        let text = std::str::from_utf8(&prefix).is_ok() && std::str::from_utf8(&suffix).is_ok();
        Envelope {
            prefix,
            suffix,
            text,
            sockets: HashMap::new(),
        }
    }

    /// None for a text publish through an envelope that is not UTF-8.
    fn frame(&self, opcode: Opcode, body: &[u8]) -> Option<Bytes> {
        (opcode == Opcode::Binary || self.text)
            .then(|| encode_parts(opcode, &[&self.prefix, body, &self.suffix]))
    }
}

#[derive(Default)]
struct Registry {
    topics: HashMap<Bytes, Vec<Envelope>>,
    /// Each socket's topics, so forgetting it touches only those.
    by_socket: HashMap<u64, Vec<Bytes>>,
}

/// Never held together with CONNS: a fan-out holds it for reading while it queues to
/// every subscriber. A Ruby thread never waits for it with the GVL held
/// (`without_waiting_on_gvl`).
static REGISTRY: LazyLock<RwLock<Registry>> = LazyLock::new(Default::default);

/// How a change to the registry takes its lock.
#[derive(Clone, Copy)]
pub(super) enum Wait {
    Block,
    Try,
}

fn lock(wait: Wait) -> Option<RwLockWriteGuard<'static, Registry>> {
    match wait {
        Wait::Block => Some(REGISTRY.write()),
        Wait::Try => REGISTRY.try_write(),
    }
}

impl Registry {
    fn add(&mut self, id: u64, topic: Bytes, envelope: Envelope, subscriber: Subscriber) {
        let envelopes = self.topics.entry(topic.clone()).or_default();
        let index = envelopes
            .iter()
            .position(|e| e.prefix == envelope.prefix && e.suffix == envelope.suffix)
            .unwrap_or_else(|| {
                envelopes.push(envelope);
                envelopes.len() - 1
            });
        envelopes[index].sockets.insert(id, subscriber);
        let topics = self.by_socket.entry(id).or_default();
        if !topics.contains(&topic) {
            topics.push(topic);
        }
    }

    /// Out of the envelopes of `topic` that `matches` selects; true if it was in one.
    fn remove(&mut self, id: u64, topic: &[u8], matches: impl Fn(&Envelope) -> bool) -> bool {
        let Some(envelopes) = self.topics.get_mut(topic) else {
            return false;
        };
        let (mut removed, mut still) = (false, false);
        envelopes.retain_mut(|envelope| {
            if matches(envelope) {
                removed |= envelope.sockets.remove(&id).is_some();
            }
            still |= envelope.sockets.contains_key(&id);
            !envelope.sockets.is_empty()
        });
        if envelopes.is_empty() {
            self.topics.remove(topic);
        }
        if !still {
            if let Some(topics) = self.by_socket.get_mut(&id) {
                topics.retain(|t| t != topic);
                if topics.is_empty() {
                    self.by_socket.remove(&id);
                }
            }
        }
        removed
    }
}

/// Deliver `topic`'s publishes to socket `id` as `prefix + body + suffix` frames; the same
/// envelope again keeps one subscription with the new `when_full`. False once the socket is
/// gone; None when `wait` is Try and the lock is held.
pub(super) fn subscribe_with(
    wait: Wait,
    id: u64,
    topic: &Bytes,
    prefix: &Bytes,
    suffix: &Bytes,
    when_full: WhenFull,
) -> Option<bool> {
    let Some(conn) = super::with(id, Conn::clone) else {
        return Some(false);
    };
    let envelope = Envelope::new(prefix.clone(), suffix.clone());
    let mut registry = lock(wait)?;
    if !conn.subscribing() {
        return Some(false);
    }
    registry.add(id, topic.clone(), envelope, Subscriber { conn, when_full });
    Some(true)
}

/// Those made with `prefix` (any suffix), or all; false if none, None as for subscribe_with.
fn unsubscribe_with(wait: Wait, id: u64, topic: &[u8], prefix: Option<&[u8]>) -> Option<bool> {
    Some(lock(wait)?.remove(id, topic, |e| {
        prefix.is_none_or(|prefix| e.prefix == prefix)
    }))
}

/// At once while the lock is free, else waiting with the GVL released: waiting with it held
/// would stop the Ractor's other threads, and under Ractors every GC, for a whole fan-out.
/// `change` must not touch Ruby.
fn without_waiting_on_gvl<T>(change: impl Fn(Wait) -> Option<T>) -> Result<T, Error> {
    if let Some(done) = change(Wait::Try) {
        return Ok(done);
    }
    crate::gvl::without_gvl(
        || change(Wait::Block).expect("a blocking lock always comes"),
        None,
    )
}

/// Every subscription of socket `id`, from its unregister once it was marked gone. On a Ruby
/// thread (a handshake refused once registered) it waits as subscribe does.
pub fn forget(id: u64) {
    let change = |wait| {
        let mut registry = lock(wait)?;
        for topic in registry.by_socket.remove(&id).unwrap_or_default() {
            registry.remove(id, &topic, |_| true);
        }
        Some(())
    };
    if Ruby::get().is_ok() {
        // Only a panic fails it, and the panic hook has reported that.
        let _ = without_waiting_on_gvl(change);
    } else {
        change(Wait::Block);
    }
}

struct Message {
    topic: Bytes,
    body: Bytes,
    opcode: Opcode,
    /// Its bytes in the queue, given back once it is fanned out or refused.
    _queued: Counted,
}

struct Fanout {
    queue: flume::Sender<Message>,
    bytes: Arc<AtomicUsize>,
    max_bytes: usize,
}

impl Fanout {
    fn bounded(capacity: usize, max_bytes: usize) -> (Fanout, flume::Receiver<Message>) {
        let (queue, published) = flume::bounded(capacity);
        let fanout = Fanout {
            queue,
            bytes: Arc::default(),
            max_bytes,
        };
        (fanout, published)
    }

    fn publish(&self, topic: &[u8], body: &[u8], opcode: Opcode) -> bool {
        let len = topic.len() + body.len();
        // A refused message gives its bytes back as it drops.
        let queued = charge(&self.bytes, self.max_bytes, len)
            && self
                .queue
                .try_send(Message {
                    topic: Bytes::copy_from_slice(topic),
                    body: Bytes::copy_from_slice(body),
                    opcode,
                    _queued: Counted::adopt(self.bytes.clone(), len),
                })
                .is_ok();
        if !queued {
            COUNTS.add(WsCount::PublishesRefused);
        }
        queued
    }
}

static FANOUT: LazyLock<Fanout> = LazyLock::new(|| {
    let (fanout, published) = Fanout::bounded(FANOUT_QUEUE, FANOUT_BYTES);
    std::thread::Builder::new()
        .name("kino-ws-fanout".into())
        .spawn(move || {
            for message in published.iter() {
                fan_out(&message);
            }
        })
        .expect("spawn the WebSocket fan-out thread");
    fanout
});

/// UTF-8 `body` as a text frame to `topic`'s UTF-8 envelopes; false only when the fan-out
/// queue is full.
pub fn publish(topic: &[u8], body: &[u8]) -> bool {
    FANOUT.publish(topic, body, Opcode::Text)
}

/// A binary frame, to every subscriber.
pub fn publish_binary(topic: &[u8], body: &[u8]) -> bool {
    FANOUT.publish(topic, body, Opcode::Binary)
}

fn fan_out(message: &Message) {
    let registry = REGISTRY.read();
    let Some(envelopes) = registry.topics.get(&message.topic) else {
        return;
    };
    for envelope in envelopes {
        let Some(frame) = envelope.frame(message.opcode, &message.body) else {
            continue;
        };
        for subscriber in envelope.sockets.values() {
            if subscriber.conn.offer(frame.clone()) {
                continue;
            }
            match subscriber.when_full {
                WhenFull::Skip => {
                    COUNTS.add(WsCount::MessagesSkipped);
                    // The app may resync it once its queue has emptied.
                    subscriber.conn.refused();
                }
                WhenFull::Close => subscriber.conn.drop_slow(),
            }
        }
    }
}

/// `skip` stands for WhenFull::Skip.
pub fn ws_subscribe(
    id: u64,
    topic: RString,
    prefix: RString,
    suffix: RString,
    skip: bool,
) -> Result<bool, Error> {
    // Copied while the GVL is held: the change may run without it.
    let [topic, prefix, suffix] =
        [topic, prefix, suffix].map(|string| Bytes::copy_from_slice(unsafe { string.as_slice() }));
    let when_full = if skip {
        WhenFull::Skip
    } else {
        WhenFull::Close
    };
    without_waiting_on_gvl(|wait| subscribe_with(wait, id, &topic, &prefix, &suffix, when_full))
}

pub fn ws_unsubscribe(id: u64, topic: RString, prefix: Option<RString>) -> Result<bool, Error> {
    let topic = unsafe { topic.as_slice() }.to_vec();
    let prefix = prefix.map(|prefix| unsafe { prefix.as_slice() }.to_vec());
    without_waiting_on_gvl(|wait| unsubscribe_with(wait, id, &topic, prefix.as_deref()))
}

/// Callable from any Ractor; ArgumentError for a text body that is not UTF-8.
pub fn ws_publish(ruby: &Ruby, topic: RString, body: RString, binary: bool) -> Result<bool, Error> {
    let (topic, body) = unsafe { (topic.as_slice(), body.as_slice()) };
    if binary {
        return Ok(publish_binary(topic, body));
    }
    if std::str::from_utf8(body).is_err() {
        return Err(Error::new(
            ruby.exception_arg_error(),
            "a text publish needs a UTF-8 body; publish bytes with binary: true",
        ));
    }
    Ok(publish(topic, body))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use crate::testing::{eventually_blocking, serial_blocking};
    use crate::ws::testing::{closes_since, subscribe, unregistered, QueuedSocket};
    use crate::ws::{self, snapshot};

    /// A frame as it should be queued (Bytes print readably).
    fn frame(wire: &'static [u8]) -> Bytes {
        Bytes::from_static(wire)
    }

    #[test]
    fn a_full_subscriber_is_dropped_others_still_get_it() {
        let _serial = serial_blocking();
        let topic = b"a_full_subscriber_is_dropped_others_still_get_it";
        let slow = QueuedSocket::new(1);
        let mut fast = QueuedSocket::new(8);
        assert!(subscribe(slow.id, topic, b"", b"", WhenFull::Close));
        assert!(subscribe(fast.id, topic, b"", b"", WhenFull::Close));
        let before = snapshot();

        // The first fills the slow socket's queue; the others find it full.
        for body in [b"1", b"2", b"3", b"4"] {
            assert!(publish(topic, body));
        }

        for wire in [b"\x81\x011", b"\x81\x012", b"\x81\x013", b"\x81\x014"] {
            assert_eq!(fast.next_frame(), frame(wire), "the socket keeping up");
        }
        assert!(slow.aborted(), "a socket too slow for a publish is dropped");
        slow.close();
        assert_eq!(
            closes_since(&before),
            [("slow", 1)],
            "a slow socket closes once as slow, however many publishes find it full"
        );
    }

    #[test]
    fn a_full_subscriber_that_skips_misses_only_what_found_it_full() {
        let _serial = serial_blocking();
        let topic = b"a_full_subscriber_that_skips";
        let mut slow = QueuedSocket::new(1);
        let mut fast = QueuedSocket::new(8);
        assert!(subscribe(slow.id, topic, b"", b"", WhenFull::Skip));
        assert!(subscribe(fast.id, topic, b"", b"", WhenFull::Close));
        let before = snapshot();

        // The first fills the slow socket's queue; the next two find it full.
        for body in [b"1", b"2", b"3"] {
            assert!(publish(topic, body));
        }
        eventually_blocking("the slow socket to be found full", || {
            (snapshot().count(WsCount::MessagesSkipped)
                == before.count(WsCount::MessagesSkipped) + 2)
                .then_some(())
        });
        assert_eq!(slow.next_frame(), frame(b"\x81\x011"));
        assert!(publish(topic, b"4"));

        let next = slow.next_frame();
        assert_eq!(next, frame(b"\x81\x014"), "room again, so it gets the next");
        assert!(!slow.aborted(), "a socket that skips is kept");
        for wire in [b"\x81\x011", b"\x81\x012", b"\x81\x013", b"\x81\x014"] {
            assert_eq!(fast.next_frame(), frame(wire), "the socket keeping up");
        }
    }

    #[test]
    fn a_closed_socket_leaves_every_topic() {
        let _serial = serial_blocking();
        let socket = QueuedSocket::new(8);
        let topics: [&[u8]; 2] = [b"a_closed_socket/1", b"a_closed_socket/2"];
        for topic in topics {
            assert!(subscribe(socket.id, topic, b"<", b">", WhenFull::Close));
        }

        socket.close();

        assert!(
            socket.released(),
            "a subscription outlived its socket and keeps its writer waiting"
        );
    }

    #[test]
    fn during_a_fan_out_writes_and_closes_go_on_and_a_subscribe_that_waited_out_a_close_fails() {
        let _serial = serial_blocking();
        let socket = Arc::new(QueuedSocket::new(8));
        let mut bystander = QueuedSocket::new(8);
        // A long fan-out in progress holds the registry for reading; the
        // socket subscribes, which found it, then waits while it closes.
        let fan_out = REGISTRY.read();
        let subscriber = std::thread::spawn({
            let id = socket.id;
            move || subscribe(id, b"during_a_fan_out", b"", b"", WhenFull::Close)
        });
        // A writer waiting for the lock bars new readers; it also goes
        // first once the fan-out ends, ahead of the forget.
        eventually_blocking("the subscribe to reach the topic lock", || {
            REGISTRY.try_read().is_none().then_some(())
        });
        let closer = std::thread::spawn({
            let socket = socket.clone();
            move || socket.close()
        });
        eventually_blocking("the close to leave CONNS during the fan-out", || {
            (unregistered(socket.id) == Some(true)).then_some(())
        });

        // Another socket's write goes through, and one that never subscribed
        // closes, with nothing to forget.
        let (done, finished) = std::sync::mpsc::channel();
        let (bystander_id, quiet) = (bystander.id, QueuedSocket::new(8));
        std::thread::spawn(move || {
            let _ = done.send(ws::write(bystander_id, Opcode::Text, b"meanwhile"));
            quiet.close();
            let _ = done.send(true);
        });
        for what in [
            "a write to another socket",
            "a close of one never subscribed",
        ] {
            let finished = finished.recv_timeout(Duration::from_secs(5));
            assert!(finished.unwrap_or_else(|_| panic!("{what} waited for the fan-out")));
        }

        drop(fan_out);
        closer.join().expect("closer thread");
        let subscribed = subscriber.join().expect("subscriber thread");
        assert!(!subscribed, "the socket closed before it subscribed");
        assert!(
            socket.released(),
            "a subscription outlived its socket and keeps its writer waiting"
        );
        assert_eq!(bystander.next_frame(), frame(b"\x81\x09meanwhile"));
    }

    #[test]
    fn a_publish_past_the_fan_out_queues_frames_or_bytes_is_refused_until_its_thread_takes_some() {
        let _serial = serial_blocking();
        let (fanout, queue) = Fanout::bounded(2, 10);
        let before = snapshot().count(WsCount::PublishesRefused);

        // A publish costs its topic and body.
        let bodies: [&[u8]; 4] = [b"12345", b"123456", b"12", b""];
        let published = bodies.map(|body| fanout.publish(b"t", body, Opcode::Text));
        assert_eq!(
            published,
            [true, false, true, false],
            "13 bytes would pass the budget, and a third publish the frames"
        );
        assert_eq!(snapshot().count(WsCount::PublishesRefused), before + 2);
        for _ in 0..2 {
            drop(queue.recv().expect("a queued publish"));
        }
        assert!(
            fanout.publish(b"t", &[b'x'; 100], Opcode::Text),
            "fanned out, the publishes give their bytes back, and any publish fits an empty queue"
        );
    }
}
