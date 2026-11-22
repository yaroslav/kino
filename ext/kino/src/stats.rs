//! A server's stats, taken once and rendered three ways: /stats JSON and
//! Server#stats through one `Emit` visitor, /metrics from a table of rows.

use std::fmt::{Display, Write};
use std::sync::atomic::Ordering;

use magnus::value::ReprValue;
use magnus::{Error, RArray, RHash, Ruby, Value};

use crate::counters::Tallied;
use crate::registry::{
    QueueHistogramSnapshot, ServerInner, QUEUE_BOUNDS_US, STATE_DRAINING, STATE_READY,
};
use crate::server::Topology;
use crate::ws::{Cause, Refusal, WsCount, WsSnapshot};

/// One dispatch slot's sensors, captured for a response. busy_ms is the
/// age of the current in-flight work (0 when idle), the wedge signal.
/// A worker_loop worker's is how long its thread has been away from its loop with work waiting.
pub struct WorkerStat {
    pub index: usize,
    pub served: u64,
    pub in_flight: usize,
    pub busy_ms: u64,
    pub quarantined: bool,
    /// Sent home by the pool scaler; leaves at its next idle tick.
    pub retired: bool,
    /// WebSockets the slot's worker accepted and still owes an on_close.
    pub sockets: usize,
    /// Connections its requests handed over through rack.hijack, still open.
    pub hijacked: usize,
    pub inline_conns: usize,
    /// Which worker the slot is on (WorkerSlot::generation).
    pub generation: u64,
}

/// Read every slot's per-worker sensors in one pass under the slots read
/// lock (the same lock lane_depths uses), computing busy_ms against one
/// "now". No Ruby, so it is safe on the control thread and below the GVL.
pub fn collect_worker_status(server: &ServerInner) -> Vec<WorkerStat> {
    let now = crate::mono::mono_us();
    server
        .slots
        .read()
        .iter()
        .enumerate()
        .map(|(index, slot)| {
            let quarantined = slot.life.quarantined();
            WorkerStat {
                index,
                served: slot.load.served(),
                in_flight: slot.load.in_flight(),
                // A quarantined slot is a known, handled wedge: report 0 so
                // it never re-trips detection or reads as a live wedge.
                busy_ms: match slot.busy_since() {
                    Some(started) if !quarantined => now.saturating_sub(started) / 1000,
                    _ => 0,
                },
                quarantined,
                retired: slot.life.seems_retired(),
                sockets: slot.sockets.open(),
                hijacked: slot.hijacked.load(Ordering::Relaxed),
                inline_conns: slot.inline.conns.load(Ordering::Relaxed),
                generation: slot.generation(),
            }
        })
        .collect()
}

/// Everything the endpoints report, captured in one pass so a response
/// is internally consistent.
#[cfg_attr(test, derive(Default))]
pub struct Snapshot {
    pub topology: Topology,
    pub respawns: u64,
    pub queued: usize,
    pub in_flight: usize,
    pub served: u64,
    pub rejected: u64,
    pub timeouts: u64,
    pub lane_depths: Option<Vec<usize>>,
    pub state: u8,
    pub worker_status: Vec<WorkerStat>,
    pub quarantined_count: usize,
    pub quarantine_replacements: u64,
    pub active_workers: usize,
    pub scale_ups: u64,
    pub scale_downs: u64,
    pub queue_histogram: QueueHistogramSnapshot,
    pub wakes: u64,
    /// The worker rows' hijacked connections together.
    pub hijacked_connections: usize,
    /// Process-wide: streamed responses cut, by cause.
    pub responses_cut: [(&'static str, u64); crate::response::Cut::ALL.len()],
    /// Process-wide, like the topics a publish reaches.
    pub websockets: WsSnapshot,
    /// None when the server serves nothing inline.
    pub inline: Option<InlineSnapshot>,
}

/// A server's inline serving: its connections now, and totals of the requests served, the
/// connections declined at accept, handed back to hyper or cut off, and the spins.
pub struct InlineSnapshot {
    pub connections: usize,
    pub requests: u64,
    pub declined: u64,
    pub handbacks: u64,
    pub closed: u64,
    pub spins: u64,
}

impl InlineSnapshot {
    /// None when `server` serves nothing inline; its connections now are its worker rows'.
    fn take(server: &ServerInner, workers: &[WorkerStat]) -> Option<Self> {
        let inline = server.inline.as_ref()?;
        let (mut requests, mut closed, mut spins) = (0, 0, 0);
        for seats in server.slots.read().iter().map(|slot| &slot.inline) {
            requests += seats.requests.load(Ordering::Relaxed);
            closed += seats.closed.load(Ordering::Relaxed);
            spins += seats.spins.load(Ordering::Relaxed);
        }
        Some(InlineSnapshot {
            connections: workers.iter().map(|w| w.inline_conns).sum(),
            requests,
            declined: inline.declined(),
            handbacks: inline.handbacks(),
            closed,
            spins,
        })
    }

    /// Its counts, in /stats's order: each one's key there, its metric's name and help, and its
    /// value.
    // A table, as Snapshot::counts's.
    #[rustfmt::skip]
    fn counts(&self) -> [(&'static str, Metric, u64); 6] {
        [
            ("connections", ("kino_inline_connections",
                "Connections served inline on the workers' threads now."),
                self.connections as u64),
            ("requests", ("kino_inline_requests_total", "Requests served inline."), self.requests),
            ("declined", ("kino_inline_declined_total",
                "Plain TCP and unix-socket connections left with hyper at accept, as no worker could take them (or none served yet)."),
                self.declined),
            ("handbacks", ("kino_inline_handbacks_total", "Inline connections handed back to hyper."),
                self.handbacks),
            ("closed", ("kino_inline_closed_total", "Inline connections Kino cut off."), self.closed),
            ("spins", ("kino_inline_spins_total",
                "Spins of an inline connection alone on its worker for its next request, before its job waits on the socket."),
                self.spins),
        ]
    }
}

/// One stat's value; a name is a fixed identifier, a Symbol in Ruby.
/// Nil is a section the server lacks: null in JSON, nil in Ruby.
enum Stat<'a> {
    Count(u64),
    Flag(bool),
    Seconds(f64),
    Name(&'a str),
    Nil,
}

/// What `nest` opens: a Map keeps its entries' keys, a List drops them.
enum Shape {
    Map,
    List,
}

/// The key a List's item is given, which the List drops.
const ITEM: &str = "";

/// Where a Snapshot goes, serde-style: each stat under its key, Maps and
/// Lists through `nest`.
trait Emit {
    fn stat(&mut self, key: &str, stat: Stat);

    fn nest(&mut self, key: &str, shape: Shape, body: impl FnOnce(&mut Self));

    fn counts<'a>(&mut self, counts: impl IntoIterator<Item = (&'a str, u64)>) {
        for (key, count) in counts {
            self.stat(key, Stat::Count(count));
        }
    }
}

impl Snapshot {
    pub fn take(server: &ServerInner) -> Self {
        let worker_status = collect_worker_status(server);
        let quarantined_count = worker_status.iter().filter(|w| w.quarantined).count();
        let hijacked_connections = worker_status.iter().map(|w| w.hijacked).sum();
        let inline = InlineSnapshot::take(server, &worker_status);
        let c = &server.counters;
        Snapshot {
            topology: server.config.topology.clone(),
            respawns: c.respawns.load(Ordering::Relaxed),
            queued: server.queued(),
            in_flight: c.in_flight.load(Ordering::Relaxed),
            served: c.served.load(Ordering::Relaxed),
            rejected: c.rejected.load(Ordering::Relaxed),
            timeouts: c.timeouts.load(Ordering::Relaxed),
            lane_depths: server.lane_depths(),
            state: server.state.load(Ordering::Relaxed),
            worker_status,
            quarantined_count,
            quarantine_replacements: c.quarantine_replacements.load(Ordering::Relaxed),
            active_workers: c.active_workers.load(Ordering::Relaxed),
            scale_ups: c.scale_ups.load(Ordering::Relaxed),
            scale_downs: c.scale_downs.load(Ordering::Relaxed),
            queue_histogram: c.queue_histogram.snapshot(),
            wakes: c.wakes.load(Ordering::Relaxed),
            hijacked_connections,
            responses_cut: crate::response::responses_cut(),
            websockets: crate::ws::snapshot(),
            inline,
        }
    }

    pub fn state_name(&self) -> &'static str {
        match self.state {
            STATE_READY => "ready",
            STATE_DRAINING => "draining",
            _ => "booting",
        }
    }

    /// The server's plain counts, in /stats's order: each one's key there, its metric's name
    /// and help (`batch` has none), and its value.
    // A table: rustfmt would give each row's key, metric and value a line each.
    #[rustfmt::skip]
    fn counts(&self) -> [(&'static str, Option<Metric>, u64); 15] {
        let topology = &self.topology;
        [
            ("workers", Some(("kino_workers", "Configured worker count.")), topology.workers as u64),
            ("threads", Some(("kino_threads_per_worker", "Configured threads per worker.")),
                topology.threads as u64),
            ("batch", None, topology.batch as u64),
            ("respawns", Some(("kino_worker_respawns_total",
                "Crashed workers replaced: a ractor respawned, or a worker thread replaced.")),
                self.respawns),
            ("queued", Some(("kino_queue_depth", "Requests waiting for a worker.")), self.queued as u64),
            ("in_flight", Some(("kino_requests_in_flight", "Requests currently inside Ruby workers.")),
                self.in_flight as u64),
            ("served", Some(("kino_requests_served_total", "Requests handed to Ruby workers.")),
                self.served),
            ("rejected", Some(("kino_requests_rejected_total", "Requests rejected with a 503.")),
                self.rejected),
            ("timeouts", Some(("kino_request_timeouts_total",
                "Responses past the request timeout (client got a 504).")),
                self.timeouts),
            ("max_workers", Some(("kino_max_workers",
                "Worker pool ceiling (equals kino_workers for a fixed pool).")),
                topology.max_workers as u64),
            ("active_workers", Some(("kino_active_workers", "Workers currently alive and serving.")),
                self.active_workers as u64),
            ("scale_ups", Some(("kino_scale_ups_total", "Workers added by the pool scaler.")),
                self.scale_ups),
            ("scale_downs", Some(("kino_scale_downs_total", "Idle workers retired by the pool scaler.")),
                self.scale_downs),
            ("wakes", Some(("kino_wakes_total",
                "Wake hook runs handed to workers; wakes that coalesce count once.")),
                self.wakes),
            ("hijacked_connections", Some(("kino_hijacked_connections",
                "Connections taken over through rack.hijack, or switched by a 101 with a callable, still open.")),
                self.hijacked_connections as u64),
        ]
    }

    /// Every stat /stats and Server#stats share, in that order.
    fn emit(&self, e: &mut impl Emit) {
        use Shape::{List, Map};
        let topology = &self.topology;
        e.stat("mode", Stat::Name(&topology.mode));
        e.stat("lanes", Stat::Flag(self.lane_depths.is_some()));
        e.counts(self.counts().map(|(key, _, count)| (key, count)));
        e.nest("responses_cut", Map, |e| e.counts(self.responses_cut));
        let ws = &self.websockets;
        e.nest("websockets", Map, |e| {
            e.stat("open", Stat::Count(ws.open));
            e.counts(WsCount::named(ws.counts));
            let refused = Refusal::named(ws.handshakes_refused);
            e.nest("handshakes_refused", Map, |e| e.counts(refused));
            e.nest("closed", Map, |e| e.counts(Cause::named(ws.closed)));
        });
        match &self.inline {
            Some(inline) => e.nest("inline", Map, |e| {
                e.counts(inline.counts().map(|(key, _, count)| (key, count)));
            }),
            None => e.stat("inline", Stat::Nil),
        }
        if let Some(depths) = &self.lane_depths {
            let depths = depths.iter().map(|&depth| (ITEM, depth as u64));
            e.nest("lane_depths", List, |e| e.counts(depths));
        }
        e.nest("worker_status", List, |e| {
            for w in &self.worker_status {
                e.nest(ITEM, Map, |e| {
                    e.counts([
                        ("index", w.index as u64),
                        ("served", w.served),
                        ("in_flight", w.in_flight as u64),
                        ("busy_ms", w.busy_ms),
                    ]);
                    e.stat("quarantined", Stat::Flag(w.quarantined));
                    e.stat("retired", Stat::Flag(w.retired));
                    e.counts([
                        ("websockets_open", w.sockets as u64),
                        ("hijacked_connections", w.hijacked as u64),
                        ("inline_connections", w.inline_conns as u64),
                    ]);
                });
            }
        });
        let queue = &self.queue_histogram;
        e.nest("queue_time", Map, |e| {
            e.stat("count", Stat::Count(queue.count));
            e.stat("sum_seconds", Stat::Seconds(queue.sum_seconds()));
        });
        e.stat("quarantined", Stat::Count(self.quarantined_count as u64));
    }

    /// /stats: what Server#stats reports, plus the state and the version.
    pub fn json(&self) -> String {
        let mut json = Json {
            out: String::with_capacity(1024),
            first: true,
            shape: Shape::List,
        };
        json.nest(ITEM, Shape::Map, |json| {
            self.emit(json);
            json.stat("state", Stat::Name(self.state_name()));
            json.stat("version", Stat::Name(env!("CARGO_PKG_VERSION")));
        });
        json.out
    }

    /// Server#stats.
    pub fn hash(&self, ruby: &Ruby) -> Result<RHash, Error> {
        let hash = ruby.hash_new();
        let mut into = RubyHash {
            ruby,
            into: Container::Hash(hash),
            failed: Ok(()),
        };
        self.emit(&mut into);
        into.failed.map(|()| hash)
    }

    /// Prometheus text exposition (version 0.0.4): each metric a counter
    /// where its name ends in `_total`, else a gauge, but the histogram.
    pub fn metrics(&self) -> String {
        let mut out = String::with_capacity(8192);
        self.write_metrics(&mut out)
            .expect("writing to a String cannot fail");
        out
    }

    fn write_metrics(&self, out: &mut String) -> std::fmt::Result {
        for (name, help, samples) in self.metric_rows() {
            let kind = match samples {
                Samples::Histogram(_) => "histogram",
                _ if name.ends_with("_total") => "counter",
                _ => "gauge",
            };
            writeln!(out, "# HELP {name} {help}\n# TYPE {name} {kind}")?;
            match samples {
                Samples::One(value) => writeln!(out, "{name} {value}")?,
                Samples::By(label, values) => {
                    for (key, value) in values {
                        writeln!(out, "{name}{{{label}=\"{key}\"}} {value}")?;
                    }
                }
                Samples::Histogram(h) => {
                    let mut cumulative = 0;
                    for (count, bound_us) in h.buckets.iter().zip(QUEUE_BOUNDS_US) {
                        cumulative += count;
                        let le = bound_us as f64 / 1_000_000.0;
                        writeln!(out, "{name}_bucket{{le=\"{le}\"}} {cumulative}")?;
                    }
                    let total = cumulative + h.overflow;
                    writeln!(out, "{name}_bucket{{le=\"+Inf\"}} {total}")?;
                    writeln!(out, "{name}_sum {}\n{name}_count {total}", h.sum_seconds())?;
                }
            }
        }
        Ok(())
    }

    /// One sample per dispatch slot: `count` of its row.
    fn per_worker(&self, count: fn(&WorkerStat) -> u64) -> Samples<'_> {
        let sample = |w: &WorkerStat| (w.index.to_string(), count(w));
        Samples::By("worker", self.worker_status.iter().map(sample).collect())
    }

    /// Each metric's name, help and samples, in exposition order: /stats's, then the per-worker
    /// series and what only /metrics has.
    fn metric_rows(&self) -> Vec<(&'static str, &'static str, Samples<'_>)> {
        use Samples::{By, One};
        let ws = &self.websockets;
        let plain = self.counts().into_iter().filter_map(|(_, metric, count)| {
            let (name, help) = metric?;
            Some((name, help, One(count)))
        });
        let sockets = WsCount::ALL.map(|count| {
            let (name, help) = ws_metric(count);
            (name, help, One(ws.count(count)))
        });
        let lane = self.lane_depths.as_ref().map(|depths| {
            let sample = |(lane, &n): (usize, &usize)| (lane.to_string(), n as u64);
            let samples = By("lane", depths.iter().enumerate().map(sample).collect());
            let help = "Queued requests in each worker lane.";
            ("kino_lane_depth", help, samples)
        });
        // A table: rustfmt would give each row's name, help and samples a line each.
        #[rustfmt::skip]
        let (head, workers, tail) = ([
            ("kino_ready", "1 when serving, 0 while booting or draining.",
                One(u64::from(self.state == STATE_READY))),
            ("kino_responses_cut_total",
                "Streamed responses in this process that ended before the app finished them, by cause.",
                by_cause(&self.responses_cut)),
            ("kino_websockets_open", "Native WebSockets open in this process.", One(ws.open)),
        ], [
            ("kino_websocket_handshakes_refused_total",
                "WebSocket handshakes the app accepted that Kino answered with a 503, by cause.",
                by_cause(&Refusal::named(ws.handshakes_refused))),
            ("kino_websockets_closed_total", "Native WebSockets closed in this process, by cause.",
                by_cause(&Cause::named(ws.closed))),
            ("kino_worker_requests_served_total", "Requests handed to each dispatch slot.",
                self.per_worker(|w| w.served)),
            ("kino_worker_in_flight",
                "Requests, socket callbacks or wake hooks executing in each dispatch slot.",
                self.per_worker(|w| w.in_flight as u64)),
            ("kino_worker_busy_ms",
                "Age in ms of each slot's current request, socket callback or wake hook (0 when idle).",
                self.per_worker(|w| w.busy_ms)),
            ("kino_worker_websockets_open",
                "Native WebSockets each dispatch slot accepted whose close has not reached its worker yet.",
                self.per_worker(|w| w.sockets as u64)),
            ("kino_worker_hijacked_connections",
                "Connections each dispatch slot's requests handed over through rack.hijack, still open.",
                self.per_worker(|w| w.hijacked as u64)),
        ], [
            ("kino_quarantined_workers", "Dispatch slots abandoned as wedged.",
                One(self.quarantined_count as u64)),
            ("kino_quarantine_replacements_total", "Replacement workers spawned after a wedge.",
                One(self.quarantine_replacements)),
            ("kino_request_queue_seconds",
                "Seconds requests waited in the queue before a worker admitted them.",
                Samples::Histogram(&self.queue_histogram)),
        ]);
        // Only a server that serves inline has its series.
        let inline = self.inline.iter().flat_map(|inline| {
            let counts = inline.counts().into_iter();
            let help = "Connections each dispatch slot's worker serves inline now.";
            let per_worker = self.per_worker(|w| w.inline_conns as u64);
            counts
                .map(|(_, (name, help), count)| (name, help, One(count)))
                .chain([("kino_worker_inline_connections", help, per_worker)])
        });
        let stats = plain.chain(head).chain(sockets).chain(lane).chain(workers);
        stats.chain(inline).chain(tail).collect()
    }
}

/// A metric's name and help.
type Metric = (&'static str, &'static str);

// A table: rustfmt would give each row's name and help a line each.
#[rustfmt::skip]
fn ws_metric(count: WsCount) -> Metric {
    match count {
        WsCount::Opened => ("kino_websockets_opened_total", "Native WebSockets opened in this process."),
        WsCount::MessagesReceived =>
            ("kino_websocket_messages_received_total", "WebSocket messages received from clients."),
        WsCount::MessagesSent => ("kino_websocket_messages_sent_total",
            "WebSocket messages written to clients: writes and publishes, one per write_raw."),
        WsCount::MessagesSkipped => ("kino_websocket_messages_skipped_total",
            "Publishes left out for when_full: :skip subscribers whose queue was full, one per subscriber per publish."),
        WsCount::WritesRefused => ("kino_websocket_writes_refused_total",
            "WebSocket writes and closes the app made that a full queue refused."),
        WsCount::PublishesRefused => ("kino_websocket_publishes_refused_total",
            "Publishes refused because the fan-out queue was full."),
        WsCount::InboundPauses => ("kino_websocket_inbound_pauses_total",
            "Times a WebSocket's reader paused with 16 of its messages waiting for its worker."),
        WsCount::ReceivedBytes =>
            ("kino_websocket_received_bytes_total", "Bytes read from WebSocket clients."),
        WsCount::SentBytes => ("kino_websocket_sent_bytes_total", "Bytes written to WebSocket clients."),
    }
}

/// One sample per cause.
fn by_cause(counts: &[(&str, u64)]) -> Samples<'static> {
    let sample = |&(cause, n): &(&str, u64)| (cause.to_string(), n);
    Samples::By("cause", counts.iter().map(sample).collect())
}

/// A metric's samples: one, one per value of its label, or a histogram.
enum Samples<'a> {
    One(u64),
    By(&'static str, Vec<(String, u64)>),
    Histogram(&'a QueueHistogramSnapshot),
}

/// /stats's JSON. Keys and names are fixed identifiers: nothing to escape.
struct Json {
    out: String,
    /// Nothing written yet in the current object or list.
    first: bool,
    /// Whether the current container is an object or a list.
    shape: Shape,
}

impl Json {
    fn put(&mut self, key: &str, value: impl Display) {
        if !std::mem::take(&mut self.first) {
            self.out.push(',');
        }
        let written = match self.shape {
            Shape::Map => write!(self.out, "\"{key}\":{value}"),
            Shape::List => write!(self.out, "{value}"),
        };
        written.expect("writing to a String cannot fail");
    }
}

impl Emit for Json {
    fn stat(&mut self, key: &str, stat: Stat) {
        match stat {
            Stat::Count(count) => self.put(key, count),
            Stat::Flag(on) => self.put(key, on),
            Stat::Seconds(seconds) => self.put(key, seconds),
            Stat::Name(name) => self.put(key, format_args!("\"{name}\"")),
            Stat::Nil => self.put(key, "null"),
        }
    }

    fn nest(&mut self, key: &str, shape: Shape, body: impl FnOnce(&mut Self)) {
        let (open, close) = match shape {
            Shape::Map => ('{', '}'),
            Shape::List => ('[', ']'),
        };
        self.put(key, open);
        let outer = std::mem::replace(&mut self.shape, shape);
        self.first = true;
        body(self);
        self.shape = outer;
        self.out.push(close);
        self.first = false;
    }
}

/// Server#stats's Hash: Symbol keys, names as Symbols. Keeps the first
/// error a Hash or Array raised, and fills nothing more after it.
struct RubyHash<'r> {
    ruby: &'r Ruby,
    into: Container,
    failed: Result<(), Error>,
}

/// The Hash or Array being filled.
enum Container {
    Hash(RHash),
    List(RArray),
}

impl RubyHash<'_> {
    fn put(&mut self, key: &str, value: Value) {
        if self.failed.is_err() {
            return;
        }
        self.failed = match self.into {
            Container::Hash(hash) => hash.aset(self.ruby.to_symbol(key), value),
            Container::List(list) => list.push(value),
        };
    }
}

impl Emit for RubyHash<'_> {
    fn stat(&mut self, key: &str, stat: Stat) {
        let ruby = self.ruby;
        let value = match stat {
            Stat::Count(count) => ruby.into_value(count),
            Stat::Flag(on) => ruby.into_value(on),
            Stat::Seconds(seconds) => ruby.into_value(seconds),
            Stat::Name(name) => ruby.to_symbol(name).as_value(),
            Stat::Nil => ruby.qnil().as_value(),
        };
        self.put(key, value);
    }

    fn nest(&mut self, key: &str, shape: Shape, body: impl FnOnce(&mut Self)) {
        let (inner, value) = match shape {
            Shape::Map => {
                let hash = self.ruby.hash_new();
                (Container::Hash(hash), hash.as_value())
            }
            Shape::List => {
                let list = self.ruby.ary_new();
                (Container::List(list), list.as_value())
            }
        };
        self.put(key, value);
        let outer = std::mem::replace(&mut self.into, inner);
        body(self);
        self.into = outer;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering::Relaxed;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use super::*;
    use crate::queue::TICK;
    use crate::registry::{QueueHistogramSnapshot, STATE_BOOTING, STATE_READY};
    use crate::testing::{assign, slot, TestServer};

    /// A snapshot with every count distinct: lanes, a busy worker that owns
    /// two WebSockets, and a retired idle one whose requests handed three
    /// connections over through rack.hijack.
    fn snapshot() -> Snapshot {
        let worker = |index, served, in_flight, busy_ms, retired, sockets, hijacked| WorkerStat {
            index,
            served,
            in_flight,
            busy_ms,
            quarantined: false,
            retired,
            sockets,
            hijacked,
            inline_conns: 0,
            generation: 0,
        };
        let mut buckets = [0; crate::registry::QUEUE_BOUNDS_US.len()];
        buckets[0] = 3; // <= 0.5 ms
        buckets[2] = 1; // <= 2.5 ms
        Snapshot {
            topology: Topology {
                mode: "ractor".to_string(),
                workers: 8,
                max_workers: 32,
                threads: 1,
                batch: 1,
            },
            respawns: 2,
            queued: 3,
            in_flight: 4,
            served: 100,
            rejected: 5,
            timeouts: 6,
            lane_depths: Some(vec![2, 0]),
            state: STATE_READY,
            worker_status: vec![
                worker(0, 10, 1, 4, false, 2, 0),
                worker(1, 7, 0, 0, true, 0, 3),
            ],
            quarantined_count: 1,
            quarantine_replacements: 7,
            active_workers: 12,
            scale_ups: 3,
            scale_downs: 1,
            queue_histogram: QueueHistogramSnapshot {
                buckets,
                overflow: 1,
                sum_us: 1_500_000,
                count: 5,
            },
            wakes: 9,
            hijacked_connections: 2,
            responses_cut: [("client_gone", 21), ("shutdown", 22), ("crash", 23)],
            websockets: crate::ws::WsSnapshot {
                open: 7,
                counts: [1000, 41, 43, 5, 12, 1, 13, 4096, 8192],
                closed: std::array::from_fn(|n| 100 + n as u64),
                handshakes_refused: [11, 14, 15],
            },
            inline: None,
        }
    }

    #[test]
    fn stats_json_reports_every_field_in_order_and_the_state_name() {
        let expected = concat!(
            r#"{"mode":"ractor","lanes":true,"workers":8,"threads":1,"batch":1,"respawns":2,"#,
            r#""queued":3,"in_flight":4,"served":100,"rejected":5,"timeouts":6,"max_workers":32,"#,
            r#""active_workers":12,"scale_ups":3,"scale_downs":1,"wakes":9,"#,
            r#""hijacked_connections":2,"#,
            r#""responses_cut":{"client_gone":21,"shutdown":22,"crash":23},"#,
            r#""websockets":{"open":7,"opened":1000,"messages_received":41,"messages_sent":43,"#,
            r#""messages_skipped":5,"writes_refused":12,"publishes_refused":1,"#,
            r#""inbound_pauses":13,"received_bytes":4096,"sent_bytes":8192,"#,
            r#""handshakes_refused":{"websocket_max":11,"draining":14,"worker_closing":15},"#,
            r#""closed":{"normal":100,"going_away":101,"protocol":102,"too_big":103,"slow":104,"#,
            r#""dead_peer":105,"client_gone":106,"crash":107,"handshake_failed":108}},"#,
            r#""inline":null,"lane_depths":[2,0],"worker_status":["#,
            r#"{"index":0,"served":10,"in_flight":1,"busy_ms":4,"quarantined":false,"#,
            r#""retired":false,"websockets_open":2,"hijacked_connections":0,"#,
            r#""inline_connections":0},"#,
            r#"{"index":1,"served":7,"in_flight":0,"busy_ms":0,"quarantined":false,"#,
            r#""retired":true,"websockets_open":0,"hijacked_connections":3,"#,
            r#""inline_connections":0}],"#,
            r#""queue_time":{"count":5,"sum_seconds":1.5},"quarantined":1,"state":"ready","#,
        );
        let version = env!("CARGO_PKG_VERSION");
        assert_eq!(
            snapshot().json(),
            format!(r#"{expected}"version":"{version}"}}"#)
        );

        let plain = Snapshot {
            lane_depths: None,
            worker_status: Vec::new(),
            state: STATE_BOOTING,
            ..snapshot()
        };
        let json = plain.json();
        for (needle, present) in [
            (r#""lanes":false"#, true),
            ("lane_depths", false),
            (r#""worker_status":[]"#, true),
            (r#""state":"booting""#, true),
        ] {
            assert_eq!(json.contains(needle), present, "{needle} in {json}");
        }
    }

    /// A labeled series: its name, kind, label, and each label's sample.
    type Labeled = (
        &'static str,
        &'static str,
        &'static str,
        &'static [(&'static str, u64)],
    );

    /// That `text` has each series: a plain one's name, kind and sample, a labeled one's.
    fn has_series(text: &str, plain: &[(&str, &str, u64)], labeled: &[Labeled]) {
        let plain = plain
            .iter()
            .map(|(name, kind, value)| format!("# TYPE {name} {kind}\n{name} {value}\n"));
        let labeled = labeled.iter().map(|(name, kind, label, values)| {
            let sample =
                |(at, value): &(&str, u64)| format!("{name}{{{label}=\"{at}\"}} {value}\n");
            let samples: String = values.iter().map(sample).collect();
            format!("# TYPE {name} {kind}\n{samples}")
        });
        for series in plain.chain(labeled) {
            assert!(text.contains(&series), "expected\n{series}in:\n{text}");
        }
    }

    #[test]
    fn metrics_text_is_prometheus_shaped() {
        let text = snapshot().metrics();
        let samples = [
            ("kino_requests_served_total", "counter", 100),
            ("kino_ready", "gauge", 1),
            ("kino_max_workers", "gauge", 32),
            ("kino_active_workers", "gauge", 12),
            ("kino_scale_ups_total", "counter", 3),
            ("kino_scale_downs_total", "counter", 1),
            ("kino_quarantined_workers", "gauge", 1),
            ("kino_quarantine_replacements_total", "counter", 7),
            ("kino_wakes_total", "counter", 9),
            ("kino_hijacked_connections", "gauge", 2),
            ("kino_websockets_open", "gauge", 7),
            ("kino_websockets_opened_total", "counter", 1000),
            ("kino_websocket_messages_received_total", "counter", 41),
            ("kino_websocket_messages_sent_total", "counter", 43),
            ("kino_websocket_messages_skipped_total", "counter", 5),
            ("kino_websocket_writes_refused_total", "counter", 12),
            ("kino_websocket_publishes_refused_total", "counter", 1),
            ("kino_websocket_inbound_pauses_total", "counter", 13),
            ("kino_websocket_received_bytes_total", "counter", 4096),
            ("kino_websocket_sent_bytes_total", "counter", 8192),
        ];
        // One row per case; rustfmt would split each row field by field.
        #[rustfmt::skip]
        let labeled: [Labeled; 9] = [
            ("kino_responses_cut_total", "counter", "cause",
                &[("client_gone", 21), ("shutdown", 22), ("crash", 23)]),
            ("kino_websocket_handshakes_refused_total", "counter", "cause",
                &[("websocket_max", 11), ("draining", 14), ("worker_closing", 15)]),
            ("kino_websockets_closed_total", "counter", "cause",
                &[("normal", 100), ("going_away", 101), ("protocol", 102), ("too_big", 103),
                  ("slow", 104), ("dead_peer", 105), ("client_gone", 106), ("crash", 107),
                  ("handshake_failed", 108)]),
            ("kino_lane_depth", "gauge", "lane", &[("0", 2), ("1", 0)]),
            ("kino_worker_requests_served_total", "counter", "worker", &[("0", 10), ("1", 7)]),
            ("kino_worker_in_flight", "gauge", "worker", &[("0", 1), ("1", 0)]),
            ("kino_worker_busy_ms", "gauge", "worker", &[("0", 4), ("1", 0)]),
            ("kino_worker_websockets_open", "gauge", "worker", &[("0", 2), ("1", 0)]),
            ("kino_worker_hijacked_connections", "gauge", "worker", &[("0", 0), ("1", 3)]),
        ];
        has_series(&text, &samples, &labeled);
        // Cumulative: le=0.0025 holds buckets 0 to 2.
        let histogram = concat!(
            "# TYPE kino_request_queue_seconds histogram\n",
            "kino_request_queue_seconds_bucket{le=\"0.0005\"} 3\n",
            "kino_request_queue_seconds_bucket{le=\"0.001\"} 3\n",
            "kino_request_queue_seconds_bucket{le=\"0.0025\"} 4\n",
        );
        let total = concat!(
            "kino_request_queue_seconds_bucket{le=\"+Inf\"} 5\n",
            "kino_request_queue_seconds_sum 1.5\n",
            "kino_request_queue_seconds_count 5\n",
        );
        assert!(text.contains(histogram) && text.contains(total), "{text}");
        let draining = Snapshot {
            state: crate::registry::STATE_DRAINING,
            ..snapshot()
        };
        assert!(draining.metrics().contains("\nkino_ready 0\n"));
    }

    #[test]
    fn a_quarantined_worker_reads_as_idle() {
        let server = TestServer::shared_queue().workers(1).build();
        let wedged = slot(&server, 0);
        // In a request since the clock began, but given up on as wedged.
        wedged.load.in_flight.store(1, Relaxed);
        wedged.life.quarantined.store(true, Relaxed);

        let row = &Snapshot::take(&server).worker_status[0];
        assert_eq!((row.quarantined, row.busy_ms), (true, 0));
    }

    /// How long a worker_loop worker's thread stays away from its loop in
    /// these tests, past any wait on its doorbell.
    const AWAY: Duration = Duration::from_millis(100);

    fn busy_ms(server: &ServerInner) -> u64 {
        collect_worker_status(server)[0].busy_ms
    }

    #[test]
    fn a_worker_loop_worker_reads_busy_once_its_thread_stays_away_with_work_waiting() {
        use crate::queue::testing::{admit_next, nonblock, socket_sends};
        use crate::queue::Room::{All, Events, Requests};
        use crate::registry::BoxedCtx;
        type Leave = fn(&ServerInner, &Arc<crate::slot::WorkerSlot>) -> Option<BoxedCtx>;
        // How its thread left the loop, what came after, and whether it
        // reads busy once the thread stayed away past any wait.
        // One row per case; rustfmt would split each row field by field.
        #[rustfmt::skip]
        let rows: [(&str, Leave, bool); 8] = [
            ("to wait on its doorbell, nothing to run",
                |server, slot| { nonblock(server, slot, All); None }, false),
            ("with a request it took", |server, slot| { assign(slot); Some(admit_next(server, slot)) }, true),
            ("to wait on its doorbell, a request half a tick in",
                |server, slot| { assert_eq!(nonblock(server, slot, All), "nothing"); std::thread::sleep(TICK / 2); assign(slot); None }, true),
            ("to wait for events, an event half a tick in",
                |server, slot| { assert_eq!(nonblock(server, slot, Events), "nothing"); std::thread::sleep(TICK / 2); socket_sends(slot, 0); None }, true),
            ("to wait for requests, its wake hook due, its doorbell taken",
                |server, slot| { assert!(slot.wake()); nonblock(server, slot, Requests); None }, true),
            ("handed its end at the drain, then woken",
                |server, slot| { server.close_queues(); nonblock(server, slot, All); slot.wake(); None }, false),
            ("handed its end at its retirement, then woken",
                |server, slot| { slot.retire(); nonblock(server, slot, All); slot.wake(); None }, false),
            ("as its worker crashed, then woken",
                |_, slot| { slot.abort(crate::response::Cut::Crash); slot.wake(); None }, false),
        ];
        for (left, leave, busy) in rows {
            let server = TestServer::worker_loop().workers(1).build();
            let slot = slot(&server, 0);
            let left_at = Instant::now();
            let open = leave(&server, &slot);
            let read = busy_ms(&server);
            // Against the time that really passed: a loaded runner can
            // oversleep half a tick by more than half a tick.
            let grace = if open.is_some() { Duration::ZERO } else { TICK };
            let overdue = left_at.elapsed().saturating_sub(grace).as_millis() as u64;
            assert!(
                read <= overdue,
                "{left}: a wait on the doorbell is time in the loop until it is overdue: read {read} ms busy, at most {overdue} ms overdue"
            );

            std::thread::sleep(TICK + AWAY);

            let read = busy_ms(&server);
            assert_eq!(
                read >= AWAY.as_millis() as u64,
                busy,
                "a worker_loop worker whose thread left its loop {left}, {AWAY:?} past any wait, read {read} ms busy"
            );
        }
    }

    #[test]
    fn inline_serving_is_reported_in_metrics_and_json_only_by_a_server_that_serves_inline() {
        let mut s = snapshot();
        s.worker_status[1].inline_conns = 2;
        // 3 connections now, 10 requests served, 2 connections declined, 4 handed back and 1
        // cut off; 7 spins for a next request.
        s.inline = Some(InlineSnapshot {
            connections: 3,
            requests: 10,
            declined: 2,
            handbacks: 4,
            closed: 1,
            spins: 7,
        });
        let plain = [
            ("kino_inline_connections", "gauge", 3),
            ("kino_inline_requests_total", "counter", 10),
            ("kino_inline_declined_total", "counter", 2),
            ("kino_inline_handbacks_total", "counter", 4),
            ("kino_inline_closed_total", "counter", 1),
            ("kino_inline_spins_total", "counter", 7),
        ];
        let per_worker: Labeled = (
            "kino_worker_inline_connections",
            "gauge",
            "worker",
            &[("0", 0), ("1", 2)],
        );
        has_series(&s.metrics(), &plain, &[per_worker]);
        let json = s.json();
        let inline = concat!(
            r#""inline":{"connections":3,"requests":10,"declined":2,"handbacks":4,"#,
            r#""closed":1,"spins":7}"#
        );
        assert!(json.contains(inline), "expected {inline} in {json}");

        // None of it from a server that serves nothing inline.
        let text = snapshot().metrics();
        assert!(
            !text.contains("kino_inline_") && !text.contains("kino_worker_inline_"),
            "inline series without inline serving:\n{text}"
        );
        assert!(snapshot().json().contains(r#""inline":null"#));
    }
}
