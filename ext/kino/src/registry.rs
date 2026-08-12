//! Global server registry. Ruby never holds a pointer to native state:
//! workers receive plain integers (server id, worker id), both
//! Ractor-shareable, and every native call resolves them here. This is what
//! keeps TypedData objects from ever crossing a ractor boundary.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock, Weak};

use parking_lot::{Mutex, RwLock};

use crate::request::RequestCtx;
use crate::response::Responder;

/// Lifecycle as seen by the control plane's /ready.
pub const STATE_BOOTING: u8 = 0;
pub const STATE_READY: u8 = 1;
pub const STATE_DRAINING: u8 = 2;

/// Boot-time configuration echoed by /stats. Stored resolved: mode is
/// "ractor" or "threaded", never "auto".
pub struct Topology {
    pub mode: String,
    pub workers: usize,
    pub threads: usize,
    pub batch: usize,
}

/// Requests travel through channels boxed: one heap allocation at accept
/// time instead of moving ~300 bytes by value through every channel hop.
pub type BoxedCtx = Box<RequestCtx>;

/// Probed on every take; keys are our own ids, so ahash over SipHash.
type HashMap<K, V> = std::collections::HashMap<K, V, ahash::RandomState>;

/// One per `Kino::Server`. Owns the tokio runtime, the request queue and the
/// worker slots.
pub struct ServerInner {
    pub id: u64,
    /// Senders' side of the request queue. `close_queue` takes it; once all
    /// clones drop, blocked workers see Disconnected and exit their loops.
    pub req_tx: Mutex<Option<flume::Sender<BoxedCtx>>>,
    pub req_rx: flume::Receiver<BoxedCtx>,
    /// Signals the accept loop to stop. Watch channel: `true` = draining.
    pub shutdown_tx: tokio::sync::watch::Sender<bool>,
    /// Runtime is kept so we can shut it down explicitly; in an Option so
    /// `shutdown_runtime` can take ownership out of the Arc.
    pub runtime: Mutex<Option<tokio::runtime::Runtime>>,
    pub slots: RwLock<Vec<Arc<WorkerSlot>>>,
    pub in_flight: AtomicUsize,
    /// Requests handed to Ruby workers (admitted), and requests rejected
    /// with a 503 (queue full / draining). Relaxed: stats-only counters.
    pub served: AtomicU64,
    pub rejected: AtomicU64,
    pub queue_timeout_ms: u64,
    /// 0 = no request timeout; otherwise the response head must arrive
    /// within this many ms or the client gets a 504.
    pub request_timeout_ms: u64,
    /// 0 = unlimited; otherwise the max request-body bytes accepted before a
    /// 413 (truthful Content-Length) or a mid-stream abort (chunked/lying).
    pub max_body_size: usize,
    pub timeouts: AtomicU64,
    /// Lifecycle for /ready: booting until Ruby reports the workers up,
    /// draining once stop_accepting runs. Relaxed everywhere (advisory).
    pub state: std::sync::atomic::AtomicU8,
    /// Worker respawns, recorded from the Ruby supervisor. Lives here so
    /// the control plane reads it without touching Ruby.
    pub respawns: AtomicU64,
    /// Replacements spawned by the quarantine monitor (Relaxed, advisory).
    pub quarantine_replacements: AtomicU64,
    pub topology: Topology,
    pub https: bool,
    /// Native access log sink (None unless log_requests is on).
    pub access_log: Option<crate::logsink::Sink>,
    /// Lane-dispatch mode: per-worker queues, awake-preferring dispatch.
    pub lanes: bool,
    /// Round-robin cursor for lane dispatch.
    pub lane_cursor: AtomicUsize,
    /// GC roots for zero-copy response buffers (pin.rs). The Ruby Server
    /// object holds the marking PinKeeper for this slab.
    pub pin_slab: Arc<crate::pin::PinSlab>,
    /// Queue-wait histogram: recorded at admit (queue.rs), emitted by the
    /// control plane.
    pub queue_histogram: QueueHistogram,
}

/// One per worker *thread* (slot count = workers × threads). The interrupt
/// flag is the UBF target and the shutdown kick: blocking natives poll it
/// between bounded waits (flume::Selector proved to lose wakeups under
/// churn, so no select-style blocking anywhere). The in-flight list lets
/// the supervisor 500 every request a dead ractor was holding; workers
/// take requests in small batches, so there can be several.
pub struct WorkerSlot {
    pub interrupted: std::sync::atomic::AtomicBool,
    pub current: Mutex<smallvec::SmallVec<[Weak<Responder>; 8]>>,
    /// Lane mode only: this worker's private queue and its parked flag.
    /// The dispatcher prefers awake (non-parked) lanes so a hot worker
    /// keeps taking without ever paying the futex wake.
    pub lane_tx: Mutex<Option<flume::Sender<BoxedCtx>>>,
    pub lane_rx: Option<flume::Receiver<BoxedCtx>>,
    pub parked: std::sync::atomic::AtomicBool,
    /// Per-slot sensors (Relaxed, advisory). served/in_flight mirror the
    /// global counters at slot granularity; last_started_ms (stamped on
    /// admit) drives busy-age (wedge) reporting.
    pub served: AtomicU64,
    pub in_flight: AtomicUsize,
    pub last_started_ms: AtomicU64,
    /// Set by the quarantine monitor when this slot is abandoned as wedged:
    /// excluded from wedge detection, and its busy_ms is reported as 0.
    pub quarantined: std::sync::atomic::AtomicBool,
}

/// Per-lane depth cap: small, so a slow handler can only ever delay this
/// many queued neighbors (work stealing rescues them anyway).
pub const LANE_DEPTH: usize = 4;

/// Fixed queue-wait bucket boundaries in microseconds (0.5ms .. 10s),
/// ascending. Emitted in seconds. Not a knob (YAGNI).
pub const QUEUE_BOUNDS_US: [u64; 14] = [
    500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000,
    250_000, 500_000, 1_000_000, 2_500_000, 5_000_000, 10_000_000,
];

/// Queue-wait histogram: per-bucket counts plus an overflow (the implicit
/// +Inf bucket), the sum of waits, and the total count. Relaxed atomics,
/// advisory like the other counters.
pub struct QueueHistogram {
    pub buckets: [AtomicU64; QUEUE_BOUNDS_US.len()],
    pub overflow: AtomicU64,
    pub sum_us: AtomicU64,
    pub count: AtomicU64,
}

/// A plain (non-atomic) snapshot for the control thread to emit.
pub struct QueueHistogramSnapshot {
    pub buckets: [u64; QUEUE_BOUNDS_US.len()],
    pub overflow: u64,
    pub sum_us: u64,
    pub count: u64,
}

impl QueueHistogram {
    pub fn new() -> Self {
        QueueHistogram {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            overflow: AtomicU64::new(0),
            sum_us: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    /// Place one wait in its bucket (first bound >= wait, else overflow) and
    /// update sum and count. A linear scan over 14 bounds is trivial.
    pub fn record(&self, wait_us: u64) {
        match QUEUE_BOUNDS_US.iter().position(|&bound| wait_us <= bound) {
            Some(i) => self.buckets[i].fetch_add(1, Ordering::Relaxed),
            None => self.overflow.fetch_add(1, Ordering::Relaxed),
        };
        self.sum_us.fetch_add(wait_us, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> QueueHistogramSnapshot {
        QueueHistogramSnapshot {
            buckets: std::array::from_fn(|i| self.buckets[i].load(Ordering::Relaxed)),
            overflow: self.overflow.load(Ordering::Relaxed),
            sum_us: self.sum_us.load(Ordering::Relaxed),
            count: self.count.load(Ordering::Relaxed),
        }
    }
}

impl WorkerSlot {
    fn new(lanes: bool) -> Self {
        let (lane_tx, lane_rx) = if lanes {
            let (tx, rx) = flume::bounded(LANE_DEPTH);
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };
        WorkerSlot {
            interrupted: std::sync::atomic::AtomicBool::new(false),
            current: Mutex::new(smallvec::SmallVec::new()),
            lane_tx: Mutex::new(lane_tx),
            lane_rx,
            parked: std::sync::atomic::AtomicBool::new(false),
            served: AtomicU64::new(0),
            in_flight: AtomicUsize::new(0),
            last_started_ms: AtomicU64::new(0),
            quarantined: std::sync::atomic::AtomicBool::new(false),
        }
    }
}

static REGISTRY: OnceLock<RwLock<HashMap<u64, Arc<ServerInner>>>> = OnceLock::new();
static NEXT_SERVER_ID: AtomicU64 = AtomicU64::new(1);

fn registry() -> &'static RwLock<HashMap<u64, Arc<ServerInner>>> {
    REGISTRY.get_or_init(|| RwLock::new(HashMap::default()))
}

pub fn next_server_id() -> u64 {
    NEXT_SERVER_ID.fetch_add(1, Ordering::Relaxed)
}

pub fn insert(server: Arc<ServerInner>) {
    registry().write().insert(server.id, server);
}

pub fn remove(id: u64) -> Option<Arc<ServerInner>> {
    registry().write().remove(&id)
}

pub fn get(ruby: &magnus::Ruby, id: u64) -> Result<Arc<ServerInner>, magnus::Error> {
    registry().read().get(&id).cloned().ok_or_else(|| {
        magnus::Error::new(ruby.exception_arg_error(), format!("unknown server {id}"))
    })
}

/// Tolerant lookup for lifecycle paths: a worker waking up after teardown
/// must see "server gone" as a clean shutdown signal, not an exception.
pub fn try_get(id: u64) -> Option<Arc<ServerInner>> {
    registry().read().get(&id).cloned()
}

impl ServerInner {
    /// Per-lane queue depths; None unless lane dispatch is on.
    pub fn lane_depths(&self) -> Option<Vec<usize>> {
        if !self.lanes {
            return None;
        }
        Some(
            self.slots
                .read()
                .iter()
                .filter_map(|s| s.lane_rx.as_ref().map(|rx| rx.len()))
                .collect(),
        )
    }

    /// Requests waiting anywhere: the global queue plus any open lanes.
    pub fn queued(&self) -> usize {
        self.req_rx.len() + self.lane_depths().map_or(0, |d| d.iter().sum())
    }

    pub fn register_worker(&self) -> usize {
        let mut slots = self.slots.write();
        slots.push(Arc::new(WorkerSlot::new(self.lanes)));
        slots.len() - 1
    }

    pub fn slot(
        &self,
        ruby: &magnus::Ruby,
        worker_id: usize,
    ) -> Result<Arc<WorkerSlot>, magnus::Error> {
        self.slots.read().get(worker_id).cloned().ok_or_else(|| {
            magnus::Error::new(
                ruby.exception_arg_error(),
                format!("unknown worker {worker_id}"),
            )
        })
    }
}

/// A ServerInner with no runtime and no registry entry, for pure-Rust
/// tests of queue accounting and dispatch. Ids are unique so tests that
/// do insert into the global registry can run in parallel.
#[cfg(test)]
pub fn test_server(lanes: bool, queue_depth: usize) -> Arc<ServerInner> {
    let (req_tx, req_rx) = flume::bounded(queue_depth);
    let (shutdown_tx, _shutdown_rx) = tokio::sync::watch::channel(false);
    Arc::new(ServerInner {
        id: next_server_id(),
        req_tx: Mutex::new(Some(req_tx)),
        req_rx,
        shutdown_tx,
        runtime: Mutex::new(None),
        slots: RwLock::new(Vec::new()),
        in_flight: AtomicUsize::new(0),
        served: AtomicU64::new(0),
        rejected: AtomicU64::new(0),
        queue_timeout_ms: 10,
        request_timeout_ms: 0,
        max_body_size: 0,
        timeouts: AtomicU64::new(0),
        state: std::sync::atomic::AtomicU8::new(STATE_BOOTING),
        respawns: AtomicU64::new(0),
        quarantine_replacements: AtomicU64::new(0),
        topology: Topology { mode: "threaded".to_string(), workers: 0, threads: 0, batch: 1 },
        https: false,
        access_log: None,
        lanes,
        lane_cursor: AtomicUsize::new(0),
        pin_slab: Arc::new(crate::pin::PinSlab::new()),
        queue_histogram: QueueHistogram::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::request::test_ctx;

    #[test]
    fn worker_registration_hands_out_sequential_slot_ids() {
        let server = test_server(false, 4);

        assert_eq!(server.register_worker(), 0);
        assert_eq!(server.register_worker(), 1);
        assert_eq!(server.slots.read().len(), 2);
        // Shared-queue mode creates no lane channels.
        assert!(server.slots.read()[0].lane_rx.is_none());
    }

    #[test]
    fn lane_mode_slots_get_bounded_lanes() {
        let server = test_server(true, 4);
        server.register_worker();

        let slots = server.slots.read();
        let lane_rx = slots[0].lane_rx.as_ref().expect("lane created");
        assert_eq!(lane_rx.capacity(), Some(LANE_DEPTH));
        assert!(slots[0].lane_tx.lock().is_some());
    }

    #[test]
    fn queued_counts_the_shared_queue() {
        let server = test_server(false, 4);
        assert_eq!(server.queued(), 0);

        let tx = server.req_tx.lock().clone().expect("queue open");
        tx.send(test_ctx()).expect("queue has room");
        tx.send(test_ctx()).expect("queue has room");

        assert_eq!(server.queued(), 2);
        assert!(server.lane_depths().is_none());
    }

    #[test]
    fn queued_includes_lanes_and_lane_depths_reports_per_slot() {
        let server = test_server(true, 4);
        server.register_worker();
        server.register_worker();

        let slots = server.slots.read();
        let lane0 = slots[0].lane_tx.lock().clone().expect("lane open");
        lane0.send(test_ctx()).expect("lane has room");
        drop(slots);

        assert_eq!(server.lane_depths(), Some(vec![1, 0]));
        assert_eq!(server.queued(), 1);
    }

    #[test]
    fn registry_lifecycle_insert_lookup_remove() {
        let server = test_server(false, 1);
        let id = server.id;

        assert!(try_get(id).is_none());
        insert(server);
        assert!(try_get(id).is_some());

        let removed = remove(id).expect("was registered");
        assert_eq!(removed.id, id);
        // Late wakers see "gone" as a clean shutdown signal, not a panic.
        assert!(try_get(id).is_none());
        assert!(remove(id).is_none());
    }

    #[test]
    fn server_ids_are_unique() {
        let a = next_server_id();
        let b = next_server_id();
        assert_ne!(a, b);
    }

    #[test]
    fn servers_boot_in_the_booting_state_with_zero_respawns() {
        let server = test_server(false, 4);
        assert_eq!(server.state.load(Ordering::Relaxed), STATE_BOOTING);
        assert_eq!(server.respawns.load(Ordering::Relaxed), 0);
        assert_eq!(server.topology.batch, 1);
    }

    #[test]
    fn fresh_slot_has_zeroed_per_worker_sensors() {
        let server = test_server(false, 4);
        server.register_worker();
        let slots = server.slots.read();
        let slot = &slots[0];
        assert_eq!(slot.served.load(Ordering::Relaxed), 0);
        assert_eq!(slot.in_flight.load(Ordering::Relaxed), 0);
        assert_eq!(slot.last_started_ms.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn fresh_slot_is_not_quarantined() {
        let server = test_server(false, 4);
        server.register_worker();
        assert!(!server.slots.read()[0].quarantined.load(Ordering::Relaxed));
        assert_eq!(server.quarantine_replacements.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn queue_histogram_buckets_by_wait() {
        let h = QueueHistogram::new();
        h.record(400);        // <= 500 -> bucket 0
        h.record(500);        // == 500 -> bucket 0 (inclusive)
        h.record(600);        // (500, 1000] -> bucket 1
        h.record(20_000_000); // > last bound -> overflow
        let s = h.snapshot();
        assert_eq!(s.buckets[0], 2);
        assert_eq!(s.buckets[1], 1);
        assert_eq!(s.overflow, 1);
        assert_eq!(s.count, 4);
        assert_eq!(s.sum_us, 400 + 500 + 600 + 20_000_000);
    }
}
