//! Global server registry. Ruby never holds a pointer to native state:
//! workers receive plain integers (server id, worker id), both
//! Ractor-shareable, and every native call resolves them here. This is what
//! keeps TypedData objects from ever crossing a ractor boundary.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};

use crate::doorbell::Doorbell;
use crate::request::RequestCtx;
use crate::server::ServerConfig;
use crate::slot::{WorkerSlot, WS_INBOX};

/// Lifecycle as seen by the control plane's /ready.
pub const STATE_BOOTING: u8 = 0;
pub const STATE_READY: u8 = 1;
pub const STATE_DRAINING: u8 = 2;

/// Requests travel through channels boxed: one heap allocation at accept
/// time instead of moving ~300 bytes by value through every channel hop.
pub type BoxedCtx = Box<RequestCtx>;

/// A request, or a doorbell: the worker's WebSocket inbox has events (WorkerSlot::notify).
pub enum LaneItem {
    Request(BoxedCtx),
    Bell,
}

/// Probed on every take; keys are our own ids, so ahash over SipHash.
type HashMap<K, V> = std::collections::HashMap<K, V, ahash::RandomState>;

/// What runs the server's I/O, taken out at shutdown. The default
/// multi-thread runtime and sharded I/O carry different teardown state,
/// so the shape stays explicit instead of parallel optional fields.
#[derive(Default)]
pub enum RuntimeHandle {
    /// Not started yet, or already shut down.
    #[default]
    None,
    MultiThread(tokio::runtime::Runtime),
    /// The shards' final-teardown signal and every I/O thread (shards plus
    /// the acceptor).
    Shards {
        shutdown_tx: tokio::sync::watch::Sender<bool>,
        threads: Vec<std::thread::JoinHandle<()>>,
    },
}

impl RuntimeHandle {
    /// Stop the I/O side, giving in-flight work `timeout` to finish. A
    /// thread that misses the deadline is abandoned rather than joined:
    /// a wedged shard must not hang `Server#shutdown`, which promises to
    /// return by its deadline.
    pub fn shutdown(self, timeout: Duration) {
        match self {
            RuntimeHandle::None => {}
            RuntimeHandle::MultiThread(runtime) => runtime.shutdown_timeout(timeout),
            RuntimeHandle::Shards {
                shutdown_tx,
                threads,
            } => {
                let _ = shutdown_tx.send(true);
                let deadline = Instant::now() + timeout;
                for thread in threads {
                    while !thread.is_finished() && Instant::now() < deadline {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    if thread.is_finished() {
                        let _ = thread.join();
                    }
                }
            }
        }
    }
}

/// One per `Kino::Server`. Owns the tokio runtime, the request queue and the
/// worker slots.
pub struct ServerInner {
    pub id: u64,
    pub config: ServerConfig,
    pub counters: Counters,
    /// Senders' side of the request queue. `close_queue` takes it; once all
    /// clones drop, blocked workers see Disconnected and exit their loops.
    pub req_tx: Mutex<Option<flume::Sender<BoxedCtx>>>,
    pub req_rx: flume::Receiver<BoxedCtx>,
    /// Signals the accept loop to stop. Watch channel: `true` = draining.
    pub shutdown_tx: tokio::sync::watch::Sender<bool>,
    /// Runtime is kept so we can shut it down explicitly; `shutdown_runtime`
    /// takes ownership out of the Arc.
    pub runtime: Mutex<RuntimeHandle>,
    pub slots: RwLock<Vec<Arc<WorkerSlot>>>,
    /// Lifecycle for /ready: booting until Ruby reports the workers up,
    /// draining once stop_accepting runs. Relaxed everywhere (advisory).
    pub state: std::sync::atomic::AtomicU8,
    /// Native access log sink (None unless log_requests is on).
    pub access_log: Option<crate::logsink::Sink>,
    /// Round-robin cursor for lane dispatch.
    pub lane_cursor: AtomicUsize,
    /// Lane dispatch: stamps each park, so the dispatcher can tell which
    /// idle worker parked last (see Park::parked_at).
    pub park_seq: AtomicU64,
    pub lane_room: crate::lane_room::LaneRoom,
    pub ticker: Mutex<Option<crate::ticker::Ticker>>,
    /// GC roots for zero-copy response buffers (pin.rs). The Ruby Server
    /// object holds the marking PinKeeper for this slab.
    pub pin_slab: Arc<crate::pin::PinSlab>,
    /// Warnings printed once per server (Worker#claim_warning).
    pub warned: Mutex<std::collections::HashSet<String>>,
    /// How far the drain has closed hijacked connections (hijack::Closed).
    pub bridges_closed: tokio::sync::watch::Sender<crate::hijack::Closed>,
    /// Counted until hyper has taken each bridge's answer; close_bridges waits for them.
    pub bridges_running: Arc<AtomicUsize>,
}

/// Relaxed and advisory: the control plane reads them without Ruby.
#[derive(Default)]
pub struct Counters {
    pub in_flight: AtomicUsize,
    /// Requests handed to workers, and 503s (queue full, or draining).
    pub served: AtomicU64,
    pub rejected: AtomicU64,
    pub timeouts: AtomicU64,
    pub respawns: AtomicU64,
    pub quarantine_replacements: AtomicU64,
    /// Workers staying in the pool (not retiring), as the Ruby pool reports; starts at the floor.
    pub active_workers: AtomicUsize,
    pub scale_ups: AtomicU64,
    pub scale_downs: AtomicU64,
    pub wakes: AtomicU64,
    pub queue_histogram: QueueHistogram,
}

/// Fixed queue-wait bucket boundaries in microseconds (0.5ms .. 10s),
/// ascending. Emitted in seconds. Not a knob (YAGNI).
pub const QUEUE_BOUNDS_US: [u64; 14] = [
    500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000, 1_000_000,
    2_500_000, 5_000_000, 10_000_000,
];

/// Queue-wait histogram: per-bucket counts plus an overflow (the implicit
/// +Inf bucket), the sum of waits, and the total count. Relaxed atomics,
/// advisory like the other counters.
#[derive(Default)]
pub struct QueueHistogram {
    pub buckets: [AtomicU64; QUEUE_BOUNDS_US.len()],
    pub overflow: AtomicU64,
    pub sum_us: AtomicU64,
    pub count: AtomicU64,
}

/// A plain (non-atomic) snapshot for the control thread to emit.
#[cfg_attr(test, derive(Default))]
pub struct QueueHistogramSnapshot {
    pub buckets: [u64; QUEUE_BOUNDS_US.len()],
    pub overflow: u64,
    pub sum_us: u64,
    pub count: u64,
}

impl QueueHistogram {
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

impl QueueHistogramSnapshot {
    /// Summed queue wait, converted from the stored microseconds to seconds.
    pub fn sum_seconds(&self) -> f64 {
        self.sum_us as f64 / 1_000_000.0
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
    /// A booting server, not registered yet, with no runtime and no slots.
    pub fn new(config: ServerConfig) -> Self {
        let (req_tx, req_rx) = flume::bounded(config.queue_depth);
        // The accept loop subscribes once there is one.
        let (shutdown_tx, _) = tokio::sync::watch::channel(false);
        ServerInner {
            id: next_server_id(),
            counters: Counters {
                active_workers: AtomicUsize::new(config.topology.workers),
                ..Counters::default()
            },
            req_tx: Mutex::new(Some(req_tx)),
            req_rx,
            shutdown_tx,
            runtime: Mutex::default(),
            slots: RwLock::default(),
            state: std::sync::atomic::AtomicU8::new(STATE_BOOTING),
            access_log: config
                .log_requests
                .then(|| crate::logsink::Sink::new(std::io::stdout())),
            lane_cursor: AtomicUsize::new(0),
            park_seq: AtomicU64::new(0),
            lane_room: Default::default(),
            ticker: Mutex::new(None),
            pin_slab: Arc::new(crate::pin::PinSlab::for_responses(
                config.topology.mode == "ractor",
            )),
            warned: Mutex::default(),
            bridges_closed: tokio::sync::watch::Sender::new(crate::hijack::Closed::Nothing),
            bridges_running: Arc::default(),
            config,
        }
    }

    /// Requests in each lane (None without lanes).
    pub fn lane_depths(&self) -> Option<Vec<usize>> {
        if !self.config.lanes {
            return None;
        }
        let slots = self.slots.read();
        Some(slots.iter().filter_map(|s| s.lane.requests()).collect())
    }

    /// Requests waiting anywhere: the global queue, any open lanes, the line for lane room.
    pub fn queued(&self) -> usize {
        let lanes = self.lane_depths().map_or(0, |d| d.iter().sum::<usize>());
        self.req_rx.len() + lanes + self.lane_room.waiting()
    }

    /// The drain is over: every queue closes, and requests waiting for lane room find it so.
    pub fn close_queues(&self) {
        self.req_tx.lock().take();
        for slot in self.slots.read().iter() {
            slot.close_lane();
        }
        self.lane_room.notify();
    }

    /// WebSockets whose on_close some worker of this server is still owed.
    pub fn open_sockets(&self) -> usize {
        self.slots
            .read()
            .iter()
            .map(|slot| slot.sockets.open())
            .sum()
    }

    /// One registered after close_queue (a late respawn) gets a closed lane, so its worker leaves.
    /// Fails only for a worker_loop slot whose doorbell cannot be made (out of descriptors).
    pub fn register_worker(&self) -> std::io::Result<usize> {
        let doorbell = self.config.worker_loop.then(Doorbell::new).transpose()?;
        let mut slots = self.slots.write();
        let slot = WorkerSlot::new(self.config.lanes, WS_INBOX, doorbell);
        if self.req_tx.lock().is_none() {
            slot.close_lane();
        }
        slots.push(Arc::new(slot));
        Ok(slots.len() - 1)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::request::test_ctx;
    use crate::testing::{assign, slot, TestServer};

    #[test]
    fn shards_shutdown_joins_threads_that_observe_the_signal() {
        let (shutdown_tx, rx) = tokio::sync::watch::channel(false);
        let thread = std::thread::spawn(move || {
            while !*rx.borrow() {
                std::thread::sleep(Duration::from_millis(1));
            }
        });

        let start = Instant::now();
        RuntimeHandle::Shards {
            shutdown_tx,
            threads: vec![thread],
        }
        .shutdown(Duration::from_secs(5));

        // The thread exits on the signal, so the join comes nowhere near
        // the deadline.
        assert!(start.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn shards_shutdown_abandons_a_wedged_thread_at_the_deadline() {
        let (shutdown_tx, _rx) = tokio::sync::watch::channel(false);
        let wedged = std::thread::spawn(|| std::thread::sleep(Duration::from_secs(30)));

        let start = Instant::now();
        RuntimeHandle::Shards {
            shutdown_tx,
            threads: vec![wedged],
        }
        .shutdown(Duration::from_millis(50));

        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(50));
        assert!(elapsed < Duration::from_secs(5));
    }

    #[test]
    fn a_slot_registered_once_the_queue_closed_has_its_lane_closed_too() {
        let server = TestServer::lanes().workers(1).build();
        server.req_tx.lock().take(); // close_queue
        slot(&server, 0).lane.tx.lock().take();

        server.register_worker().expect("a worker slot");

        let lane = slot(&server, 1).lane.rx.clone().expect("lane");
        let why = "a worker that comes up during shutdown must see its lane closed and leave";
        assert!(lane.is_disconnected(), "{why}");
    }

    #[test]
    fn queued_counts_the_requests_in_the_shared_queue_or_the_lanes_not_their_doorbells() {
        let shared = TestServer::shared_queue().build();
        let tx = shared.req_tx.lock().clone().expect("queue open");
        for _ in 0..2 {
            tx.send(test_ctx()).expect("queue has room");
        }
        assert_eq!((shared.queued(), shared.lane_depths()), (2, None));

        let lanes = TestServer::lanes().workers(2).build();
        assign(&slot(&lanes, 0));
        assert!(slot(&lanes, 1).wake(), "a doorbell in the other lane");
        assert_eq!(lanes.lane_depths(), Some(vec![1, 0]));
        assert_eq!(
            lanes.queued(),
            1,
            "the pool scaler must not see a worker's own events as queue pressure"
        );
    }

    #[test]
    fn registry_lifecycle_insert_lookup_remove() {
        let server = TestServer::shared_queue().build();
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
    fn queue_histogram_buckets_by_wait() {
        let h = QueueHistogram::default();
        h.record(400); // <= 500 -> bucket 0
        h.record(500); // == 500 -> bucket 0 (inclusive)
        h.record(600); // (500, 1000] -> bucket 1
        h.record(20_000_000); // > last bound -> overflow
        let s = h.snapshot();
        assert_eq!(s.buckets[0], 2);
        assert_eq!(s.buckets[1], 1);
        assert_eq!(s.overflow, 1);
        assert_eq!(s.count, 4);
        assert_eq!(s.sum_us, 400 + 500 + 600 + 20_000_000);
    }
}
