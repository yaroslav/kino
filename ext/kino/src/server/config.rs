//! What a server is built from: the boot-config Hash Ruby hands server_start, read once.

use std::sync::Arc;
use std::time::Duration;

use magnus::{Error, RHash, Ruby, TryConvert};

/// Echoed by /stats; `mode` is resolved: "ractor" or "threaded", never "auto".
#[derive(Clone)]
#[cfg_attr(test, derive(Default))]
pub struct Topology {
    pub mode: String,
    /// The pool floor.
    pub workers: usize,
    /// The pool ceiling (`workers` for a fixed pool), which HTTP/2 admission is advertised from.
    pub max_workers: usize,
    pub threads: usize,
    pub batch: usize,
}

#[cfg_attr(test, derive(Default))]
pub struct ServerConfig {
    pub bind: String,
    pub port: u16,
    pub queue_depth: usize,
    pub queue_timeout_ms: u64,
    /// 0: none; else the head must arrive within this many ms or the client gets a 504.
    pub request_timeout_ms: u64,
    /// 0: unlimited; else a 413 for a truthful Content-Length, or a mid-stream abort.
    pub max_body_size: usize,
    pub max_connections: usize,
    pub io_shards: bool,
    pub io_threads: usize,
    pub tokio_threads: usize,
    /// The TLS cert and key, both or neither.
    pub tls: Option<(String, String)>,
    pub lanes: bool,
    /// Each worker runs its own loop under the app's fiber scheduler (lanes only).
    pub worker_loop: bool,
    /// Worker threads share a GVL (:threaded mode, or `threads` past one in a Ractor), which each
    /// passes between items once it has held it long (shared_gvl.rs).
    pub shared_gvl: bool,
    /// False pins every connection to HTTP/1.
    pub http2: bool,
    /// Serve plain HTTP/1.1 connections on worker_loop workers' threads (inline/), as resolved.
    pub inline: bool,
    /// An inline connection alone on its worker may spin at its socket (inline/spin.rs); Ruby
    /// turns it off on a process that may use one CPU.
    pub inline_spin: bool,
    pub log_requests: bool,
    pub topology: Topology,
    pub ws: Arc<crate::ws::Limits>,
    pub control_bind: Option<String>,
    pub control_token: Option<String>,
    /// The socket file of a `unix://` bind, removed at shutdown; set by binding, not a key.
    pub unix_path: Option<std::path::PathBuf>,
}

impl TryFrom<RHash> for ServerConfig {
    type Error = Error;

    /// A missing key or half a TLS pair is an ArgumentError; a wrong type raises in its conversion.
    fn try_from(hash: RHash) -> Result<Self, Error> {
        let mut config = ServerConfig {
            bind: required(hash, "bind")?,
            port: required(hash, "port")?,
            queue_depth: required(hash, "queue_depth")?,
            queue_timeout_ms: required(hash, "queue_timeout_ms")?,
            request_timeout_ms: optional(hash, "request_timeout_ms")?.unwrap_or(0),
            max_body_size: optional(hash, "max_body_size")?.unwrap_or(0),
            max_connections: optional(hash, "max_connections")?.unwrap_or(1024),
            io_shards: optional(hash, "io_shards")?.unwrap_or(false),
            io_threads: optional(hash, "io_threads")?.unwrap_or(0),
            tokio_threads: optional(hash, "tokio_threads")?.unwrap_or(0),
            tls: match (optional(hash, "tls_cert")?, optional(hash, "tls_key")?) {
                (Some(cert), Some(key)) => Some((cert, key)),
                (None, None) => None,
                _ => return Err(arg_error(hash, "TLS requires both cert and key")),
            },
            lanes: optional(hash, "lanes")?.unwrap_or(false),
            worker_loop: optional(hash, "worker_loop")?.unwrap_or(false),
            shared_gvl: false,
            // Default true guards embedders calling the native layer directly.
            http2: optional(hash, "http2")?.unwrap_or(true),
            inline: optional(hash, "inline")?.unwrap_or(true),
            inline_spin: optional(hash, "inline_spin")?.unwrap_or(false),
            log_requests: optional(hash, "log_requests")?.unwrap_or(false),
            topology: Topology {
                mode: optional(hash, "mode")?.unwrap_or_else(|| "threaded".to_string()),
                workers: optional(hash, "workers")?.unwrap_or(0),
                max_workers: optional(hash, "max_workers")?.unwrap_or(0),
                threads: optional(hash, "threads")?.unwrap_or(0),
                batch: optional(hash, "batch")?.unwrap_or(1),
            },
            ws: Arc::new(crate::ws::Limits {
                max_message: optional(hash, "websocket_max_message")?
                    .unwrap_or(crate::ws::DEFAULT_MAX_MESSAGE),
                ping_interval: optional(hash, "websocket_ping_interval_ms")?
                    .map_or(crate::ws::DEFAULT_PING_INTERVAL, Duration::from_millis),
                max: optional(hash, "websocket_max")?.unwrap_or(0),
                ..crate::ws::Limits::default()
            }),
            control_bind: optional(hash, "control_bind")?,
            control_token: optional(hash, "control_token")?,
            unix_path: None,
        };
        // Ruby refuses this up front: a queue of none would turn every request away.
        if config.queue_depth == 0 {
            let error = "queue_depth must be a positive Integer (got 0)";
            return Err(arg_error(hash, error));
        }
        // Guards embedders calling the native layer directly; Ruby refuses this up front.
        if config.worker_loop && !config.lanes {
            return Err(arg_error(hash, "worker_loop needs lanes"));
        }
        // Absent or below the floor means a fixed pool.
        let topology = &mut config.topology;
        topology.max_workers = topology.max_workers.max(topology.workers);
        config.shared_gvl = topology.mode == "threaded" || topology.threads > 1;
        // TLS serves through hyper alone.
        config.inline &= config.worker_loop && config.tls.is_none();
        Ok(config)
    }
}

fn required<T: TryConvert>(hash: RHash, key: &str) -> Result<T, Error> {
    let missing = || arg_error(hash, format!("server_start: missing config key :{key}"));
    optional(hash, key)?.ok_or_else(missing)
}

fn arg_error(hash: RHash, message: impl Into<std::borrow::Cow<'static, str>>) -> Error {
    Error::new(Ruby::get_with(hash).exception_arg_error(), message)
}

/// Absent or nil: None.
fn optional<T: TryConvert>(hash: RHash, key: &str) -> Result<Option<T>, Error> {
    hash.lookup(Ruby::get_with(hash).to_symbol(key))
}
