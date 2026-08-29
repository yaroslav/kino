## Unreleased

- Add opt-in `io_shards true`: accepted HTTP connections can run on
  current-thread Tokio I/O shards instead of Tokio's shared multi-thread
  runtime, reducing scheduler contention on very fast handlers. `io_threads`
  sets the shard count; otherwise Kino uses half the available CPUs
  ([Patrik Wenger](https://github.com/paddor)).

## [0.4.0] - 2026-08-22

- Rack handler: `rails server -u kino` and `rackup -s kino` boot Kino
  through `Rackup::Handler::Kino`, reading the same config file as the
  `kino` CLI with the host's flags on top; `rackup -s kino --help` lists
  the `-O` options (Workers, Threads, Mode, Config).
- The config file is also looked up at `config/kino.rb` (the Rails
  layout) when there is no `kino.rb`, by the CLI and the handler alike.
- `Kino::Server#run` serves an already built server the way
  `Kino::Server.run` does (banner, signal traps, block until shutdown),
  and syncs stdout there so the banner is never held back by block
  buffering under a pipe, whichever entry point booted the server.
- `workers` now defaults to `Kino.available_parallelism`, the CPUs the
  process may actually use: the affinity mask and, in a container, the
  cgroup CPU quota (a pod limited to 2 CPUs on a 64-core node gets 2
  workers, not 64). `Etc.nprocessors` only ever saw the mask.
- `bind "unix:///path/to.sock"` listens on a unix domain socket, the
  usual shape behind nginx: a stale socket file is reclaimed, a live one
  is refused, and the file is removed on shutdown. `port` is unused on
  it and TLS is rejected (terminate TLS at the proxy). Requests arriving
  over the socket report `REMOTE_ADDR` 127.0.0.1.
- `Kino::Server#url`, `#control_url`, and `#unix?` report where a started
  server and its control plane listen.
- The access log is two records per request: an arrival line queued
  before the app runs (a hang shows as an arrow with no answer) and a
  status-tinted completion line with a timing breakdown of `ruby`,
  `kino`, and `wait`, the `ruby` part carrying the GC pause and objects
  allocated where one request at a time can own the VM's counters
  (`:threaded`, or `:ractor` with `workers 1`). Local timestamps with
  their UTC offset; a blank line between requests. The former one-line
  format is gone.
- A failed request is reported as `500 GET /path · Class: message (site)`
  followed by its backtrace relative to the working directory, the app's
  own frames first, the rest folded into `… N more`.
- Every line Kino logs about itself (draining, a crash and its respawn,
  hook failures, quarantine, the USR1 stats line, `rack.errors`) reads
  `kino[<pid>] <source>: message`, the source naming the worker that
  spoke (`worker-3`, `worker-3/thread-2`) or `main`; worker ractors and
  threads now carry those names. The label is dim, yellow, or red by
  level on color terminals. `Kino::Log.info`, `.warn`, and `.error` are
  public, for hooks.
- The startup banner lists the Ruby build with its JIT and parser flags,
  the environment, the topology, the pid, and the control-plane address
  when one is bound.

## [0.3.0] - 2026-08-13

- Queue-time histogram: `/metrics` exposes `kino_request_queue_seconds`, a
  histogram of how long each request waited for a free worker (the
  saturation signal), and `server.stats`/`/stats` gain `queue_time`
  (count and summed seconds). Measured internally with a monotonic clock,
  so it needs no proxy header and is immune to clock skew.
- Lifecycle hooks: `after_boot`, `after_worker_boot`,
  `after_request_complete`, and `on_worker_exit` join `on_error`, so apps
  can wire their own metrics, readiness, and error tracking. The
  worker-context hooks must be Ractor-shareable in `:ractor` mode; a raising
  hook is logged and never kills a worker.
- Stuck-worker quarantine: past `quarantine_timeout`, a wedged dispatch
  slot is quarantined and a replacement worker is spawned to restore
  capacity (capped by `quarantine_max`), surfaced via `/stats`, `/metrics`,
  and `server.stats`. The wedged worker is never force-killed.
- Control plane: a read-only monitoring listener (`control_bind`,
  optional `control_token`) serving live stats as JSON at `/stats`,
  Prometheus metrics at `/metrics`, and `/ready`/`/live` probes, answered
  from the native layer on a dedicated thread so it stays responsive
  while workers are busy, stuck, or draining.
- Per-worker stats: `/stats`, `/metrics`, and `server.stats` now break the
  counters down per dispatch slot (served, in-flight, and `busy_ms`, the
  age of the slot's current request), so a stuck slot is visible
  individually.
- Update Rust and Ruby dependencies.

## [0.2.1] - 2026-07-27

- Update Rust dependencies for Kino.
- Update: puma 8 in benchmarks.

## [0.2.0] - 2026-07-13

- Strip debug info from release builds.
- A panic in the native layer now raises a RuntimeError on the affected
  worker (visible to `on_error` and the error log) instead of killing the
  server process.
- The pidfile is claimed exclusively: starting refuses (instead of silently
  overwriting) while the pidfile's owner is alive, a leftover file from a
  dead process is replaced, symlinks are never followed, and shutdown
  removes the file only while it still holds our pid.
- Zero-copy response bodies: bodies of 4 KB and up ride to the network
  layer by reference instead of being copied at the FFI boundary, in both
  dispatch modes. A 10 KB-body endpoint now serves at plaintext speed.

## [0.1.3] - 2026-07-04

- Non-String response header names and values (booleans, numbers, symbols)
  are now serialized with `to_s`, matching Puma, instead of failing the
  request with a `TypeError`. (Fixes [#3], reported by Max Erkin @rus-max)
- New `on_error` directive: a callable invoked with `(exception, env)` when
  a worker catches an app or delivery error, after the client got its 500.
  Delivery errors (unserializable header, a body that raised mid-stream)
  happen after the middleware stack returned, so this hook is the only
  place an error tracker can see them. Handler failures are logged and
  swallowed; in :ractor mode the handler must be Ractor-shareable. (Fixes [#3], reported by Max Erkin @rus-max)
- Worker error log lines now include the exception backtrace (first 12
  frames), not just the exception class and message.
- A streaming body that raises mid-stream now aborts the connection
  instead of finishing the chunked response cleanly, so a client can no
  longer mistake a truncated response for a complete one.

## [0.1.2] - 2026-06-22

- Drop a connection that has not sent its complete request headers
  within 15 seconds. Closes a slowloris hole: hyper's built-in header-read
  timeout was inert because the server installed no timer, so a slow-header
  client could tie up a connection (and its tokio task) indefinitely.
- Cap concurrent connections (new `max_connections` directive). Past the cap,
  new connections wait in the kernel backlog instead of piling up until a
  flood exhausts file descriptors or memory. Defaults to most of the process
  open-file limit (`ulimit -n`), so it scales with the OS limit and only
  engages under a flood.
- Bound the TLS handshake to 10 seconds. A client that completed the TCP
  connect but stalled the handshake could otherwise hold a connection slot
  indefinitely, since the request and header-read deadlines only begin once
  the handshake finishes.
- Cap the request body at 50 MB by default (new `max_body_size` directive,
  configurable; nil or 0 disables and delegates to a fronting proxy). An app
  that reads `rack.input` could otherwise be driven to run out of memory by an
  oversized or endless upload. A truthful oversize Content-Length is refused
  with a 413 before the app runs; a chunked or lying client is cut off
  mid-stream once it passes the cap.
- Bound the idle time between request-body frames to 30 seconds. A client that
  began a request then stalled mid-body would otherwise keep a worker blocked
  in `rack.input.read` indefinitely; now the read raises and the worker
  reclaims its slot. Only a silent client trips it: a steadily-sent body resets
  the deadline each frame, so slow-but-active uploads are unaffected.

## [0.1.1] - 2026-06-11

- Mode-dependent `threads` default: 1 per worker in :ractor mode (threads
  inside a ractor share its lock and cost a per-request handoff; +16-18%
  on fast handlers, measured on dedicated hardware), 3 in :threaded mode.
  Explicit `threads` always wins; waiting-heavy ractor apps should raise
  `workers` instead.
- `queue_timeout` default raised from 1 to 5 seconds: a brief burst now
  waits out the spike instead of shedding 503s within a second.

## [0.1.0] - 2026-06-11

Initial release.

- HTTP/1.1 server with all network I/O in Rust on tokio + hyper.
- Worker **Ractors** for true parallel request handling (`mode: :ractor`,
  requires a Ractor-shareable app), with a threaded fallback (`mode: :threaded`)
  that runs any Rack app, Rails included. Puma-style `workers × threads`
  topology in both modes.
- Rack 3 spec compliance verified by Rack::Lint over real sockets: streaming
  request bodies (forward-only `rack.input`), enumerable and callable
  (full-duplex stream) response bodies, lowercase/multi-value headers.
- Supervised crash recovery: a dying ractor 500s its in-flight requests
  immediately and is respawned.
- Graceful shutdown: drain to deadline, then abort in-flight clients and
  reap workers; second signal force-exits.
- Bounded request queue with 503 backpressure; bounded body channels give
  per-request backpressure in both directions with the GVL released.
- TLS via rustls (file paths or inline PEM).
- Near-zero-allocation env construction: frozen LRU caches for
  Host/peer-address values, shared frozen `rack.errors`, and a shared
  frozen null `rack.input` for bodyless requests.
- The Rust side allocates through mimalloc (chosen over jemalloc in a three-way benchmark).
- Fused worker loop: the env arrives with the request handle embedded
  (`env["kino.request"]`) and the common complete-body response rides a
  single respond-and-take native call—~one FFI crossing per request,
  no per-request arrays. Opt-in `batch` directive for grabbing several
  queued requests per visit (default 1; >1 trades fairness for
  throughput).
- Experimental `lanes` mode: per-worker queues with awake-preferring
  dispatch and work stealing (+20% ractor-mode plaintext on Linux,
  making ractor the fastest Kino configuration on real hardware). Off
  by default.
- Live stats: `server.stats` (queued, in-flight, served, rejected,
  respawns, lane depths) and a `SIGUSR1` one-line dump for CLI servers.
- Native async logging: a `log_requests` access log (status-colored on
  color terminals—2xx green, 3xx yellow, 4xx maroon, 5xx bright red—503
  rejections included) and `Kino::Logger` / `Kino::Logger::Device`—lines
  flow through a lock-free channel to a Rust flusher thread, so
  request threads never take a log mutex or issue a write syscall. The
  device is Ractor-shareable.
- `kino --check` (and `Kino::Check.report`): explains why an app is not
  Ractor-shareable—captured variables with definition sites, ivar
  paths, and the class-ivar trap—without freezing anything.
- Puma-style Ruby DSL config file (`kino.rb`) and a `kino` CLI
  (`kino -C kino.rb config.ru`); precedence kwargs > file > defaults.
  Directives include `environment`, `pidfile`, and `rackup`.
- Hot-path performance work: a GVL-free queue fast path, zero-copy response
  headers, a frozen Ractor-shareable env-string cache, and no per-request
  task spawns. Single-process throughput is on par with (or modestly above)
  a same-topology process cluster in our benchmarks; see README.
- Request timeouts: `request_timeout: seconds` (off by default) returns an
  immediate 504 when the app misses the deadline; the late response is
  dropped and the handler is never killed. Counted as `stats[:timeouts]`.

[#3]: https://github.com/yaroslav/kino/issues/3
