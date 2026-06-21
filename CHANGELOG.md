## [Unreleased]

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
