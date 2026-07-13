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
