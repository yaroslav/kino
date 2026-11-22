## [0.8.0] - 2026-11-23

### WebSockets

- Experimental native WebSockets. With `lanes true`, an app accepts a
  WebSocket handshake by setting `env["rack.upgrade"]` to a handler
  when `env["rack.upgrade?"]` is `:websocket`, the convention iodine
  and Agoo use. Kino runs the protocol in Rust, and the handler's
  `on_open`, `on_message`, `on_drained`, `on_shutdown` and `on_close`
  run on the worker that accepted the socket, one event at a time and
  between its requests; an idle socket costs Ruby nothing. With it come
  topics (`ws.subscribe`, and `Kino::WebSocket.publish` from any thread
  or Ractor), frozen, shareable socket handles (`Kino::WebSocket[id]`),
  a per-worker wake hook (`Kino::Worker.on_wake`), flow control
  (`ws.pending`, `on_drained`), bounded messages, queues and socket
  counts (`websocket_max_message`, `websocket_max`), keepalive pings
  (`websocket_ping_interval`), and a graceful drain that runs
  `on_shutdown` and closes each socket with 1001. A callback that
  raises drops only its own socket and goes to `on_error`. The Rails
  example app streams new posts to WebSocket clients in ractor mode.
- `rack.hijack`, full and partial, is offered on HTTP/1.1 requests that
  ask to upgrade, in both modes and with or without lanes, over TLS too,
  so Action Cable runs unmodified in threaded mode. A full hijack
  answers with a 101, or with a response that carries a Content-Length
  or has no body (a 204, a 304, the answer to a HEAD); any other
  response gets a 502. A drain does not wait for hijacked
  connections. Each holds three file descriptors, so many open at once
  can run the process out of them below `max_connections`.

### `worker_loop` and inline serving

- Experimental `worker_loop`: an app can run its own fiber scheduler,
  such as Async's, on every Kino worker thread, in both modes. Kino
  hands the scheduler each request as a job, so a request that waits
  suspends only its fiber while the worker takes the next one. Kino
  still owns the Ractors, threads, dispatch and HTTP, and ships no
  scheduler: the app's `worker_loop` proc starts one and calls
  `worker.serve(concurrency: 32) { |job| task.async { job.call } }`.
  At most `concurrency` requests run at once on a worker, WebSocket
  callbacks and the wake hook run alongside them, and `rack.input`
  reads and streamed writes wait by readiness under any fiber
  scheduler. Dispatch spreads requests by what each worker has open,
  and with every worker full a request waits its turn, in the order
  requests came (counted in `queued`), up to `queue_timeout`.
  Quarantine and `busy_ms` judge such a worker by how long its thread
  stays away from its loop. `worker_loop` turns lanes on, refuses
  `lanes false` and ignores `batch`; apps without it keep Kino's own
  loop as it was.
- Inline connections: a `worker_loop` server serves plain HTTP/1.1
  connections, over TCP or a unix socket, on its worker threads, with
  nothing to configure. Each goes to a worker with room for it, whose
  loop runs it as a job: Kino reads, parses and answers each request on
  the worker's thread, with no hand-off between threads, no wake-up and
  no task per request. Anything outside a strict subset of HTTP/1.1
  (HTTP/1.0, a chunked or larger body, `Expect`, an upgrade) goes back
  to hyper before the app sees it, with the bytes already read, and
  TLS and HTTP/2 stay with hyper. Behind nginx, connections stay inline
  while nginx speaks HTTP/1.1 with keep-alive, as it does by default
  since 1.29.7. With it:
  - A worker takes at most `concurrency` connections and serves them in
    turn, so a slow request delays the other connections on its worker.
  - A connection's requests run on one fiber, so fiber-local state one
    sets is still there for the next, which behind a proxy can be
    another client's: reset per request what you set.
  - Inline connections keep hyper's deadlines, drain and answers, with a
    few recorded differences: a stricter deadline for the first head,
    headers an app should not set, and four at the drain, one of which
    drops a kept-alive request whose head is over 16 KiB. A retiring
    worker hands its connections to hyper, and a crashing one those
    waiting for a request.
  - A connection alone on an otherwise idle worker spins for up to
    100 µs for its client's next request, with the GVL released, backing
    off to a spin in 32 requests for a client that mostly comes later.
  - `stats[:inline]` (nil, and null in `/stats`, on a server that serves
    nothing inline) and `kino_inline_*` in `/metrics` count connections,
    requests, hand-backs, cuts and spins; each `worker_status`
    row counts its inline connections.

### Performance

- With `lanes true`, a request that arrives while every worker is idle
  now goes to the worker that finished most recently, whose caches are
  still warm, instead of rotating through all of them. A lightly loaded
  server keeps one client's requests on one worker; a burst still
  spreads across idle workers. On the heavier server-rendered pages of
  a database-backed Rails chat app, together with `io_shards true`,
  this turned a 13-15% single-client deficit against a fiber-based
  server into parity or better. A request also no longer queues behind
  a worker that is 100 µs or more into its current request while
  another worker sits idle: a fast request next to a one-second one used
  to wait up to 50 ms and is now answered as quickly as without lanes,
  while handlers faster than 100 µs keep their hot lane. In threaded
  mode, where every worker shares one GVL, a CPU-bound app keeps its
  throughput and mean latency, but with four clients on a 0.6 ms
  endpoint the median rose from 1.2 to 2.4 ms while the 99th percentile
  fell from 56 to 3 ms.
- Built for a Ruby that supports it (4.1 and later), Kino's own native
  objects are freed in each Ractor's own garbage collection, and no
  longer wait for the batch frees that stop every Ractor. On a Ruby 4.1
  development build this raised throughput and cut tail latency under
  load.
- `rack.input` reads cost less: each read takes its bytes from an
  offset into what Kino pulled from the client, sharing them or filling
  the caller's buffer, where it used to cut and copy them. A 500 KiB
  upload read 4 KiB at a time spends about a sixth of the time it did.

### Reliability

- Every listener and every worker is hardened. Changes that may need
  action: a `queue_depth` below 1 fails at boot; `on_worker_exit` gets
  a `Kino::Error` for a killed worker thread, where it got nil; over
  TLS, a client that speaks other than what it chose by ALPN is no
  longer served; a `Thread#raise` into an idle worker crashes it, and
  it is respawned. A connection that has not sent its first bytes (with
  HTTP/2, the whole preface) 15 s after the accept is closed, and one
  that has sent nothing as soon as the drain begins, where with HTTP/2
  on it used to stay open for good. An HTTP/2 peer that is gone no
  longer holds its connection: one silent for 30 s gets a ping, and is
  closed unless it answers within 20 s. With no request waiting, an
  answer's body is closed as soon as the answer is out, not when the
  next request arrives, and every body is closed exactly once; a
  `close` that raises no longer loses the request taken with its answer.
- In ractor mode, a worker thread that crashes (an exception that is not
  a `StandardError`) is now cleaned up and replaced inside its ractor:
  its in-flight requests get their 500 at once, any sibling threads
  carry on, and the ractor's own state and app threads survive, where
  before the crash could take the whole ractor down. `on_worker_exit`
  fires for each crashed thread, with a `Kino::Error` naming a cause
  that cannot be copied out of its ractor. In both modes, a worker that
  keeps crashing within a second of starting, before it served a
  request, `after_worker_boot` included, is now respawned after a delay
  that doubles from 0.1 s to 5 s instead of thousands of times a second,
  the log showing the first crash with its backtrace and each later one
  as a single line; and a crashed worker's slot goes to a later worker
  once its connections are gone, so crashes no longer grow the
  per-worker rows of `stats`, `/stats` and `/metrics`, and with
  `lanes true` a request that arrives while a crashed worker is being
  replaced waits for a worker instead of getting a 503. A worker thread
  that ends without being asked to, `Thread#kill` included, is a crash
  in both modes. A crashed worker's queued requests go to the other
  workers as new arrivals, their `queue_timeout` starting over, rather
  than get a 500, and a request a worker let go of unanswered gets its
  500 as soon as the worker comes back for its next request or leaves.
- A streaming response body's write now raises `Kino::ClientGone`, an
  `IOError`, once its client has gone, for enumerable bodies and Rack 3
  streaming bodies alike. Writes to a vanished client used to succeed
  silently, so an endless body such as a server-sent event feed never
  ended and held its worker for good. A body whose response can carry
  none (a HEAD request, a 204 or 304, or one a `request_timeout` 504
  already answered) ends the same way as soon as it writes. Kino treats
  this as the normal end of the request: the body is closed and
  `after_request_complete` runs, with nothing logged and nothing sent
  to `on_error`. Giving up on a stream whose client stopped reading no
  longer waits for that client, so a body that raised mid-stream no
  longer stalls its worker, nor a shutdown past its deadline. Answering
  a request while a thread of the app's own still reads its body works
  too, so a full-duplex body can read on one thread while another
  writes; a `rack.hijack` call then raises an `IOError`, and a WebSocket
  handshake gets a 500. A Rack 3 streaming body that raises mid-stream
  now has its connection cut, as an enumerable body's is, instead of
  ending as if complete. Likewise, a request body cut short by a
  shutdown past its deadline now raises "request interrupted during
  shutdown" when read, instead of ending early as if it were complete.
- Kino's listener now asks for the longest accept queue the kernel
  allows (`net.core.somaxconn`, 4096 on current Linux) where it had 128,
  so on Linux a burst of new connections no longer loses SYNs and waits
  a second to retry. While Kino is not accepting, that many clients now
  wait connected: overload shows as a slow first byte, and a health
  check that only opens a TCP connection passes for longer. macOS, at
  128 by default, is unchanged.
- A server booting on a unix socket path, its listener's or its control
  plane's, no longer waits on Linux when a live server there has a full
  accept queue: it is refused at once. A crashed server's socket is
  still reclaimed at the next boot; a path that holds something other
  than a socket is now refused, where Linux used to delete it. On macOS
  a boot can still take the path of a live server whose accept queue is
  full, and two boots at the same instant on a stale path can both bind
  it; a `pidfile` the two share prevents both.
- Out of file descriptors, Kino now waits 10 ms between attempts to
  accept a connection instead of retrying in a tight loop that kept a
  CPU busy.

### Defaults and stats

- In ractor mode the default worker count is now the CPU count capped
  at `RUBY_MAX_CPU` (8 unless set), so a default boot no longer warns
  on machines with more than 8 cores. An explicit `workers` is
  unchanged, and threaded mode still defaults to the CPU count.
- `stats`, `/stats` and `/metrics` now count WebSockets (process-wide,
  under `websockets`; `kino_websockets_*` and `kino_websocket_*_total`
  with causes in a `cause` label), wake hook runs (`kino_wakes_total`),
  open hijacked connections (`kino_hijacked_connections`, and in each
  `worker_status` row with its open sockets) and streamed responses cut
  short, by cause (`responses_cut`, `kino_responses_cut_total`). A
  request leaves `in_flight` once the app hijacks it. `Server#stats`
  keys and the USR1 stats line now follow the order `/stats` reports
  them in.

### Fixed

- Fixed requests in threaded mode waiting for seconds under sustained
  load. A worker that kept finding requests queued held the GVL until
  Ruby's 100 ms timer took it away, most often in the middle of a
  request, which then waited for every other worker thread's 100 ms:
  over two seconds with 8 workers of 3 threads. A request handed to an
  idle worker could wait as long. A worker now lets the threads waiting
  for the GVL run between two requests once it has held it for a
  millisecond, plus the request it was running. On 8 CPUs the slowest
  response of a CPU-bound endpoint went from 2.3 s to 26 ms at the same
  throughput, while its median rose from 2.2 to 3.4 ms (with `lanes
  true`, where a request waits for its own worker, from 0.16 to 3.7 ms).
  Where workers otherwise rarely hand the GVL over it costs throughput:
  plain-text responses gave up 5% with `io_shards true`, and 2% under a
  `worker_loop` (1% with 16 clients), whose waits in its own scheduler
  Kino does not see, so it may pass the GVL again right after one. Ractor
  mode with more than one thread per worker gets the same bound; with
  one, the default, it is unchanged.
- Fixed a complete response going out with the app's `Content-Length`
  when that disagreed with its body: on a kept-alive HTTP/1.1
  connection, the rest of a longer body was read as the next response,
  or a shorter one swallowed the start of the next, and over HTTP/2 the
  client got a mismatched length. A complete body now goes out with its
  own length, and an app's own `Transfer-Encoding` on it is dropped
  (beside a `Content-Length` it used to make Kino close the connection
  unanswered); an app that compresses its body says so in
  `Content-Encoding`.
- Fixed `Server.run`, the `kino` executable and the Rack handler
  waiting for the workers to exit rather than for the shutdown to end:
  after INT or TERM the process could exit with its pidfile and socket
  files still in place; with `max_workers` above `workers` it could
  exit on its own once the pool retired its first worker; and in
  ractor mode a worker stuck past `shutdown_timeout` kept it from
  exiting at all. They now return once the shutdown has ended and
  raise its error if it failed, so the process exits non-zero. A
  `shutdown` called during another now waits for it.
- Fixed a worker that waited for its next request leaving for good when
  another thread called `Thread#wakeup` or `Thread#run` on it: it took
  the wake for a shutdown, nothing replaced it, and requests sent its
  way hung until the drain gave up on them. It now goes back to
  waiting, and a request it takes just as a `Thread#raise` arrives is
  no longer lost: it goes back to wait for the next take, or, with
  nowhere to go back to, is answered with a 500 at once, and the
  exception is raised first. A request waiting on its client, for more
  of its body or for room for the next chunk of a streamed response, no
  longer fails with "request interrupted during shutdown" when woken
  either, and a `Thread#raise`, which `Timeout.timeout` uses, reaches
  the app's `rescue` with the request intact: a streamed response the
  app then ends now ends for the client too, where it used to stay
  open, and the app can still take the connection over with
  `rack.hijack`, which used to fail.
- Fixed a threaded worker killed by an exception outside
  `StandardError` (`raise Exception`, a stack overflow) leaving its
  client unanswered and its request in flight, so the next shutdown
  waited out `shutdown_timeout` or was cut short by the crash. As in
  ractor mode, the client now gets a 500 at once and a fresh thread
  takes its place, counted in `respawns` and
  `kino_worker_respawns_total`.
- Fixed a streamed response cut short, as the app raised mid-stream or
  its worker crashed, sometimes reaching its client as nothing at all,
  head included, rather than as what was written followed by the cut,
  over HTTP/1.1 and HTTP/2 alike.
- Fixed HTTP/2 answers that may carry no body: one to a HEAD request,
  Kino's own 500, 503 and 504 included, sent its body, and so did a 204
  or 304 the app streamed, while a 204 sent a `Content-Length`; clients
  built on nghttp2, such as curl, refuse either as a protocol error.
  Such answers now carry no body, and a 204 no length.
- Fixed `Server#shutdown` with a `timeout:` that is NaN, negative or not
  a number: it is now refused with an `ArgumentError` before the
  shutdown begins, leaving the server serving and no `Server#wait`
  seeing the error, and such a `shutdown_timeout` fails at boot.
  `Float::INFINITY` still waits for the drain however long it takes.
- Fixed `SERVER_PORT` for a request whose `Host` (or HTTP/2
  `:authority`) names no port: it is now the port of the listener that
  took the request (80 on a unix socket), where a process running more
  than one server reported the port of whichever saw that host first.

### Dependencies

- Updated dependencies, including magnus 0.9.4, bytes 1.12.2 and
  rustls-webpki 0.103.17.

## [0.7.2] - 2026-10-24

- Updated dependencies, including tokio 1.54, hyper 1.12 and
  parking_lot 0.13, which is faster under lock contention.

## [0.7.1] - 2026-09-25

- Updated Magnus to 0.9 and other dependencies.
- Kino is now tested against Ruby HEAD (the unreleased next version)
  as well as Ruby 4.0.
- Fixed a crash on Ruby 4.1 in ractor mode: with Ruby's new
  per-ractor GC, a worker ractor could collect cached env strings and
  large response bodies that other ractors were still using.

## [0.7.0] - 2026-09-08

- Experimental elastic worker pool. Set `max_workers` above `workers`
  and the pool grows under load, one worker at a time, then shrinks
  back to `workers` once the extra workers have sat idle for
  `scale_down_after` seconds (default 30). Works in both modes; a
  retiring worker finishes its request first. `stats`, `/stats` and
  `/metrics` gain `max_workers`, `active_workers`, `scale_ups` and
  `scale_downs`, and each `worker_status` row gains `retired`. Leave
  `max_workers` unset and the pool is fixed, as before.
- Ractor mode warns at boot when `max_workers` (or `workers`) exceeds
  `RUBY_MAX_CPU` (default 8), Ruby's cap on how many ractors run Ruby
  code at once. Set the variable to your worker count to lift it.
- Ractor-readiness fixes, so external Ractor audits of Kino pass: the
  env string caches root their strings through the lock-free pin slab
  instead of per-value GC registration (unsynchronized across ractors
  in Ruby 4.0, a crash) and no longer call into Ruby while locked (a
  GC-barrier deadlock); the shared rack.errors and rack.input
  singletons must be Ractor-shareable, not just frozen; the Rack
  handler's option table is shareable. Throughput is unchanged.

## [0.6.0] - 2026-09-01

- HTTP/2 support. Kino now speaks HTTP/2 natively, on by default:
  browsers negotiate it over TLS (ALPN), h2-preferring balancers and
  tools get prior-knowledge h2c on plaintext, and everyone else keeps
  getting HTTP/1.1 on the same port. `http2 false` restores an
  HTTP/1-only server. What ships with it:
  - A spec-complete Rack env on h2: `SERVER_PROTOCOL` is `"HTTP/2"`,
    `HTTP_HOST`/`SERVER_NAME`/`SERVER_PORT` come from the `:authority`
    pseudo-header, split cookie headers are rejoined, and uploads
    stream with the same backpressure as HTTP/1.
  - Uploads at full speed: body reads now drain every arrived chunk in
    one native call, so h2's 16 KB data frames don't pay a GVL
    round-trip each—h2 uploads run at HTTP/1 parity, and chunked
    HTTP/1 uploads got faster too.
  - `MAX_CONCURRENT_STREAMS` advertised from worker-slot capacity, so
    an h2-aware balancer sees the server's real admission and a single
    connection cannot multiply into hundreds of queued requests.
  - Graceful connection drains: on shutdown, every connection finishes
    its in-flight request and then closes (`Connection: close` on
    HTTP/1, GOAWAY on h2) instead of being cut mid-stream at the
    deadline.
  - Interned header values: low-cardinality headers (user-agent,
    accept-*, sec-ch-*) reuse one frozen string instead of allocating
    per request—the env-side analogue of HPACK's wire dedup, worth a
    few percent on header-heavy traffic and 6-8 fewer allocations per
    request, on HTTP/1 too. Cookies and authorization are deliberately
    never cached.

  Measured on the reference box: native h2 is +79% over HTTP/1.1
  cleartext and 2× over TLS on the same server, +50% over
  nginx-terminated h2 in front of Kino, ~3× falcon on fast handlers.
- Leaner request hot path: up to 2-4% more throughput on fast handlers
  with `lanes` (measured on Linux), no change elsewhere.
- All benchmarks re-measured from scratch on a fresh reference box
  (c7a.2xlarge, Ruby 4.0.6, Puma 8.0.2): the 2026-06 numbers
  reproduced within 1-3% at matched configurations, memory to the
  megabyte. The published tables carry the new run, with two additions
  and one honest downgrade: new studies for sharded I/O (+1-3% on 8
  cores, best at `io_threads 8`) and HTTP/2 (above), and a narrower
  ractor /cpu lead over the cluster (+25%, was +34%) because Puma
  8.0.2 itself got faster. The bench harness gained the sharded-I/O
  lanes and the Rails memory comparison, and the methodology notes now
  record two measurement traps: memory is only comparable after the
  full endpoint battery, and /io only at equal slot counts.

## [0.5.0] - 2026-08-29

- Add opt-in `io_shards true`: accepted HTTP connections can run on
  current-thread Tokio I/O shards instead of Tokio's shared multi-thread
  runtime, reducing scheduler contention on very fast handlers. `io_threads`
  sets the shard count; otherwise Kino uses half the available CPUs
  ([Patrik Wenger](https://github.com/paddor)).
- Update Rust dependencies: hyper 1.11.1, rb-sys 0.9.130,
  rustls-webpki 0.103.15, among others.

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
