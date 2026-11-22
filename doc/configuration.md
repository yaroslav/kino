# Configuring Kino

Every server option, the config file and the CLI, the modes,
`kino --check`, timeouts and intake limits, `on_error`, the lifecycle
hooks, sharded I/O and HTTP/2. The [README](../README.md) has the short
version of each; [operations.md](operations.md) covers signals, stats,
logging, the elastic pool and quarantine.

## Server options

`Kino::Server.new` takes every option as a keyword argument. Embedded,
with everything spelled out:

```ruby
server = Kino::Server.new(app,
  bind: "127.0.0.1",          # or "unix:///run/kino.sock" behind a proxy
  port: 9292,                 # 0 = ephemeral; read back via server.port
  workers: 8,                 # ractors (parallelism); default: CPUs, capped at RUBY_MAX_CPU
  max_workers: nil,           # experimental: grow past workers under load (see Elastic pool)
  scale_down_after: 30,       # seconds idle before an extra worker retires
  threads: 1,                 # per worker; ractor default 1, threaded default 3
  mode: :auto,                # :auto | :ractor | :threaded
  queue_depth: 1024,          # bounded queue; overflow → 503
  queue_timeout: 5.0,         # seconds before 503 on a full queue
  request_timeout: nil,       # seconds before a slow response becomes a 504 (nil = off)
  max_connections: 8192,      # cap concurrent connections; default: most of ulimit -n
  max_body_size: 50 * 1024 * 1024,  # bytes before a 413; nil = let a proxy handle it
  on_error: ->(e, env) { ErrorTracker.capture(e) },  # after the client got its 500
  shutdown_timeout: 30,       # drain deadline
  control_bind: "127.0.0.1:9293",   # monitoring: /stats /metrics /ready /live; port 0 reads back via server.control_port
  control_token: ENV["KINO_CONTROL_TOKEN"],  # optional Bearer auth for /stats + /metrics
  tls: { cert: "cert.pem", key: "key.pem" },  # file paths or inline PEM
  http2: true,                # ALPN h2 on TLS + plaintext h2c; false = HTTP/1 only
)
server.start
server.shutdown               # graceful: drain → deadline → abort stragglers
```

## Config file and CLI

Settings can live in a Puma-style Ruby DSL file: `kino.rb` in the
working directory, or `config/kino.rb` (the Rails layout), is picked up
automatically; `-C PATH` names any other. Precedence: explicit kwargs
and CLI flags > config file > defaults.

```ruby
# kino.rb
port 9292
workers 8
threads 1
mode :ractor
```

```sh
kino --init                   # write a fully commented sample kino.rb
kino                          # config.ru + kino.rb, port 9292
kino --check                  # explain whether the app can run in :ractor mode
kino -C config/kino.rb -p 3000 -w 4 -m ractor my_app.ru
```

The generated sample documents every directive, including the Rails
settings and the performance notes.

## Modes

- **`:ractor`**: `workers` Ractors × `threads` Threads each. The app must
  be `Ractor.shareable?` (frozen middleware, `shareable_proc` endpoints).
  Forcing `:ractor` with an unshareable app raises
  `Kino::UnshareableAppError`. A crashed worker thread returns 500 to
  its in-flight requests right away and is replaced; the other threads
  of its ractor serve on.
- **`:threaded`**: the same machinery on `workers × threads` plain
  Threads. Runs **any** Rack app, including all of Rails. Parallel for
  I/O, serialized by the GVL for CPU. A crashed worker thread is
  replaced the same way.
- **`:auto`** (default): `:ractor` when the app is shareable, otherwise
  a warning and `:threaded`. One caveat: a *class* used as a Rack app
  always counts as "shareable" (classes are), even if calling it touches
  unshareable state. Force `:threaded` for those.

A crash is an exception that is not a `StandardError` (a `StandardError`
only fails its request, unless `Thread#raise` sends it into a worker
waiting for its next request), or a worker thread that ends short of its
loop's end, `Thread#kill` included. With lanes, the requests waiting for
a crashed worker go to the other workers as new arrivals, their
`queue_timeout` starting over, so behind workers that keep crashing a
client can wait longer than `queue_timeout`. A worker that keeps
crashing within a second of starting, before it served a request (in
`after_worker_boot` too), is respawned after a delay doubling from 0.1 s
up to 5 s; any other crash respawns it at once. More in
[Failure handling](architecture.md#failure-handling).

## `kino --check`

When an app cannot run in `:ractor` mode, Kino can tell you why, instead
of leaving you with a bare `Ractor::IsolationError`. The check changes
nothing (it does not freeze your objects) and names each blocker:
captured variables with the place they were defined, instance variables
by path, and the class-level instance variable trap that catches
class-style apps:

```
$ kino --check
check: app is NOT Ractor-shareable
  - app (Proc at app.rb:12)—captures `cache` = {} (Hash) (unshareable)
  - app (HelloApp).@instance—class-level ivar holds #<HelloApp…>—classes
    pass Ractor.shareable?, but reading this from a worker ractor raises
    Ractor::IsolationError on the first request
  hints: freeze config at boot; build endpoints with Ractor.shareable_proc;
  keep per-worker resources in Ractor.store_if_absent; or run mode :threaded.
```

Exit status is 0/1, so it works in CI. The programmatic form is
`Kino::Check.report(app)`.

## Request timeouts

`request_timeout: seconds` (or `request_timeout 30` in `kino.rb`) limits
how long the app may take to produce a response. Past the deadline the
client gets an immediate **504** while the handler keeps running; its
late response is dropped without harm. Off by default. The handler is
deliberately *not* killed, because interrupting arbitrary Ruby mid-flight
is unsafe. A stuck handler still occupies its worker slot until it
returns, so set the deadline above your slowest legitimate endpoint and
watch `stats[:timeouts]`.

## Intake limits

Timeouts guard your app; the network intake guards itself. New
connections past `max_connections` (default: most of `ulimit -n`) wait
in the kernel's accept queue, as long a queue as the kernel allows
([more](architecture.md#backpressure-in-both-directions)); request
bodies past `max_body_size` (default 50 MB, `nil` delegates to a
fronting proxy) get a **413**; and fixed deadlines drop a client whose
first bytes are not in 15 s after it connected (with HTTP/2, the whole
preface; on an inline connection, the whole first head), or whose
request head is not whole 15 s after it began, stalled TLS handshakes
(10 s), uploads stalled mid-body (30 s), and an HTTP/2 peer that leaves
a ping unanswered (see
[Intake deadlines](architecture.md#intake-deadlines)). A client that
ends its side of the connection once its request is sent is taken as
gone, as nginx takes it.

## Error reporting: `on_error`

When a worker catches an app or delivery error,
`on_error ->(error, env) { ErrorTracker.capture(error) }` is called
after the client got its 500—the only place a tracker sees errors
raised while the response was being written (in `:ractor` mode, build
the handler with `Ractor.shareable_proc`). For what a WebSocket callback
or a wake hook raised, `env` is a small frozen Hash naming the callback
(`"kino.callback"`), not a Rack env.

## Lifecycle hooks

Kino fires four lifecycle hooks alongside `on_error`, split by firing context.

**Worker-context hooks** run inside the worker and are available to all workers:
- `after_worker_boot { |worker_id| }`: runs once before the worker begins serving, with its slot id. In `:ractor` mode it runs inside the worker ractor and must be `Ractor.shareable_proc`.
- `after_request_complete { |env, status| }`: fires inside the worker after each successful response. This is the hot path—leave it unset for zero cost. In `:ractor` mode it must be `Ractor.shareable_proc`. It fires once an accepted WebSocket handshake's 101 is out (status 101) and after a partial hijack, but not after a full `rack.hijack`, where the app owns the connection and its response.

**Main-context hooks** run on the main thread, outside workers, and are plain procs:
- `after_boot { }`: fires once after the worker pool is up. Wire readiness here—sd_notify, a "server ready" metric, and so on.
- `on_worker_exit { |worker_index, error| }`: fires when a worker exits, with its index and the crash cause (or nil on a clean exit). In `:ractor` mode it fires for each worker thread that crashed, with the ractor's index and a copy of the cause (a `Kino::Error` naming it when it cannot be copied out of the ractor).

`after_worker_boot`'s argument is the worker's slot id, while in `:ractor` mode `on_worker_exit`'s argument identifies the exited ractor (`0`..`workers - 1`)—a different number space—so don't correlate boot and exit by that number in `:ractor` mode.

A raising hook is logged and never kills a worker.

## Sharded I/O

`io_shards true` (off by default) moves HTTP I/O from Tokio's shared
multi-thread runtime onto current-thread shards: one thread accepts and
hands each connection to the least-loaded shard (open WebSockets and
hijacked connections count), which then owns it for its lifetime—no
work-stealing, no cross-thread wakeups on the hot path.
On the 2026-09 reference box (a c7a.2xlarge) fast handlers gained +1-3%
(best with `io_threads 8`); the win grows with scheduler contention, so measure on
your own core count. Ruby-bound endpoints are unchanged. Orthogonal to
`mode`: it reshapes the Rust side only.

```ruby
# kino.rb
io_shards true
io_threads 8  # optional; default: half the available CPUs
```

The measurements are in
[benchmarks.md](benchmarks.md#sharded-io-io_shards-true).

## HTTP/2

On by default, on both transports, with nothing to configure:

- **TLS binds** advertise `h2` via ALPN, so browsers and h2-capable
  clients negotiate HTTP/2 and everything else stays on HTTP/1.1.
- **Plaintext binds** serve prior-knowledge h2c: a client that opens
  with the HTTP/2 preface (an h2-preferring load balancer, a gRPC-style
  backend hop, `curl --http2-prior-knowledge`) gets HTTP/2; ordinary
  clients are HTTP/1.1 exactly as before. Browsers never do h2 on
  plaintext, so a certificate-less kino behaves identically for them.

The Rack side is spec-complete on h2: `SERVER_PROTOCOL` is `"HTTP/2"`,
`HTTP_HOST`/`SERVER_NAME`/`SERVER_PORT` come from the `:authority`
pseudo-header (h2 requests carry no Host header), split cookie headers
are rejoined with `"; "`, and streamed uploads flow through the same
backpressured body channel as HTTP/1. Streams multiplex into the same
worker slots as keep-alive requests—`workers × threads` bounds
concurrency either way. (`rack.hijack` and WebSockets stay on HTTP/1:
h2 multiplexes streams on one connection, so no request has a
connection of its own to hand over.) `http2 false` pins the server to
HTTP/1 and drops `h2` from ALPN.

How it works inside is in [architecture.md](architecture.md#http2),
and the numbers are in [benchmarks.md](benchmarks.md#http2).
