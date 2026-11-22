# Kino

**Kino** is a high-performance **Ractor** web server for Ruby 4.0+.

[![GitHub Release](https://img.shields.io/github/v/release/yaroslav/kino)](https://github.com/yaroslav/kino/releases)
[![Docs](https://img.shields.io/badge/yard-docs-blue.svg)](https://rubydoc.info/gems/kino)

Ruby threads cannot run Ruby code in parallel, so production setups fork
a process per core and pay for each copy in memory. Kino runs your code
on every core in **one small process**. A **Rust** (tokio + hyper)
front-end owns the network, parallel **Ractors** run your Rack 3 app,
and a threaded fallback mode runs everything else. Rails main partially
runs on Ractors too (see [Rails on Ractors](doc/rails-on-ractors.md)).

* **Fast.** On a real 8-core server, Kino's ractor mode is **1.5-1.6×**
  ahead of a Puma fork cluster on I/O-light endpoints (threaded mode
  1.1-1.4×)—with native **HTTP/2** adding another **+41%** over
  HTTP/1.1 on the same server (+58% over TLS).
  Ractor mode also wins on pure CPU, **+14%**.
  [Benchmarks](#benchmarks) below.
* **A fraction of the memory.** About **~7×** on the simplistic bench
  Ractor app (14× on a Ruby 4.1 development build), and about **6.7×
  less memory** than a Puma cluster serving Rails (measured in threaded
  mode, before Rails could run on Ractors).
* **Parallel without forking.** Ractor mode runs CPU work **more than
  5× faster** than Kino's own GVL-bound threaded mode, in the same
  small process.
* **Production plumbing included.** Graceful drain, crash supervision
  and respawn, bounded queues with 503 backpressure, request timeouts,
  hardened intake (slowloris and TLS-handshake deadlines, connection
  and body-size caps), an `on_error` hook for your error tracker,
  TLS (rustls), live stats, async access and app logging.
* **Tells you why.** `kino --check` lists exactly what blocks your app
  from ractor mode, finding by finding, so you do not have to decode
  `Ractor::IsolationError` yourself.
* **Puma-shaped.** The same `workers × threads` topology, a familiar
  config DSL, a `kino` CLI. If you can run Puma, you can run Kino.

**N.B.:** Ractors are officially **experimental** in Ruby 4.0, and so is this server. The threaded mode is solid. Still, Kino aims to be the best way to experiment with Ractors today—and the best Ractor server when they become stable.

---

## Table of Contents

- [Why](#why)
- [Benchmarks](#benchmarks)
- [Install](#install)
- [Usage](#usage)
- [Config file and CLI](#config-file-and-cli)
- [`kino --check`](#kino---check)
- [Request timeouts](#request-timeouts)
- [Stats](#stats)
- [Logging](#logging)
- [Timer waits](#timer-waits)
- [Rack 3 compliance](#rack-3-compliance)
- [Streaming responses](#streaming-responses)
- [WebSockets](#websockets-experimental)
- [Fibers inside Kino workers](#fibers-inside-kino-workers-experimental)
- [Rails](#rails)

## Why

The GVL allows only one Ruby thread to run at a time. To use all cores,
Ruby servers fork processes, and every fork costs a full copy of the
app. Ractors do not have this limit: each one has its own lock, so one
process can run Ruby in parallel. What was missing is a server that
dispatches requests to them. Ruby 4.0 reworked Ractors (`Ractor::Port`,
`shareable_proc`, less lock contention) and made this worth building.

Why a Ractor server has to be built this way, and which Rust parts make
Ractors fast here: [doc/why-kino.md](doc/why-kino.md). The full design
notes live in [doc/architecture.md](doc/architecture.md).

## Benchmarks

Measured on a real server: AWS **c8a.2xlarge** (8-core AMD EPYC 9R45,
16 GB, Amazon Linux 2023). This is a realistic app-server size.

**These tables run a tiny synthetic Rack app**—plaintext, a 10 KB body,
a CPU-bound `fib`, a 5 ms wait—deliberately small, to measure the server
rather than an app. It is Ractor-shareable, so Kino runs it in `:ractor`
mode (and `:threaded` for comparison). **A real Rails app is a different
story:** the Rails numbers were measured in Kino's `:threaded` mode,
before Rails could run on Ractors, and are reported separately—see
[Rails](#rails) below.
Ruby 4.0.9 with YJIT (re-measured 2026-11), every server at 8 workers:
Puma forks 8 workers × 3 threads, Kino stays in one process
(8 workers; 1 thread each in ractor modes, 3 in threaded). Numbers are
req/s by wrk (8-second windows, 64 connections, same host, no CPU
pinning), the median of 3 boots. Ruby 4.0.9 is the released Ruby, so it
is the primary number; the same tables on a Ruby 4.1 development build
are [further down](#ruby-41-development-build).
Methodology: [doc/benchmarks.md](doc/benchmarks.md).

| endpoint    | Kino :ractor | + lanes | :ractor, `workers 32`² | Kino :threaded | Puma (cluster) |
|-------------|-------------:|--------:|-----------------------:|---------------:|---------------:|
| /plaintext  |      465,055 | **525,155** |         245,612 |        394,053 |        291,409 |
| /10k        |      375,470 | **409,085** |         203,154 |        285,270 |        251,357 |
| /cpu (fib)  |  **118,095**¹| 111,751 |          83,713 |         20,187 |        103,653 |
| /io (5 ms)  |        1,553 |   1,566 |       **5,721** |          4,738 |          4,729 |
| /io_native  |        1,578 |   1,579 |       **6,264** |          4,740 |          4,729 |

Memory, by **PSS** (proportional set size; see note) after sustained
load, for two apps.

**The tiny benchmark app** (Ractor-shareable, so Kino runs it in `:ractor`
or `:threaded`). Kino is **~7× lighter in :ractor mode, ~11× in :threaded**
than the Puma cluster—a trivial app is almost all private per-worker
heap, which copy-on-write can't share:

| tiny app, Kino  | Kino (one process) | Puma cluster (8 workers) | ratio |
|-----------------|-------------------:|-------------------------:|------:|
| :ractor (8×1)   |         **167 MB** |                 1,171 MB |  ~7×  |
| :threaded (8×3) |         **110 MB**³|                 1,171 MB | ~11×  |

**A real Rails app** (measured in Kino's `:threaded` mode, before Rails
could run on Ractors; [below](#rails)). The gap is **~6.7×**: Puma's
forks share the Rails framework copy-on-write (about 370 MB of shared
pages), but each fork keeps its own heap, and about 600 MB of the
cluster is private:

| Rails hello-world | Kino :threaded | Puma cluster (8 workers) | ratio |
|-------------------|---------------:|-------------------------:|------:|
| **PSS**           |      **96 MB** |               **640 MB** | ~6.7× |

"+ lanes" is the experimental per-worker-queue dispatcher (`lanes true`).
It posts the fastest plaintext/10k of any configuration here. Details:
[doc/benchmarks.md](doc/benchmarks.md#lane-dispatch-experimental-lanes-true).

¹ Stock settings, no tuning. Ractor mode beats the fork cluster on pure
CPU by +14% (+8% with lanes). Threaded mode shows the GVL ceiling that
every single-process Ruby server hits. The old CPU-tuning recipe stays
retired: its `threads 1` half **is** the default, and its
`tokio_threads 1` half cost throughput when it was last measured
(2026-09); see
[doc/benchmarks.md](doc/benchmarks.md#cpu-bound-tuning).

² Wait-bound throughput is slots ÷ wait, and the default columns bring
8 single-thread workers against the cluster's 24 threads. Kino slots
are threads, not processes—when your app waits a lot, raise `workers`.
The `workers 32` column is that tuning: **+21% over the cluster on /io
(+32% via `Kino.sleep`)**, all in one small process. On Ruby 4.0.9 it
pays for that on every other row: 32 ractors on 8 cores land 16-19%
behind the cluster on /plaintext, /10k and /cpu. On Ruby 4.1 they are
ahead of it on every row ([below](#ruby-41-development-build)). Pick
the topology your app's wait profile needs. See
[doc/benchmarks.md](doc/benchmarks.md#why-io-lags-in-ractor-mode-on-linux).

³ With `MALLOC_ARENA_MAX=2` (the standard Ruby deployment setting;
Heroku's default). Without it the same threaded process ends at 144 MB,
so the cap still saves about 30 MB. Kino used to balloon here: before
its threaded workers passed the GVL between requests, the uncapped
process reached 724 MB on this box. See
[doc/benchmarks.md](doc/benchmarks.md#memory-under-load-and-the-glibc-arena-footgun).

A common first idea is to keep your current server and wrap the app in
a ractor pool. We measured that too (same box; the analysis is in the
doc):

| endpoint   | Kino :ractor (8×3) | Puma + ractor wrapper | Falcon + ractor wrapper |
|------------|-------------------:|----------------------:|------------------------:|
| /plaintext |        **412,687** |                40,675 |                 241,390 |
| /cpu (fib) |        **108,063** |                33,322 |                  94,417 |
| /io (5 ms) |          **4,629** |                 1,509 |                   1,564 |

(The Kino column here runs 8×3—the same 24 slots the wrappers get—so
the /io row is comparable; the main table's ractor column runs the 8×1
default.)

### Ruby 4.1 (development build)

The same box and method on a **development build** of Ruby 4.1
(`4.1.0dev`, master f2d62c677e of 2026-11-21, YJIT on). Ruby 4.1 is not
released, and these figures can move before it is:

| endpoint, Ruby 4.1 dev | Kino :ractor | + lanes | :ractor, `workers 32` | Kino :threaded | Puma (cluster) |
|-------------|-------------:|--------:|-----------------------:|---------------:|---------------:|
| /plaintext  |      512,945 | **608,283** |         463,223 |        378,380 |        269,662 |
| /10k        |      447,144 | **515,785** |         388,306 |        275,927 |        218,884 |
| /cpu (fib)  |      128,097 | 112,276 |     **128,109** |         19,739 |         99,379 |
| /io (5 ms)  |        1,558 |   1,542 |       **6,102** |          4,702 |          4,728 |
| /io_native  |        1,576 |   1,581 |       **6,314** |          4,738 |          4,730 |

| tiny app, Ruby 4.1 dev | Kino (one process) | Puma cluster (8 workers) | ratio |
|-----------------|-------------------:|-------------------------:|------:|
| :ractor (8×1)   |          **52 MB** |                   746 MB | ~14×  |
| :threaded (8×3), `MALLOC_ARENA_MAX=2` | **150 MB** |           746 MB |  ~5×  |

What Ruby 4.1 changes, against the 4.0.9 tables above:

- **Every ractor column rises.** Ruby 4.1 collects garbage per Ractor,
  and on /plaintext and /10k the default ractor column gains +10% and
  +19%, lanes +16% and +26%. On /cpu the default column's median is
  +8% and lanes' is flat; both overlap the range of the 4.0.9 boots.
- **`workers 32` gains about 90% on the light rows** (+89% /plaintext,
  +91% /10k) and +53% on /cpu, where it now matches the 8×1 default. It
  is ahead of the cluster on every row.
- **`:ractor` memory falls to 52 MB** (from 167 MB), **14× lighter than
  the cluster**, which itself falls to 746 MB. Arena-capped `:threaded`
  goes the other way, 110 MB to 150 MB.
- **`:threaded` and Puma lose 2-13%** on /plaintext, /10k and /cpu. The
  wait-bound rows do not move.

The ractor-wrapper, HTTP/2 and Rails tables on Ruby 4.1 are in
[doc/benchmarks.md](doc/benchmarks.md#ruby-41-development-build).

### HTTP/2 head-to-head

Kino speaks HTTP/2 natively, so it skips the usual nginx-termination
hop. Same box as the tables above, Ruby 4.0.9; all over TLS, h2load, 64
in-flight, same app:

| /plaintext                    |       req/s | /upload (64 KB) |
|-------------------------------|------------:|----------------:|
| Kino native h2                | **355,281** |      **52,635** |
| Kino HTTP/1.1 (same boot)     |     225,264 |          47,940 |
| nginx h2 → Kino HTTP/1.1      |     245,741 |          1,224¹ |
| Falcon (native h2)            |     100,399 |          49,162 |

Native h2 is **+58%** over HTTP/1.1 on the same TLS server
(+41% on cleartext h2c; fewer, larger socket operations, and HPACK
spares re-sending cookies) and is **+45% over fronting the same Kino
with nginx**—the proxy hop is pure cost. Uploads, h2's classic weak
spot, run ahead of HTTP/1.1 over TLS (+10%) and within 3% of it in
cleartext. Full matrix, cleartext h2c lanes, and
methodology: [doc/benchmarks.md](doc/benchmarks.md#http2); reproduce
with `bench/h2.sh`.

¹ Not an HTTP/2 cost: nginx spools a request body larger than
`client_body_buffer_size` to a temporary file, and an HTTP/1.1 client
gets the same ~1.2k. With a larger `client_body_buffer_size`, or
`proxy_request_buffering off`, the lane runs at 51-52k; see the doc.

### Rails

These Rails numbers were measured in `:threaded` mode, one GVL-bound
process, before Rails could run on Ractors. On the same box, Ruby 4.0.9
(the controller-only hello app, kept as the bench subject in
`bench/rails_hello.ru`; edge Rails, production, 8×5):

| Rails hello-world            |  req/s | memory (PSS) |
|------------------------------|-------:|-------------:|
| Kino :threaded (one process) |  7,660 |    **96 MB** |
| Puma cluster (8 workers)     | 34,688 |       640 MB |

The honest trade-off: Puma's fork cluster uses all 8 cores, so it serves
~4.5× the throughput—at ~6.7× the memory. Ractor-mode Rails is not
benchmarked yet; what runs and what does not is in
[doc/rails-on-ractors.md](doc/rails-on-ractors.md).

In short: on the tiny synthetic app, ractor mode beats fork-level CPU parallelism (**5.8×** Kino's
own GVL-bound threaded mode, +14% over the cluster) in one process, at
about 1/7th of the cluster's memory by PSS (~6.7× on the Rails hello
app, in threaded mode). Ractor mode is 1.5-1.6× ahead of the cluster on
I/O-light endpoints (1.6-1.8× with lanes, 1.1-1.4× in threaded mode),
and native HTTP/2 adds +41% over cleartext HTTP/1.1 on top (+58% over
TLS). The macOS numbers
(secondary; everything there hits the loopback ceiling) and the
YJIT × Ractors gotcha are in [doc/benchmarks.md](doc/benchmarks.md).

Reproduce: `bench/run.sh [seconds] [concurrency]` for the main table
(it boots every Kino variant at 3 threads; the table's own commands are
in [doc/benchmarks.md](doc/benchmarks.md#methodology)),
`bench/studies.sh` for the follow-ups (CPU recipe, topology, scaling,
sharded I/O, logging, memory), `bench/h2.sh` for the HTTP/2 matrix.

## Install

You need Ruby >= 4.0. Add Kino to your application's bundle:

```sh
bundle add kino      # or: gem install kino (outside a bundle)
```

or put it in the `Gemfile` yourself:

```ruby
gem "kino"
```

Then generate a config and serve:

```sh
bundle exec kino --init    # writes kino.rb; every directive documented in place
bundle exec kino           # picks up config.ru + kino.rb, serves on :9292
```

(After a standalone `gem install`, the `kino` command works without
`bundle exec`.)

Prefer your framework's own command? Kino ships a Rack handler, so
`rails server -u kino` and `rackup -s kino` boot it too. They read the
same `kino.rb` (or `config/kino.rb`), the host's `-p`/`-b` flags win over
the file, and `rackup -s kino -O Workers=4 -O Mode=threaded` reaches the
rest (`rackup -s kino --help` lists them).

No Rust compiler needed: released versions ship precompiled native gems
for Linux (x86_64/aarch64, glibc and musl) and macOS (arm64). On other
platforms the gem compiles at install time; that needs a Rust toolchain,
plus clang/libclang on Linux.

## Usage

```ruby
require "kino"

# Ractor mode needs a Ractor-shareable app: capture nothing, freeze config.
app = Ractor.shareable_proc do |env|
  [200, { "content-type" => "text/plain" }, ["Hello from #{Ractor.current}"]]
end

Kino::Server.run(app, port: 9292)   # traps INT/TERM; Ctrl-C drains gracefully
```

Or embedded, with everything spelled out:

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

### Modes

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
[Failure handling](doc/architecture.md#failure-handling).

### Sharded I/O

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

### HTTP/2

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

Timeouts guard your app; the network intake guards itself. New
connections past `max_connections` (default: most of `ulimit -n`) wait
in the kernel's accept queue, as long a queue as the kernel allows
([more](doc/architecture.md#backpressure-in-both-directions)); request
bodies past `max_body_size` (default 50 MB, `nil` delegates to a
fronting proxy) get a **413**; and fixed deadlines drop a client whose
first bytes are not in 15 s after it connected (with HTTP/2, the whole
preface; on an inline connection, the whole first head), or whose
request head is not whole 15 s after it began, stalled TLS handshakes
(10 s), uploads stalled mid-body (30 s), and an HTTP/2 peer that leaves
a ping unanswered (see
[Intake deadlines](doc/architecture.md#intake-deadlines)). A client that
ends its side of the connection once its request is sent is taken as
gone, as nginx takes it.

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

## Elastic pool (experimental)

Size the pool for the quiet hours and let it grow for the busy ones:

```ruby
# kino.rb
workers 4            # always running
max_workers 16       # reached only under load
scale_down_after 30  # seconds idle before an extra worker retires
```

Or `Kino::Server.new(app, workers: 4, max_workers: 16)`. Leave
`max_workers` unset and the pool is fixed at `workers`, as before.

While requests wait in the queue, Kino adds a worker every 100 ms until
the queue clears or the pool hits `max_workers`. When the load passes,
workers above `workers` retire one at a time after `scale_down_after`
seconds idle, each finishing its current request first. Same behavior
in `:ractor` and `:threaded` mode. A worker with open WebSockets or
hijacked connections counts as busy.

**Use it when your app waits**: on databases, upstream services, slow
clients. With `workers` at your core count, all workers can be blocked
on I/O while cores sit idle; a higher ceiling puts those cores to work,
and a ractor starts in microseconds, so the pool follows load closely.
Pure CPU work gains nothing past the core count. In `:ractor` mode Ruby
itself runs at most `RUBY_MAX_CPU` ractors' Ruby code at once (default
8). The default `workers` in `:ractor` mode is the CPU count capped at
that limit, so a default boot never warns; Kino warns only when an
explicit `workers` or `max_workers` exceeds it. On a bigger box:

```sh
RUBY_MAX_CPU=16 kino
```

**Watch it breathe** in `server.stats`, `GET /stats` and `GET /metrics`:

```sh
$ curl -s localhost:9293/stats | jq '{workers, max_workers, active_workers, scale_ups, scale_downs}'
{
  "workers": 4,
  "max_workers": 16,
  "active_workers": 9,
  "scale_ups": 12,
  "scale_downs": 7
}
```

Prometheus gets `kino_max_workers`, `kino_active_workers`,
`kino_scale_ups_total` and `kino_scale_downs_total`. `after_worker_boot`
fires for every worker the pool adds, `on_worker_exit` (with a nil
cause) for every one it retires.

## Stuck-worker quarantine

`quarantine_timeout: seconds` (or `quarantine_timeout 60` in `kino.rb`)
quarantines a dispatch slot whose request (or WebSocket callback, or wake
hook) has run longer than the deadline (under a
[`worker_loop`](doc/worker-loop.md#stuck-workers-and-quarantine), whose
thread has stayed away from its loop that long) and spawns a replacement
worker to restore capacity—distinct from
`request_timeout`, which gives the client a 504 but leaves the slot
occupied. `quarantine_max` (default: the worker count in `:ractor` mode,
workers × threads in `:threaded`) caps the total number of replacement
events over the process lifetime—past it the monitor stops replacing and
the server runs at reduced capacity.

The wedged worker is never interrupted or force-killed, and its slot stays
quarantined for good. In `:threaded` mode, if the blocked thread
eventually returns, it keeps serving requests on that same slot—but the
slot itself stays flagged quarantined (busy_ms reported as 0) for the rest
of the process; in `:ractor` mode the wedged ractor (and its supervisor
thread) leaks until the process exits, since a wedged ractor cannot be
safely interrupted. Monitor quarantine activity via `server.stats`
(top-level `quarantined` count and per-slot `worker_status[].quarantined`
flag), `GET /stats` (same), and `GET /metrics` (`kino_quarantined_workers`
gauge and `kino_quarantine_replacements_total` counter).

## Stats

`server.stats` returns a live snapshot: the configuration plus counters
from the native layer (one relaxed atomic per request, no measurable
cost):

```ruby
server.stats
# => {mode: :ractor, lanes: false, workers: 8, threads: 1, batch: 1,
#     respawns: 0, queued: 0, in_flight: 2, served: 1041, rejected: 0,
#     timeouts: 0, worker_status: [...], …}
# plus lane_depths: [...] when lane dispatch is on
```

From the outside, `kill -USR1 <pid>` logs the same snapshot as one line
(pair it with `pidfile` to find the pid):

```
kino[4213] main: stats mode=:ractor lanes=false workers=8 threads=1 batch=1 respawns=0 queued=0 in_flight=2 served=1041 rejected=0 timeouts=0 …
```

For pull-based monitoring, `control_bind "127.0.0.1:9293"` (or a
`unix://` path) serves a read-only **control plane** from the native
layer on its own thread—it keeps answering even while every Ruby worker
is busy or stuck, and reports `draining` through a graceful shutdown:

- `GET /stats`—the same snapshot as `server.stats`, as JSON (plus
  `state` and `version`).
- `GET /metrics`—Prometheus text format (`kino_requests_served_total`,
  `kino_queue_depth`, `kino_ready`, …).

Both `/stats` and `/metrics` also break the counters down per dispatch
slot: `/stats` carries a `worker_status` array (`index`, `served`,
`in_flight`, `busy_ms`, `quarantined`, `retired`, `websockets_open`,
`hijacked_connections`, `inline_connections`) and `/metrics` emits
`kino_worker_*{worker="N"}` series, one entry per execution slot
(`workers × threads`). A crashed or retired worker's slot goes to a
later worker, its counters starting over, once its sockets, hijacked
connections and inline connections are gone. `busy_ms` is how long the
slot's current request, WebSocket callback or wake hook has been running
(0 when idle; under a
[`worker_loop`](doc/worker-loop.md#stats-and-logs), how long its thread
has stayed away from its loop), so a single slot climbing while the rest
sit at 0 is your stuck worker.

The `/stats` response and `server.stats` carry `queue_time` (count and
summed seconds), and `/metrics` exposes `kino_request_queue_seconds`—a
Prometheus histogram of queue-wait time, the worker-saturation signal.
Counts admitted requests only; a 503 after queue wait goes to `rejected`,
not `queue_time`.

A hijacked request leaves `in_flight` when the app takes it over and
counts in `hijacked_connections` until its connection closes; `wakes`
counts wake hook runs (under a
[`worker_loop`](doc/worker-loop.md#stats-and-logs), every wake its loop
takes). `responses_cut` counts, for the whole process,
[streamed responses](#streaming-responses) that ended early, by cause:
`client_gone`, `shutdown` (the drain's deadline) and `crash`. The
process's WebSocket counters sit under `websockets`;
[doc/websockets.md](doc/websockets.md#stats) lists them and their
`/metrics` series. Inline connections count under `inline` (nil on a
server that serves none), listed with their `/metrics` series in
[doc/worker-loop.md](doc/worker-loop.md#stats-and-logs).

- `GET /ready`—`200` when serving, `503` while booting or draining:
  wire it to your load balancer or Kubernetes readiness probe.
- `GET /live`—`200` whenever the process is alive: the liveness probe.

`control_token "..."` puts `/stats` and `/metrics` behind
`Authorization: Bearer`; the probes stay open.

## Logging

With one log line per request, `Kino::Logger` sustained **1.7× the
throughput of a shared `::Logger`** (155k vs 90k req/s on the 2026-09
c7a.2xlarge benchmark box). There are two native pieces. Both write through a lock-free
channel to a Rust flusher thread, so request threads never take a log
mutex and never make a write syscall:

- **Access log** (`log_requests true`): two records per request to
  stdout, including the 503s that never reach your app. The arrival line
  is queued before the app runs, so a request that hangs shows as an
  arrow with no answer; the completion line carries the status, the
  total, and a timing breakdown: `ruby` is the time the request spent in
  Ruby (with the GC pause and the objects allocated during it), `kino`
  the server's own overhead, `wait` the queue time before a worker took
  it. Recommended in development; cheap enough for production. On color
  terminals the completion line is tinted by status class: 2xx green,
  3xx yellow, 4xx maroon, 5xx bright red:

  ```
  2026-08-22 14:03:11 +0300 → GET /users?q=1  from 127.0.0.1
  2026-08-22 14:03:11 +0300 ← 200 GET /users?q=1  12.4ms (ruby 9.1ms [gc 0.8ms; 1.5k obj]; kino 3.2ms; wait 0.1ms)
  ```

  The GC and allocation figures come from the VM's process-wide
  counters, so they appear only where one request at a time can own
  them: in `:threaded` mode, or in `:ractor` mode with `workers 1`, and
  never under a [`worker_loop`](doc/worker-loop.md#stats-and-logs),
  whose requests interleave. Parallel ractors would bill each other's
  work, so there the breakdown is `(ruby; kino; wait)` alone.

- **`Kino::Logger`**: a `::Logger` over the same async sink, for your
  app's own logging (`Kino::Logger.new("log/production.log")`, or no
  argument for stdout). The raw IO-like device is `Kino::Logger::Device`,
  for integrations that want bytes without `::Logger` formatting. The
  device is frozen and Ractor-shareable, so one device serves every
  worker.

`Kino::Logger` in a **Rails** app: it is a real `::Logger` subclass, so
it fits anywhere Rails expects a logger:

```ruby
# config/environments/production.rb, simplest forms:
config.logger = Kino::Logger.new                          # stdout
config.logger = Kino::Logger.new("log/production.log")    # file
# both file and stdout:
config.logger = ActiveSupport::BroadcastLogger.new(
  Kino::Logger.new("log/production.log"), Kino::Logger.new
)
# tagged logging wraps it like any ::Logger:
config.logger = ActiveSupport::TaggedLogging.new(Kino::Logger.new)
```

From a plain **Rack** app, give middleware the logger, or hand
`Rack::CommonLogger` the raw device (it just calls `write`):

```ruby
# config.ru
use Rack::CommonLogger, Kino::Logger::Device.new   # access-style app log
run MyApp
```

(If you only want request lines, prefer Kino's own `log_requests true`.
It is free for your Ruby threads, and it also sees the 503s that never
reach Rack.)

Graceful shutdown drains both logs fully. A hard crash can lose the tail
of the buffer, and when you log faster than the disk can take (over 100k
lines/s), the sink drops lines instead of blocking request threads.
These trade-offs are measured in
[doc/benchmarks.md](doc/benchmarks.md#logging-costs).

**Server lines.** Everything Kino says about itself (draining, a crash
and its respawn, a hook that raised, quarantine, the stats line,
`rack.errors`) reads `kino[<pid>] <source>: message`, the source being
the worker that spoke, `worker-3` (or `worker-3/thread-2` in a
multi-threaded ractor), or `main`. On color terminals the label is dim
for notes, yellow for warnings, red for errors; the message stays plain.
A failed request gets a report instead of a bare backtrace: the request
line, the error, and where it raised in your code, then the trace with
your frames first, relative to the working directory, and the rest
folded:

```
kino[4213] worker-2: 500 GET /boom · RuntimeError: kaboom (app.rb:12:in 'explode')
    app.rb:12:in 'explode'
    /usr/lib/ruby/gems/4.0.0/gems/rack-3.2.7/lib/rack/builder.rb:...
    … 38 more
```

Hooks can log through the same channel with `Kino::Log.info`, `.warn`,
and `.error`; it is safe inside worker ractors.

## Timer waits

`Kino.sleep(seconds)` is a high-resolution sleep on the OS clock with
the GVL released. MRI's own `sleep` wakes up late inside non-main
ractors (details and numbers in [doc/benchmarks.md](doc/benchmarks.md)).
Use `Kino.sleep` for explicit timer waits in handlers (but not under a
[`worker_loop`](doc/worker-loop.md#waiting-the-right-way), where it
holds every fiber on the worker: use `sleep` there). Ordinary blocking
I/O does not need it.

## Rack 3 compliance

The spec suite runs every test app under `Rack::Lint` over real sockets:
streaming request bodies (forward-only `rack.input`), enumerable and
callable (full-duplex stream) response bodies, lowercase and multi-value
headers, HEAD/204 semantics. `rack.hijack`, full and partial, is offered
on HTTP/1.1 requests that ask to upgrade (see
[WebSockets](#websockets-experimental)).

## Streaming responses

A body that does not respond to `to_ary` streams: each chunk goes out
as the body yields it (chunked unless the app set `content-length`),
and a Rack 3 streaming body, one that responds to `call` but not
`each`, writes to the stream it is called with. A client slower than
the app makes the next write wait for it, with the GVL released.

Once the client has gone (it hung up, or its connection broke), the
body's next write raises `Kino::ClientGone`, an `IOError`, so an endless
body such as a server-sent event feed or an LLM token stream ends there
and frees its worker. A response that can carry no body (a HEAD
request, a 204 or 304, one already answered with a `request_timeout`
504) ends its body the same way. Kino closes the body and runs
`after_request_complete` as for any finished response, and logs
nothing; an `ensure` in the body is the place to clean up:

```ruby
class Ticks
  def each
    loop do
      yield "data: #{Time.now.to_i}\n\n"
      Kino.sleep(1)
    end
  ensure
    Kino::Log.info("ticker stopped")
  end
end

app = ->(env) { [200, {"content-type" => "text/event-stream"}, Ticks.new] }
```

A body that raises anything else mid-stream is logged and reported to
`on_error`, and its client sees the connection cut rather than a
response that looks complete. Cutting a stream short never waits on a
client that stopped reading, so such a client cannot hold up the worker
or a shutdown.

## WebSockets (experimental)

Kino runs WebSockets natively: Rust does the handshake, framing, pings,
the close handshake and topic fan-out, and your handler's callbacks run
on the worker that accepted the socket, so an idle socket costs Ruby
nothing. Native WebSockets need `lanes true`. Libraries with a
WebSocket stack of their own, such as Action Cable, get `rack.hijack`
in either mode.

```ruby
module Chat
  def self.on_open(ws) = ws.subscribe("chat")
  def self.on_message(ws, data) = Kino::WebSocket.publish("chat", data)
end

app = Ractor.shareable_proc do |env|
  if env["rack.upgrade?"] == :websocket
    env["rack.upgrade"] = Chat
    [101, {}, []]   # the headers go into the 101; the status is ignored
  else
    [200, {"content-type" => "text/plain"}, ["hello"]]
  end
end
```

`ws` is a frozen, shareable handle that works from any thread or
Ractor, `Kino::WebSocket.publish` builds each frame once for all of a
topic's subscribers, and `Kino::Worker.on_wake` runs background work on
a worker's own thread. [doc/websockets.md](doc/websockets.md) is the
guide: callbacks, flow control, topics, limits, close codes,
`rack.hijack` and Action Cable, stats.

## Fibers inside Kino workers (experimental)

Kino serves from Ractors; with `worker_loop`, your own fiber scheduler
runs inside each worker. A request that waits (a database permit, an
upstream call, a slow client) suspends only its fiber, and the worker
takes the next request meanwhile, with no extra thread and no extra
wake-up per request. Kino still owns the Ractors, threads, dispatch and
HTTP, and ships no scheduler and depends on none; Async is the example:

```ruby
# kino.rb
require "async"

threads 1
worker_loop Ractor.shareable_proc { |worker|
  Sync do |task|
    worker.serve(concurrency: 32) { |job| task.async { job.call } }
  end
}
```

[doc/worker-loop.md](doc/worker-loop.md) is the guide: what counts
toward `concurrency`, how requests, WebSocket callbacks and the wake
hook share a worker, waiting the right way, shutdown, quarantine,
dispatch, inline connections, per-request cost and stats.

### Inline connections

A `worker_loop` server serves plain HTTP/1.1 connections on its worker
threads, with nothing to configure: Kino reads each request and writes
its answer on the worker's own thread, with no hand-off, wake-up or task
per request. TLS, HTTP/2 and anything outside a strict subset of
HTTP/1.1 stay with hyper. A connection's requests share one
fiber, so reset fiber-local state per request.
[Inline connections](doc/worker-loop.md#inline-connections) is the
guide.

## Rails

Rails main partially runs on Ractors: `Rails.application.ractorize!`
makes a production app shareable. `examples/rails-hello` runs in `:ractor`
mode with Active Record on SQLite and a live feed of new posts over
Kino's native WebSockets (`bundle exec kino` in that directory;
`--mode threaded` also works). Its Ractor setup follows Shopify's
[ractor-safe-rails](https://github.com/Shopify/ractor-safe-rails)
template. Action Cable, the default cache store, the Solid gems, the
async Active Job adapter and `render json:` are not Ractor-safe yet, so
apps that use them stay on `:threaded` (`rails server -u kino`, or the
`kino` CLI), where Action Cable runs unmodified through `rack.hijack`.
What the example needs, and what still does not work, is in
[doc/rails-on-ractors.md](doc/rails-on-ractors.md).

## Development

```sh
bin/setup
bundle exec rake                       # compile, Rust tests, specs, RBS, lint
RB_SYS_CARGO_PROFILE=dev bundle exec rake compile   # fast dev rebuilds
```

## Acknowledgements

Thanks to [Mat Sadler](https://github.com/matsadler) for [magnus](https://github.com/matsadler/magnus).

For ractors, thanks to [Koichi Sasada](https://github.com/ko1), [John Hawthorn](https://github.com/jhawthorn), [Jean Boussier](https://github.com/byroot), [Luke Gruber](https://github.com/luke-gruber), and other Ruby core contributors.

For the Rust network stack, thanks to [Sean McArthur](https://github.com/seanmonstar) for [hyper](https://github.com/hyperium/hyper), and to [Carl Lerche](https://github.com/carllerche), [Alice Ryhl](https://github.com/Darksonn), and the other [Tokio](https://github.com/tokio-rs/tokio) maintainers for the runtime underneath it. Thanks to [Joshua Barretto](https://github.com/zesterer) for [flume](https://github.com/zesterer/flume)—its channels carry every request between the network side and the workers.

## Assisted by

Claude Code (Fable 5.2, Opus 5.5).

## Contributing

Bug reports and pull requests are welcome on GitHub at
https://github.com/yaroslav/kino.

## License

The gem is available as open source under the terms of the
[MIT License](https://opensource.org/licenses/MIT).
