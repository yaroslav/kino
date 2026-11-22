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
- [Lifecycle hooks](#lifecycle-hooks)
- [Elastic pool](#elastic-pool-experimental)
- [Stuck-worker quarantine](#stuck-worker-quarantine)
- [Stats](#stats)
- [Logging](#logging)
- [Timer waits](#timer-waits)
- [Rack 3 compliance](#rack-3-compliance)
- [Streaming responses](#streaming-responses)
- [WebSockets](#websockets-experimental)
- [Fibers inside Kino workers](#fibers-inside-kino-workers-experimental)
- [Rails](#rails-1)

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
pinning), the median of 3 boots.
[Methodology](doc/benchmarks.md#methodology).

| endpoint    | Kino :ractor | + lanes | :ractor, `workers 32`² | Kino :threaded | Puma (cluster) |
|-------------|-------------:|--------:|-----------------------:|---------------:|---------------:|
| /plaintext  |      465,055 | **525,155** |         245,612 |        394,053 |        291,409 |
| /10k        |      375,470 | **409,085** |         203,154 |        285,270 |        251,357 |
| /cpu (fib)  |  **118,095**¹| 111,751 |          83,713 |         20,187 |        103,653 |
| /io (5 ms)  |        1,553 |   1,566 |       **5,721** |          4,738 |          4,729 |
| /io_native  |        1,578 |   1,579 |       **6,264** |          4,740 |          4,729 |

Memory, by **PSS** (proportional set size;
[why](doc/benchmarks.md#methodology)) after sustained load. On the tiny
app Kino is **~7× lighter in :ractor mode, ~11× in :threaded** than the
Puma cluster—a trivial app is almost all private per-worker heap, which
copy-on-write can't share:

| tiny app, Kino  | Kino (one process) | Puma cluster (8 workers) | ratio |
|-----------------|-------------------:|-------------------------:|------:|
| :ractor (8×1)   |         **167 MB** |                 1,171 MB |  ~7×  |
| :threaded (8×3) |         **110 MB**³|                 1,171 MB | ~11×  |

On a real Rails app (in `:threaded` mode, [below](#rails)) the gap is
**~6.7×**: Puma's forks share the Rails framework copy-on-write, but
each fork keeps its own heap:

| Rails hello-world | Kino :threaded | Puma cluster (8 workers) | ratio |
|-------------------|---------------:|-------------------------:|------:|
| **PSS**           |      **96 MB** |               **640 MB** | ~6.7× |

"+ lanes" is the experimental per-worker-queue dispatcher (`lanes true`),
the fastest plaintext/10k of any configuration here
([lanes](doc/benchmarks.md#lane-dispatch-experimental-lanes-true)).

¹ Stock settings, no tuning: ractor mode beats the fork cluster on pure
CPU by +14% (+8% with lanes); threaded mode shows the GVL ceiling that
every single-process Ruby server hits
([CPU tuning](doc/benchmarks.md#cpu-bound-tuning)).

² Wait-bound throughput is slots ÷ wait, and Kino slots are threads, not
processes: `workers 32` is **+21% over the cluster on /io (+32% via
`Kino.sleep`)** in one small process, at 16-19% behind it on /plaintext,
/10k and /cpu on Ruby 4.0.9 (ahead on every row on
[Ruby 4.1](#ruby-41-development-build);
[why](doc/benchmarks.md#why-io-lags-in-ractor-mode-on-linux)).

³ With `MALLOC_ARENA_MAX=2`, the standard Ruby deployment setting
(144 MB without it;
[arenas](doc/benchmarks.md#memory-under-load-and-the-glibc-arena-footgun)).

A common first idea is to keep your current server and wrap the app in
a ractor pool. We measured that too, with Kino at 8×3 for the wrappers'
24 slots ([analysis](doc/benchmarks.md#the-ractor-pool-wrapper-comparison)):

| endpoint   | Kino :ractor (8×3) | Puma + ractor wrapper | Falcon + ractor wrapper |
|------------|-------------------:|----------------------:|------------------------:|
| /plaintext |        **412,687** |                40,675 |                 241,390 |
| /cpu (fib) |        **108,063** |                33,322 |                  94,417 |
| /io (5 ms) |          **4,629** |                 1,509 |                   1,564 |

### Ruby 4.1 (development build)

Ruby 4.0.9 is the released Ruby, and the primary number above. The same
box and method on a **development build** of Ruby 4.1 (`4.1.0dev`,
master f2d62c677e of 2026-11-21, YJIT on), which can move before 4.1 is
released:

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

Ruby 4.1 collects garbage per Ractor: **every ractor column rises** on
/plaintext and /10k, **`workers 32` gains about 90% there** and leads
the cluster on every row, and **`:ractor` memory falls to 52 MB**, 14×
lighter than the cluster; `:threaded` and Puma lose 2-13% on
/plaintext, /10k and /cpu. The rest, with the Ruby 4.1 wrapper, HTTP/2
and Rails tables:
[doc/benchmarks.md](doc/benchmarks.md#ruby-41-development-build).

### HTTP/2 head-to-head

Kino speaks HTTP/2 natively, so it skips the usual nginx-termination
hop. Same box, Ruby 4.0.9; all over TLS, h2load, 64 in-flight, same app:

| /plaintext                    |       req/s | /upload (64 KB) |
|-------------------------------|------------:|----------------:|
| Kino native h2                | **355,281** |      **52,635** |
| Kino HTTP/1.1 (same boot)     |     225,264 |          47,940 |
| nginx h2 → Kino HTTP/1.1      |     245,741 |          1,224¹ |
| Falcon (native h2)            |     100,399 |          49,162 |

Native h2 is **+58%** over HTTP/1.1 on the same TLS server (+41% on
cleartext h2c) and **+45% over fronting the same Kino with nginx**—the
proxy hop is pure cost. Uploads, h2's classic weak spot, run ahead of
HTTP/1.1 over TLS (+10%) and within 3% of it in cleartext
([full matrix](doc/benchmarks.md#http2); `bench/h2.sh`).

¹ Not an HTTP/2 cost: nginx spools a body larger than
`client_body_buffer_size` to a temporary file; with a larger buffer, or
`proxy_request_buffering off`, the lane runs at 51-52k.

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
benchmarked yet ([what runs](doc/rails-on-ractors.md)).

Reproduce: `bench/run.sh [seconds] [concurrency]` for the main table
(the [exact commands](doc/benchmarks.md#methodology)), `bench/studies.sh`
for the follow-ups (CPU recipe, topology, scaling, sharded I/O, logging,
memory), `bench/h2.sh` for HTTP/2. The macOS numbers and the
YJIT × Ractors gotcha are in [doc/benchmarks.md](doc/benchmarks.md).

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

To embed it, `Kino::Server.new(app, **options)` returns a server you
`start` and `shutdown` (graceful: drain → deadline → abort stragglers);
[every option](doc/configuration.md#server-options), spelled out.

### Modes

**`:ractor`** runs `workers` Ractors × `threads` Threads each, and the
app must be `Ractor.shareable?` (frozen middleware, `shareable_proc`
endpoints). **`:threaded`** is the same machinery on plain Threads and
runs **any** Rack app, including all of Rails. **`:auto`** (default)
picks `:ractor` when the app is shareable, otherwise a warning and
`:threaded`. Either way a crashed worker thread is replaced while the
others serve on ([Modes](doc/configuration.md#modes)).

### Sharded I/O

`io_shards true` (off by default) moves HTTP I/O onto current-thread
shards, each owning its connections for their lifetime: no
work-stealing, no cross-thread wakeups on the hot path.
[Sharded I/O](doc/configuration.md#sharded-io) says when it pays.

### HTTP/2

On by default, with nothing to configure: TLS binds advertise `h2` via
ALPN, plaintext binds serve prior-knowledge h2c, every other client
stays on HTTP/1.1, and `http2 false` pins the server to HTTP/1. What
the Rack side sees: [HTTP/2](doc/configuration.md#http2).

## Config file and CLI

Settings can live in a Puma-style Ruby DSL file: `kino.rb` in the
working directory, or `config/kino.rb` (the Rails layout), is picked up
automatically; `-C PATH` names any other. Precedence: explicit kwargs
and CLI flags > config file > defaults. `kino --init` writes a sample
that documents every directive; the flags and a sample file are in
[doc/configuration.md](doc/configuration.md#config-file-and-cli).

```ruby
# kino.rb
port 9292
workers 8
threads 1
mode :ractor
```

## `kino --check`

When an app cannot run in `:ractor` mode, `kino --check` tells you why,
instead of leaving you with a bare `Ractor::IsolationError`. It changes
nothing and names each blocker: captured variables with the place they
were defined, instance variables by path, and the class-level instance
variable trap that catches class-style apps. Exit status is 0/1, so it
works in CI ([sample output](doc/configuration.md#kino---check)).

## Request timeouts

`request_timeout 30` (off by default) answers an app slower than that
with an immediate **504**; the handler is deliberately *not* killed,
because interrupting arbitrary Ruby mid-flight is unsafe. The network
intake guards itself with `max_connections`, `max_body_size` (a **413**)
and fixed deadlines for slow clients, and `on_error` hands what a worker
caught to your error tracker ([more](doc/configuration.md#request-timeouts)).

## Lifecycle hooks

`after_worker_boot` and `after_request_complete` run inside the worker;
`after_boot` and `on_worker_exit` run on the main thread. A raising
hook is logged and never kills a worker
([arguments](doc/configuration.md#lifecycle-hooks)).

## Elastic pool (experimental)

Size the pool for the quiet hours and let it grow for the busy ones:

```ruby
# kino.rb
workers 4            # always running
max_workers 16       # reached only under load
scale_down_after 30  # seconds idle before an extra worker retires
```

While requests wait in the queue, Kino adds a worker every 100 ms.
**Use it when your app waits**: on databases, upstream services, slow
clients. [Elastic pool](doc/operations.md#elastic-pool-experimental).

## Stuck-worker quarantine

`quarantine_timeout 60` quarantines a dispatch slot whose request has
run past the deadline and spawns a replacement worker to restore
capacity; the wedged worker is never interrupted or force-killed.
[Quarantine](doc/operations.md#stuck-worker-quarantine).

## Stats

`server.stats` returns a live snapshot, and `kill -USR1 <pid>` logs it
as one line. `control_bind "127.0.0.1:9293"` serves a read-only
**control plane** from the native layer on its own thread, so it keeps
answering even while every Ruby worker is busy or stuck: `/stats` as
JSON, `/metrics` for Prometheus, and `/ready` and `/live` probes.
[Every counter](doc/operations.md#stats).

```sh
$ curl -s localhost:9293/stats | jq '{state, mode, workers, queued, in_flight, served, rejected}'
{
  "state": "ready",
  "mode": "ractor",
  "workers": 8,
  "queued": 0,
  "in_flight": 0,
  "served": 200,
  "rejected": 0
}
```

## Logging

With one log line per request, `Kino::Logger` sustained **1.7× the
throughput of a shared `::Logger`** (155k vs 90k req/s on the 2026-09
c7a.2xlarge benchmark box): a lock-free channel feeds a Rust flusher
thread, so request threads never take a log mutex and never make a
write syscall. `log_requests true` adds an access log with a timing
breakdown:

```
2026-08-22 14:03:11 +0300 → GET /users?q=1  from 127.0.0.1
2026-08-22 14:03:11 +0300 ← 200 GET /users?q=1  12.4ms (ruby 9.1ms [gc 0.8ms; 1.5k obj]; kino 3.2ms; wait 0.1ms)
```

Rails and Rack setup, the trade-offs and Kino's own server lines:
[Logging](doc/operations.md#logging).

## Timer waits

`Kino.sleep(seconds)` is a high-resolution sleep on the OS clock with
the GVL released, for explicit timer waits in handlers, as MRI's own
`sleep` wakes up late inside non-main ractors
([numbers](doc/benchmarks.md#why-io-lags-in-ractor-mode-on-linux)).
Ordinary blocking I/O does not need it, and under a
[`worker_loop`](doc/worker-loop.md#waiting-the-right-way) use `sleep`.

## Rack 3 compliance

The spec suite runs every test app under `Rack::Lint` over real sockets:
streaming request bodies, enumerable and callable response bodies,
lowercase and multi-value headers, HEAD/204 semantics, and
`rack.hijack`, full and partial, on HTTP/1.1 requests that ask to
upgrade ([Rack 3](doc/streaming.md#rack-3-compliance)).

## Streaming responses

A body that does not respond to `to_ary` streams, and a client slower
than the app makes the next write wait for it, with the GVL released.
Once the client has gone, the next write raises `Kino::ClientGone`, so
an endless server-sent event feed or LLM token stream ends there and
frees its worker ([Streaming](doc/streaming.md#streaming-responses)).

## WebSockets (experimental)

Kino runs WebSockets natively: Rust does the handshake, framing, pings,
the close handshake and topic fan-out, and your callbacks run on the
worker that accepted the socket, so an idle socket costs Ruby nothing.
Native WebSockets need `lanes true`; Action Cable and other libraries
with their own stack get `rack.hijack` in either mode.

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

[doc/websockets.md](doc/websockets.md) is the guide: callbacks, flow
control, topics, limits, close codes, `rack.hijack` and Action Cable.

## Fibers inside Kino workers (experimental)

With `worker_loop`, your own fiber scheduler runs inside each worker. A
request that waits (a database permit, an upstream call, a slow client)
suspends only its fiber, and the worker takes the next request
meanwhile, with no extra thread and no extra wake-up per request. Kino
still owns the Ractors, threads, dispatch and HTTP, and ships no
scheduler and depends on none; Async is the example:

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

[doc/worker-loop.md](doc/worker-loop.md) is the guide.

### Inline connections

A `worker_loop` server serves plain HTTP/1.1 connections on its worker
threads, with nothing to configure and no hand-off, wake-up or task per
request. TLS, HTTP/2 and anything outside a strict subset of HTTP/1.1
stay with hyper ([Inline connections](doc/worker-loop.md#inline-connections)).

## Rails

Rails main partially runs on Ractors: `Rails.application.ractorize!`
makes a production app shareable, and `examples/rails-hello` runs in
`:ractor` mode with Active Record on SQLite and a live feed over Kino's
native WebSockets, following Shopify's
[ractor-safe-rails](https://github.com/Shopify/ractor-safe-rails)
template. Apps that need what is not Ractor-safe yet, such as Action
Cable, stay on `:threaded`, where Action Cable runs unmodified
([what works](doc/rails-on-ractors.md)).

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
