# Kino

**Kino** is a high-performance **Ractor** web server for Ruby 4.0+.

[![GitHub Release](https://img.shields.io/github/v/release/yaroslav/kino)](https://github.com/yaroslav/kino/releases)
[![Docs](https://img.shields.io/badge/yard-docs-blue.svg)](https://rubydoc.info/gems/kino)

Ruby threads cannot run Ruby code in parallel, so production setups fork
a process per core and pay for each copy in memory. Kino runs your code
on every core in **one small process**. A **Rust** (tokio + hyper)
front-end owns the network, parallel **Ractors** run your Rack 3 app,
and a threaded fallback mode runs everything else, Rails included.

* **Fast.** On a real 8-core server, every Kino mode is **1.4-2×** ahead
  of a same-topology Puma cluster on I/O-light endpoints. Ractor mode
  also wins on pure CPU. [Benchmarks](#benchmarks) below.
* **A fraction of the memory.** One process instead of a fork per core:
  about **1/19th of the Puma cluster's memory** under the same load, and
  about 1/8th when serving the Rails hello-world.
* **Parallel without forking.** Ractor mode runs CPU work **5×** faster
  than Kino's own GVL-bound threaded mode, in the same small process.
* **Production plumbing included.** Graceful drain, crash supervision
  and respawn, bounded queues with 503 backpressure, request timeouts,
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
- [Rails](#rails)

## Why

The GVL allows only one Ruby thread to run at a time. To use all cores,
Ruby servers fork processes, and every fork costs a full copy of the
app. Ractors do not have this limit: each one has its own lock, so one
process can run Ruby in parallel. What was missing is a server that
dispatches requests to them. Ruby 4.0 reworked Ractors (`Ractor::Port`,
`shareable_proc`, less lock contention) and made this worth building.
The design notes live in [doc/architecture.md](doc/architecture.md).

## Benchmarks

Measured on a real server: AWS **c7a.2xlarge** (8-core AMD EPYC 9R14,
16 GB, Amazon Linux 2023). This is a realistic app-server size. The same
Ractor-shareable app runs on every server, Ruby 4.0.5 with YJIT, equal
topology (8 workers × 3 threads; Puma forks, Kino stays in one process).
Numbers are req/s by wrk (8-second windows, 64 connections, same host).
Methodology and the analysis behind every column:
[doc/benchmarks.md](doc/benchmarks.md).

| endpoint    | Kino :ractor | + lanes | Kino :threaded | Puma (cluster) |
|-------------|-------------:|--------:|---------------:|---------------:|
| /plaintext  |      201,472 | **241,501** |    218,348 |        117,838 |
| /10k        |      156,635 | **183,564** |    153,442 |        106,666 |
| /cpu (fib)  |       66,735¹| **70,373**  |     13,298 |         58,207 |
| /io (5 ms)  |        4,527²|   4,530 |      **4,715** |          4,691 |
| /io_native  |        4,714 | **4,717** |        4,709 |          4,692 |

Memory on the same box, RSS under load:

| serving               | Kino (one process) | Puma cluster (8 workers) |
|-----------------------|-------------------:|-------------------------:|
| bench app, :ractor    |          **57 MB** |                 1,078 MB |
| bench app, :threaded  |          **50 MB** |                 1,078 MB |
| Rails hello-world     |          **97 MB** |                   797 MB |

"+ lanes" is the experimental per-worker-queue dispatcher (`lanes true`).
It adds +20% over the shared queue on this hardware and makes ractor
mode the fastest Kino configuration. Details:
[doc/benchmarks.md](doc/benchmarks.md#lane-dispatch-experimental-lanes-true).

¹ Stock settings, no tuning. Ractor mode beats the fork cluster on pure
CPU by +15% (+21% with lanes). Threaded mode shows the GVL ceiling that
every single-process Ruby server hits. The CPU-tuning recipe that our
earlier Docker measurements needed makes no difference on real hardware
(+0.5%); see [doc/benchmarks.md](doc/benchmarks.md#cpu-bound-tuning).

² The ractor timer tax is small on real hardware: −4% against threaded
mode (it was −18% in Docker). Wait-bound throughput is slots ÷ wait, and
Kino slots are threads, not processes. `workers 32, threads 1` measured
**5,922 /io (+27% over the cluster) and 6,254 /io_native (+34%)**, still
one small process. See
[doc/benchmarks.md](doc/benchmarks.md#why-io-lags-in-ractor-mode-on-linux).

A common first idea is to keep your current server and wrap the app in
a ractor pool. We measured that too (same box; the analysis is in the
doc):

| endpoint   | Kino :ractor | Puma + ractor wrapper | Falcon + ractor wrapper |
|------------|-------------:|----------------------:|------------------------:|
| /plaintext |  **201,472** |                19,425 |                 100,624 |
| /cpu (fib) |   **66,735** |                17,106 |                  49,083 |
| /io (5 ms) |    **4,527** |                 1,447 |                   1,549 |

In short: ractor mode reaches fork-level CPU parallelism (**5×** Kino's
own GVL-bound threaded mode) in one process, at about 1/19th of the
cluster's memory. Every Kino mode is 1.4-2× ahead of the cluster on
I/O-light endpoints. The macOS numbers (secondary; everything there hits
the loopback ceiling) and the YJIT × Ractors gotcha are in
[doc/benchmarks.md](doc/benchmarks.md).

Reproduce: `bench/run.sh [seconds] [concurrency]` for the main table,
`bench/studies.sh` for the follow-ups (CPU recipe, topology, scaling,
logging, memory).

## Install

You need Ruby >= 4.0. Add Kino to your application's bundle:

```sh
bundle add kino      # or: gem install kino (outside a bundle)
```

or put it in the `Gemfile` yourself:

```ruby
gem "kino", "~> 0.1"
```

Then generate a config and serve:

```sh
bundle exec kino --init    # writes kino.rb; every directive documented in place
bundle exec kino           # picks up config.ru + kino.rb, serves on :9292
```

(After a standalone `gem install`, the `kino` command works without
`bundle exec`.)

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
  bind: "127.0.0.1",
  port: 9292,                 # 0 = ephemeral; read back via server.port
  workers: Etc.nprocessors,   # ractors (parallelism)
  threads: 3,                 # threads per ractor (I/O concurrency, Puma-style)
  mode: :auto,                # :auto | :ractor | :threaded
  queue_depth: 1024,          # bounded queue; overflow → 503
  queue_timeout: 1.0,         # seconds before 503 on a full queue
  request_timeout: nil,       # seconds before a slow response becomes a 504 (nil = off)
  shutdown_timeout: 30,       # drain deadline
  tls: { cert: "cert.pem", key: "key.pem" },  # file paths or inline PEM
)
server.start
server.shutdown               # graceful: drain → deadline → abort stragglers
```

### Modes

- **`:ractor`**: `workers` Ractors × `threads` Threads each. The app must
  be `Ractor.shareable?` (frozen middleware, `shareable_proc` endpoints).
  Forcing `:ractor` with an unshareable app raises
  `Kino::UnshareableAppError`. A crashed ractor returns 500 to its
  in-flight requests right away, then respawns.
- **`:threaded`**: the same machinery on `workers × threads` plain
  Threads. Runs **any** Rack app, including Rails, today. Parallel for
  I/O, serialized by the GVL for CPU.
- **`:auto`** (default): `:ractor` when the app is shareable, otherwise
  a warning and `:threaded`. One caveat: a *class* used as a Rack app
  always counts as "shareable" (classes are), even if calling it touches
  unshareable state. Force `:threaded` for those.

## Config file and CLI

Settings can live in a Puma-style Ruby DSL file. Precedence: explicit
kwargs and CLI flags > config file > defaults.

```ruby
# kino.rb
port 9292
workers 8
threads 3
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

## Stats

`server.stats` returns a live snapshot: the configuration plus counters
from the native layer (one relaxed atomic per request, no measurable
cost):

```ruby
server.stats
# => {mode: :ractor, lanes: false, workers: 8, threads: 3, batch: 1,
#     respawns: 0, queued: 0, in_flight: 2, served: 1041, rejected: 0,
#     timeouts: 0}
# plus lane_depths: [...] when lane dispatch is on
```

From the outside, `kill -USR1 <pid>` prints the same snapshot as one line
(pair it with `pidfile` to find the pid):

```
Kino stats: mode=:ractor lanes=false workers=8 threads=3 batch=1 respawns=0 queued=0 in_flight=2 served=1041 rejected=0 timeouts=0
```

## Logging

With one log line per request, `Kino::Logger` sustained **2.4× the
throughput of a shared `::Logger`** (151k vs 63k req/s on the benchmark
box). There are two native pieces. Both write through a lock-free
channel to a Rust flusher thread, so request threads never take a log
mutex and never make a write syscall:

- **Access log** (`log_requests true`): one line per request to stdout,
  including the 503s that never reach your app. On color terminals the
  lines are tinted by status class: 2xx green, 3xx yellow, 4xx maroon,
  5xx bright red:

  ```
  127.0.0.1 [Tue, 10 Jun 2026 13:39:56 GMT] "GET / HTTP/1.1" 200 0.1ms
  ```

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

## Timer waits

`Kino.sleep(seconds)` is a high-resolution sleep on the OS clock with
the GVL released. MRI's own `sleep` wakes up late inside non-main
ractors (details and numbers in [doc/benchmarks.md](doc/benchmarks.md)).
Use `Kino.sleep` for explicit timer waits in handlers. Ordinary blocking
I/O does not need it.

## Rack 3 compliance

The spec suite runs every test app under `Rack::Lint` over real sockets:
streaming request bodies (forward-only `rack.input`), enumerable and
callable (full-duplex stream) response bodies, lowercase and multi-value
headers, HEAD/204 semantics. Full hijack is left out on purpose; it is
optional in Rack 3.

## Rails

Rails (edge) runs on Kino today in `:threaded` mode; see
`examples/rails-hello`. Ractor-mode Rails is blocked upstream. The exact
blockers, the `Ruby::Box` findings, and what would unlock it are written
up in [doc/rails-on-ractors.md](doc/rails-on-ractors.md). The example
ships a probe script that re-tests against whatever Rails you bundle.

## Development

```sh
bin/setup
bundle exec rake                       # compile, Rust tests, specs, RBS, lint
RB_SYS_CARGO_PROFILE=dev bundle exec rake compile   # fast dev rebuilds
```

## Assisted by

Claude Code (Mythos, Opus).

## Contributing

Bug reports and pull requests are welcome on GitHub at
https://github.com/yaroslav/kino.

## License

The gem is available as open source under the terms of the
[MIT License](https://opensource.org/licenses/MIT).
