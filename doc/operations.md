# Operating Kino

Signals and shutdown, live stats and the control plane, logging, the
elastic pool and stuck-worker quarantine. Every other option is in
[configuration.md](configuration.md).

## Signals and shutdown

`Kino::Server.run` and the `kino` command trap INT and TERM: the first
drains gracefully, a second force-exits. `kill -USR1 <pid>` logs a
[stats](#stats) line. An embedded server drains with `server.shutdown`:
drain, then the deadline (`shutdown_timeout`), then abort the
stragglers. Every step of the drain is in
[Graceful shutdown](architecture.md#graceful-shutdown).

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

## The control plane

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
[`worker_loop`](worker-loop.md#stats-and-logs), how long its thread
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
[`worker_loop`](worker-loop.md#stats-and-logs), every wake its loop
takes). `responses_cut` counts, for the whole process,
[streamed responses](streaming.md#streaming-responses) that ended early, by cause:
`client_gone`, `shutdown` (the drain's deadline) and `crash`. The
process's WebSocket counters sit under `websockets`;
[websockets.md](websockets.md#stats) lists them and their
`/metrics` series. Inline connections count under `inline` (nil on a
server that serves none), listed with their `/metrics` series in
[worker-loop.md](worker-loop.md#stats-and-logs).

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
  never under a [`worker_loop`](worker-loop.md#stats-and-logs),
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
[benchmarks.md](benchmarks.md#logging-costs).

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
[`worker_loop`](worker-loop.md#stuck-workers-and-quarantine), whose
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
