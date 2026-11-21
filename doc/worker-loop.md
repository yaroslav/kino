# Fibers inside Kino workers: `worker_loop` (experimental)

Kino's own loop runs one request at a time on each worker thread, so a
request that waits (on a database permit, an upstream API, a slow
client) holds its worker until it is done. `worker_loop` lets your app
run its own fiber scheduler on every Kino worker thread, in both modes.
Kino hands the scheduler each request as a job: a request that waits
suspends only its fiber, and the worker takes the next one meanwhile.
Kino keeps the Ractors, the threads, dispatch and all of HTTP. It ships
no scheduler and depends on none: Async is the example on this page,
and any `Fiber::Scheduler` works. How it works inside is in
[architecture.md](architecture.md#worker-loops).

Turn it on when your requests spend their time waiting and you want
many of them on one worker, or when your app is fiber-native already.
For requests that never wait, Kino's own loop stays the leaner path
(see [Per-request cost](#per-request-cost)).

## Setting it up

```ruby
# kino.rb
require "async"

threads 1
worker_loop Ractor.shareable_proc { |worker|
  Sync do |task|
    # This worker's own background tasks, if any, start here as
    # transient tasks (see "Background work" below).
    worker.serve(concurrency: 32) { |job| task.async { job.call } }
  end
}
```

Or pass `worker_loop:` to `Kino::Server.new`. The sample config that
`kino --init` writes has the same example, commented out.

- Kino calls the proc once on each worker thread, after
  `after_worker_boot`, with that worker's `Kino::Worker::Loop`. In
  `:ractor` mode it runs inside the worker Ractor, so build it with
  `Ractor.shareable_proc`: with a plain block, `:auto` mode falls back
  to `:threaded` with a warning, and `mode :ractor` refuses to start.
- `worker.serve(concurrency:) { |job| ... }` is Kino's loop, written for
  a scheduler: without one on the thread it raises `ArgumentError`.
  `concurrency` is a positive Integer, the most requests the worker
  runs at once.
- The block starts each job on the scheduler, and must run it at once
  until it first waits, as `task.async { job.call }` does under Async
  (and `Fiber.schedule { job.call }` under any scheduler); see
  [Dispatch](#dispatch). Jobs must run on the worker thread, as fibers
  of its scheduler: never hand one to another thread or a thread pool.
- `serve` returns `nil` once the worker is drained (the server shuts
  down, or the elastic pool retires the worker) and every job has
  finished. Let the proc return only after `serve` has: Kino treats an
  earlier return as a crash (`Kino::Error`, "worker_loop returned
  before its worker was drained"), and a new worker takes its place.
- `worker_loop` turns `lanes` on; `lanes false` with a `worker_loop`
  raises `ArgumentError`. `batch` has no effect, and Kino warns when it
  is set.
- `threads 1` is the natural setting: the fibers are the concurrency.
  For parallelism, raise `workers` in `:ractor` mode; in `:threaded`
  mode every loop shares the GVL.

## What runs where

Everything on a worker still runs on its one thread, so nothing on it
runs in parallel; requests, WebSocket callbacks and the wake hook
interleave wherever one of them waits. State they share across a wait
needs the care any fiber-concurrent code needs.

### Requests

Each request is one job, which does what Kino's own loop does for a
request, from the env to `after_request_complete`. A job raises into
your scheduler only when Kino's own handling failed, after the client
got its 500. Stopping a request's job from outside aborts its request
(a 500, or a cut connection mid-stream) and frees its slot.

`concurrency` counts a request from the moment the loop takes it until
its job ends, `after_request_complete` included. A native WebSocket
handshake is a request like any other; the socket's events then go to
a job of their own (below). A request stops counting before its job
ends only when the app takes its connection over: a full `rack.hijack`,
or a 101 answered with a callable (a partial hijack, or a Rack 3
streaming body). A partial hijack with any other status is a streamed
response and counts until it ends. With `concurrency` requests running,
the loop takes no more requests, though it keeps taking socket events
and wakes.

### WebSockets

- Each socket's events run one at a time and in order, on a job of
  their own while the socket has any. Different sockets' callbacks run
  concurrently: an `on_message` that waits holds up only its own
  socket. Socket jobs do not count toward `concurrency`.
- `on_open` runs on the handshake's job, before the client gets the
  101. Events that arrive meanwhile wait behind it, even when the
  handshake timed out (`request_timeout`) while `on_open` ran.
  `on_close` is the socket's last event.
- A worker's loop holds at most 64 socket events and wakes it has taken
  and not yet done. At that many it takes only requests until some are
  done, and Kino pushes back on the sockets' clients meanwhile. At most
  16 of one socket's messages wait or run at once, so one flooding
  socket never fills the backlog alone.
- A callback that raises is handled as in Kino's own loop, and the
  events behind it keep running.

Everything else in [websockets.md](websockets.md) holds under a
`worker_loop` too: flow control, topics, limits, pings and the drain's
goodbye.

### The wake hook

`Kino::Worker.on_wake` works as in Kino's own loop: register it in
`after_worker_boot`, or in the `worker_loop` proc before `serve`. Its
runs go on a job of their own, alongside requests and socket callbacks
rather than between them, and never two at once: wakes that come while
it runs fold into one more run. The wake job does not count toward
`concurrency`.

### Background work

Background work that belongs to a worker can run as tasks on its
scheduler, started inside the `Sync` block before `serve`. Start them
transient, or stop them once `serve` has returned:

```ruby
Sync do |task|
  task.async(transient: true) { loop { MyApp.flush; sleep 1 } }
  worker.serve(concurrency: 32) { |job| task.async { job.call } }
end
```

`Sync` waits for every task that is not transient, so a periodic task
started with a plain `task.async` would keep the worker from returning
after a drain: shutdown would wait out `shutdown_timeout`, and a
retired worker would never leave. Kino logs a warning for each worker
whose `worker_loop` still runs a second after its `serve` returned.

### Jobs your scheduler stops

A socket's job and the wake job carry events Kino has already taken
from the worker's queues. When your scheduler stops one of them before
its events are done, nothing else would run those events, so `serve`
raises `Kino::Error` ("a worker_loop job ended before its socket events
or wakes were done"), and the worker crashes and is replaced.

## Waiting the right way

Whatever waits through the scheduler suspends only its fiber:

- `sleep`, Ruby's socket and pipe I/O, `Thread::Queue#pop`, `Mutex`, and
  the scheduler's own primitives;
- Kino's body I/O: `rack.input` reads, and the writes of a streamed
  response (an enumerable body, or a Rack 3 stream). While the client
  is slower than the app, the fiber waits on a descriptor and costs no
  wake-ups meanwhile. Each such request holds up to four descriptors
  that `max_connections` does not count: near the open-file limit, many
  slow clients can run the process out of descriptors, and a wait then
  raises an `IOError` in the app. Leave room for them in `ulimit -n`.

Whatever blocks the thread instead holds every fiber on the worker:

- `Kino.sleep`, which waits on the OS clock: use `sleep` under a
  `worker_loop`;
- a C extension call that blocks without going through the scheduler
  (some database drivers do);
- CPU-bound work, and anything inside `Fiber.blocking { }`.

Body I/O otherwise behaves as in Kino's own loop. A write raises
`Kino::ClientGone` once the client has gone or Kino has cut the stream
(see [Streaming responses](../README.md#streaming-responses)), and a
body read still waiting on its client when shutdown passes its deadline
raises `RuntimeError` ("Kino: request interrupted during shutdown").
Beyond that:

- While a thread of the app's own waits to read a request's body, the
  connection cannot change hands under that read: `rack.hijack` raises
  an `IOError`, and a native WebSocket handshake or a 101 with a
  callable gets a 500. A fiber's wait on the body holds nothing.
- A request's reader and its writer wait apart, so a full-duplex body
  can read on one fiber while it writes on another. Two fibers that
  wait on the same side of one request can take each other's wake-up,
  and one then waits for the next change.

These waits work under any scheduler: an app on Kino's own loop that
reads `rack.input` inside `Sync { }` gets them too. There an enumerable
body is written after the app returned, outside its scheduler, the
blocking way.

## Shutdown and retirement

When the server drains, or the elastic pool retires the worker, the
loop takes nothing more and `serve` waits for every job: suspended
requests, work in `after_request_complete`, jobs serving switched
connections, and each socket's remaining events up to its `on_close`.
A drain waits up to `shutdown_timeout` for that, and past it gives up
on what still runs as it does on a stuck handler: clients still waiting
get a 500 or a cut connection, and body reads still waiting raise.

A drain closes connections that switched protocols once requests have
drained, so the jobs serving them read EOF and can end. A hijacked
request the app has not answered yet, such as a long poll held on its
fiber, stays open until the workers have stopped, so its job holds the
drain until it answers, up to `shutdown_timeout` (see
[websockets.md](websockets.md#rackhijack-and-action-cable)).

## Stuck workers and quarantine

A `worker_loop`'s requests may wait for seconds by design, so Kino
judges the thread, not the requests: a worker is busy while its thread
stays away from its loop and there is work for it. A worker whose
fibers only wait reads at most about 50 ms of `busy_ms`; a job that
computes without waiting adds its own run time. Keep
`quarantine_timeout` well above that. A fiber that spins while the
worker has no work at all, or after the loop got its end (a drain or a
retirement), is not seen: that worker reads 0. `request_timeout` works
as usual: the client gets its 504 while the job runs on, holding its
slot until it ends.

## Dispatch

[Lane dispatch](architecture.md#lane-dispatch) counts what a worker has
open: a worker whose fibers all wait counts as idle, one with
`concurrency` requests open gets no more, and with every worker full a
request waits its turn, in the order requests came, up to
`queue_timeout`, then gets a 503. While a sibling sits idle, a worker
holding a request it has not answered yet is passed over, so a few
clients spread over the workers.

What the app controls is when a job starts: dispatch sees a worker's
loop, not its fibers. A job left queued (on a `Thread::Queue`, or on
Async's `Queue`, `Condition` or `Notification`) starts only after the
loop told dispatch that the worker is idle, so requests wait behind it
while idle siblings get none. Start each job at once, as `task.async`
does.

`concurrency` counts a request until its job ends, so a worker whose
answered requests still run (in `after_request_complete`, or past a
`request_timeout` 504) can be sent a request it has no room for yet. It
waits in the worker's lane until a job ends, or until an idle sibling
takes it, within 50 ms.

## Per-request cost

A request that reaches an idle `worker_loop` worker costs more than in
Kino's own loop, which answers one request and takes the next in a
single native call: one wake-up of the scheduler (none while work
waits, so nothing under load), and a task and a fiber per request, the
larger part. For requests that wait on a database or another service
none of this shows; for an app whose requests do almost nothing, these
are its levers:

- **A task per request**, as above. This is the default for good
  reason: each request has a task and a fiber of its own, so its fiber
  storage and task-local state are its own, stopping its task aborts it
  alone, and a job that raises ends nothing else.
- **`RUBY_SHARED_FIBER_POOL_FREE_STACKS=0`** in the environment Ruby
  starts with keeps a finished fiber's stack where Ruby would hand its
  memory back, one system call per request; every stack a worker has
  used then stays in memory, up to `concurrency` per worker plus those
  of its socket and wake jobs.
- **A fixed set of job fibers (runners), each job handed straight to
  an idle one**: no task and no fiber per request. It looks like
  this:

<!-- spec/worker_loop_spec.rb runs this block as a kino.rb. -->

```ruby
worker_loop Ractor.shareable_proc { |worker|
  Sync do |task|
    concurrency = 32 # requests at once, and as many runners
    scheduler = Fiber.scheduler
    idle = []
    done = false
    concurrency.times do
      # Transient, so Sync never waits for a runner.
      task.async(transient: true) do
        runner = Fiber.current
        loop do
          idle << runner
          # A job, or nil once serve has returned: any other wake-up is
          # not a hand-off, and the runner waits on.
          job = scheduler.transfer until done || job.respond_to?(:call)
          break unless job.respond_to?(:call)
          job.call
        end
      end
    end
    worker.serve(concurrency:) do |job|
      if (runner = idle.pop)
        # Runs the job now, until it first waits, as task.async would.
        scheduler.resume(runner, job)
      else
        # Socket and wake jobs, and requests whose connection switched,
        # hold runners outside concurrency: every runner can be busy.
        task.async { job.call }
      end
    end
    done = true
    scheduler.resume(idle.pop, nil) until idle.empty?
  end
}
```

The hand-off uses two methods of Async's scheduler that io-event's
selectors implement: `scheduler.transfer` gives control to the event
loop, so an idle runner wakes only when resumed, and
`scheduler.resume(runner, job)` switches to the runner at once, as
`task.async` starts a task. Neither is part of Ruby's
`Fiber::Scheduler` interface, so check the recipe again after an Async
or io-event upgrade (it was written against async 2.46 and io-event
1.22). Feeding the runners through a queue instead would start each job
too late (see [Dispatch](#dispatch)).

What a task per request guarantees no longer holds: requests share the
runners' fibers, so fiber storage and task-local state carry over, and
a wake-up that comes late for one request's wait can end a later wait
of its runner early. Stopping a runner stops its request and the runner
with it, a job that raises ends its runner too, and the pool does not
refill: once no runner is idle, jobs get tasks of their own.

## Stats and logs

- `worker_status[].in_flight` counts the requests the worker has open.
  Socket callbacks and wake hook runs do not count, while in Kino's own
  loop a running callback or hook counts as one.
- `worker_status[].busy_ms` is how long the worker's thread has stayed
  away from its loop while there is work for it (see
  [Stuck workers](#stuck-workers-and-quarantine)).
- `wakes` counts each wake the loop takes, also one that folds into a
  run already waiting, so it can be higher than the hook's runs.
- The access log leaves out the GC and allocation figures, as requests
  interleave: the breakdown is `(ruby; kino; wait)`.
