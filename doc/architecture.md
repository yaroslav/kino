# Architecture

```
tokio (Rust threads)                          Ruby
┌──────────────────────────┐
│ accept loop (hyper)      │   bounded MPMC      ┌─ worker: Ractor × threads ─┐
│ per request:             │ ──── queue ───────► │ loop {                     │
│   parse → RequestCtx     │                     │   env = take_one           │ ← blocks with the
│   queue full → 503       │ ◄─── response ───── │   status,h,b = app.(env)   │   per-ractor lock
│   TLS (rustls)           │ ◄─── body chunks ── │   respond / stream         │   RELEASED
└──────────────────────────┘                     └────────────────────────────┘
```

All network I/O lives in Rust on tokio runtimes; hyper parses HTTP/1.1 and
handles keep-alive; rustls terminates TLS. The default is Tokio's
multi-thread runtime. With `io_shards true`, one current-thread runtime
accepts connections and assigns them to current-thread I/O shards, where
each connection stays for its lifetime. Ruby never reads, writes or
closes a client's socket: an app that hijacks a request gets a socket of
its own, which Kino bridges to the client's. Each request becomes a
Rust-side `RequestCtx` pushed to a bounded flume MPMC queue; Ruby
workers pull from it.

## Topology

Puma-style two-level: `workers × threads`.

- `:ractor` mode—`workers` Ractors, each running `threads` Ruby Threads
  over the same worker loop. Parallel across ractors (each has its own VM
  lock); concurrent within one only for I/O-bound handlers.
- `:threaded` mode—the same total capacity as plain Threads on the main
  ractor. Runs any Rack app; the GVL serializes CPU work.
- Identical machinery either way: the flume queue is MPMC, a "worker slot"
  is per-thread, and the worker loop (`lib/kino/worker.rb`) is shared
  verbatim.
- Elastic pool (`max_workers`): a scaler thread on the main ractor
  samples queue depth and the per-slot sensors every 100 ms, adds one
  worker per sample while requests wait, and retires the longest-idle
  worker above the floor after `scale_down_after`. Retirement is a
  per-slot flag raised under the slot's lane lock: the lane dispatcher
  skips the slot, the take loop honors the flag at its next idle tick (a
  request already taken finishes first, a lane worker drains its own
  lane), and the slot is reset and reused by the next worker, so the
  slot table never grows with churn. Both pools (the ractor supervisor
  and the threaded pool) expose the same grow/retire/groups seam.
- Experimental `lanes true` replaces the one shared queue with a small
  private queue per worker slot (see [Lane dispatch](#lane-dispatch) and
  [benchmarks](benchmarks.md#lane-dispatch-experimental-lanes-true)).
  Native WebSockets need it: a lane also carries the bell that wakes its
  worker for its sockets' events (see [WebSockets](#websockets)).
- `worker_loop` hands each worker thread to the app's own fiber
  scheduler, which runs many requests at once there through Kino's
  nonblocking takes (see [Worker loops](#worker-loops)). It runs on
  lanes and turns them on.

## Lane dispatch

With `lanes true` each worker slot has a lane of at most 4 requests,
or for a `worker_loop` worker as many as it has room for (its
`concurrency` less the requests it has open) when that is more, and
the tokio side picks one for each request:

1. An awake lane with room (its worker is not parked), round-robin, so
   a hot worker keeps taking without paying a futex wake; this step
   fills a lane only to 4 requests, however deep it is. While a sibling
   sits idle, a classic worker 100 µs or more into its current item (a
   request, a socket event, a wake hook) is passed over: fast handlers
   keep their hot lane, and past that, waking the sibling (tens of
   microseconds) beats waiting out the rest. A `worker_loop` worker is
   passed over while it holds a request at all: one it has not answered
   yet (or a socket event, a wake hook), or one waiting in its lane.
   What it runs after its answer, and its fibers that only wait, do not
   count, so one client's requests in turn keep their hot lane. This
   spreads a few clients over the workers instead of stacking them on
   one, a trade: with four clients a page endpoint served about a fifth
   more requests with a median about 15% lower, for a 99th percentile
   about 40% higher and about a third more CPU per request (more
   workers awake); at one client and at sixteen nothing moved.
2. Else the idle worker with the fewest requests open, and of those the
   one that parked last, whose caches are still warm. A classic worker
   idles with none open; a `worker_loop` worker idles while its fibers
   wait, so requests that wait spread over the workers instead of
   piling onto the one that parked last. Only an empty lane qualifies,
   so a burst into an idle server spreads instead of queueing behind
   one worker.
3. Else any lane with room, round-robin. A `worker_loop` lane fills
   past 4 only here.

A `worker_loop` worker takes a request whenever its thread comes back
to its loop with a job slot free, so its lane may hold as many as it
has room for: a lane of 4 turned requests away from a worker with room
for them while its thread ran a job. A lane deeper than the room left
would hold requests its worker cannot start yet, behind long polls or
slow clients, while a sibling could. Dispatch reads the worker's loop,
not its fibers: a job the app leaves queued starts only once the loop
has parked the worker, which then reads idle and is sent requests that
wait behind those jobs. In a chat app on 8 workers, requests then
waited 2.0 ms in the scheduler before they started, against 0.02-0.03
ms when each job started at once.

With every lane full, on a `worker_loop` server, a request waits its
turn for room behind those that came before it, and gets a 503 if none
comes within `queue_timeout`. Only the first in line dispatches: a
worker that takes a request from a lane, parks, or drops below its
`concurrency` tells it to look again, and once it has gone in, the next
in line looks; a lane that gains room any other way (it deepens as its
worker answers below its `concurrency`) is seen at the next look, 50 ms
later at most. A newcomer lines up behind those waiting, so requests
that waited go in the order they came. While nobody waits, the signal
costs a worker's take one load. `queued` in the stats counts the
requests in line too. Without a `worker_loop` a request retries every
500 µs instead, in whatever order the retries fall; requests retrying
do not count in `queued`. Measured against that retry on a
`worker_loop` server with every lane full, the line served the same
throughput with a 99th percentile 3 to 10 times lower and, with
hundreds waiting, less CPU; at 64 clients its median was higher.

Retired slots, and a crashed worker's slot until it is recycled, take
nothing. Workers steal from a sibling's lane before they park and on
every 50 ms tick while parked, so a request that reached a worker just
before its item turned slow waits for a sibling that runs out of work
of its own, at most until the next steal of an idle one. A worker that
a tick woke leaves a lane whose own worker idles on it, as the send
woke that one: taking its request would move one client's requests in
turn to a cold worker. (A `worker_loop` worker whose doorbell wait is a
tick overdue is away on a job, and its lane is not left to it.) A
WebSocket handshake tries the workers that own the fewest open sockets
first, in the same order.

In `:threaded` mode every worker shares one GVL, so passing a CPU-bound
request to an idle thread spreads the wait rather than shortening it:
with a 0.6 ms CPU-bound handler, 8 threads and 4 clients, throughput
and mean latency are unchanged, the median rises from 1.2 to 2.4 ms
and the 99th percentile falls from 56 to 3 ms.

## The Rust ↔ Ruby boundary

- **No native (TypedData) handle crosses a ractor boundary.** Worker
  ractors receive plain integers (server id, worker ids) plus the
  Ractor-shareable app; native state lives in a global Rust-side registry
  keyed by those ids. The per-request handle
  (`Kino::Native::Request`, a TypedData object) is created *inside* the
  worker ractor by the take calls (`take_one`/`take_batch`), so its
  ownership is correct by construction.
- **Blocking discipline:** every blocking native call releases the GVL
  through `rb_thread_call_without_gvl2` (rb-sys; magnus doesn't wrap it,
  and a lint bars every other release or pass of the GVL, which would
  raise past Rust frames) so a blocked worker holds no VM lock. Waits
  poll an atomic interrupt flag between bounded `recv_timeout` ticks;
  the unblock function (UBF) just sets the flag. A worker waiting for
  its next request, and a request waiting on its client (a body read, a
  streamed write), tell the shutdown, which raises the slot's halted
  flag, from the VM's own unblocking (`Thread#wakeup`, `#run`, `#raise`,
  `#kill`), which raises a flag of the wait's own:
  then the wait runs its thread's pending interrupts in a protected call
  and waits again, or raises what they raised, so no jump crosses Rust
  frames and every borrow of the request is released on the way out. A
  request a worker's wait took as the unblock function fired goes back
  (to the shared queue, or the worker's lane) before those interrupts
  run, so a raise cannot beat it to Ruby and lose it; one with no room
  to go back is held while they run, and a raise answers it with a 500
  at once. A WebSocket event goes to the worker as taken, and a raise
  fires at Ruby's first interrupt check, before the event's callback,
  so that event is lost (a message keeps its inbound credit, a shutdown
  or close event never runs its callback); the socket's later events
  keep their order.
  `flume::Selector` lost wakeups under sustained load
  (workers went permanently deaf to a non-empty queue after ~100k
  requests) and is not used anywhere.
- **Fast path:** when a request is already queued, `take_one` takes it
  with `try_recv` while still holding the GVL—the release/reacquire pair
  (two scheduler round-trips) is skipped entirely. Under load this is the
  common case.
- **Passing the GVL:** where worker threads share a GVL (every worker in
  `:threaded` mode, a Ractor's `threads` past one), a worker that keeps
  finding work would hold it until Ruby's 100 ms timer preempts it, most
  often mid-request; under a backlog that request then waits for every
  other worker's 100 ms (2.3 s with 8 × 3 threads), and a request handed
  to a parked worker waits as long. So between two items a worker passes
  the GVL, as `Thread.pass` does, once it has held it for 1 ms (2 or 5 ms
  cost more throughput, for a longer wait); with no thread waiting, the
  pass returns at once. A take judges the hold at its last item's start,
  a clock read the slot already keeps (one of its own cost 1-2.5% of
  throughput), so a worker holds the GVL for 1 ms plus an item, a batch
  passes only before it, and a take that rides an answer takes nothing
  when a pass is due, so the answered body closes first. A `worker_loop`
  reads the clock, and its hold runs from its last pass: its scheduler's
  waits go unseen, so after one it may pass at once and wait a round of
  the line.
- **Fused crossing:** the common complete-body response rides
  `respond_and_take_one`: answer the previous request and take the next in
  one FFI call, ~one crossing per request once the loop is warm. The env
  Hash carries the request handle under `env["kino.request"]`, so no
  per-request pair array exists either.
- **Env construction:** one FFI call builds the full CGI side of the Rack
  env as a real Hash. Static keys, common methods/protocols and 44 common
  `HTTP_*` header names come from a frozen (and therefore Ractor-shareable)
  string cache built once at init on the main ractor. Frozen keys also
  skip the dup that `Hash#[]=` performs on unfrozen string keys. Only
  `rack.input` is lazy/streaming.
- **Response path:** the Rack headers Hash is passed through as-is and
  iterated on the Rust side (`RHash#foreach`); header bytes are borrowed
  in place from rooted Ruby strings (safe: GVL held, hyper copies
  immediately). Single-chunk bodies skip the join copy.

## Worker loops

What an app sees is in [worker-loop.md](worker-loop.md); this is how
it works inside.

A `worker_loop` worker's `serve` is Kino's loop written for the app's
fiber scheduler. It never blocks in native code: its takes do not wait,
and with nothing to take it waits on the worker's readiness descriptor,
which the scheduler turns into a fiber wait. A request's job runs the
classic loop's own code (the env, the app call, the response path,
upgrades, hijacks, hooks); jobs end in any order, so the fused
respond-and-take is not used. Apps without a `worker_loop` keep the
classic loop as it was.

**Readiness doorbell.** Each slot of a `worker_loop` server has a
doorbell: a nonblocking Unix socket pair and an armed flag. A take that
finds nothing arms it and looks once more, so a send that raced the arm
is taken now. Every lane send, the lane's close, and a socket event or
wake for a worker too full to read its lane ring it: a ring loads the
flag and, only if it is armed, swaps it off and writes one byte. A busy
worker never arms, so a send to it costs no syscall; an idle one costs
one write per wake-up, as a futex wake does in the classic loop. The
wait has no timeout, so the scheduler keeps no timer for it: one ticker
thread per server rings every armed doorbell each 50 ms, and the take
that answers is the lane tick a parked classic worker gets (a retired
worker's exit, a deferred wake, a steal).

**Takes.** The loop takes what it has room for: anything, in the
classic take's order; with every job slot busy, only socket events and
wakes (a full worker does not park, so dispatch does not read it as
idle); with its backlog of 64 taken events full, only requests, so the
inbox fills and pushes back on the sockets' readers. With room for one
kind only and none of it ready, the loop waits on its doorbell, which a
job that frees a slot or a backlog that drains also rings.

**Open requests.** A take with room prunes the slot's list of open
requests instead of resetting it, and answering a request prunes it
too, so dispatch sees the worker's load at once. A slot at its
`concurrency` is the exception: only its next take with room lowers the
count, as that take is what tells the request first in line for lane
room.

**Jobs.** Socket events and wakes queue in Ruby, per socket and for the
wake hook: the first event of a queue starts a job that drains it in
order, and a socket's `on_open` holds its queue on the handshake's job.
At most one wake run waits beside the running one. Jobs report their
end through a queue the scheduler waits on: a request's job frees its
slot, and a socket's or the wake job that ended with events left fails
`serve`.

**Heartbeat.** Each time the thread comes back to its loop, the slot is
stamped with the time; going to wait on the doorbell stamps it one tick
ahead, as that wait is time in the loop until it is overdue. A worker
is busy while it has a request open or something waits for it, and its
`busy_ms` is the stamp's age. Whether the thread holds something right
now, which dispatch's pass-over reads, is a flag of its own: raised by
a take that hands it something, lowered as it answers that request or
comes back to its loop.

**Per-request bells.** Under any fiber scheduler, `rack.input` reads
and streamed writes take steps that never block: each returns its
result, or a descriptor to wait on. A request has two bells of the
doorbell's kind, opened by their first wait: one for the next body
chunk or the body's end, one for room in the response stream or its
end. A step arms its bell and looks once more before it hands out the
descriptor. The body forwarder rings the first after each chunk and
when it stops, and the response body rings the second each time hyper
takes a frame or drops it, so a read and a write of one request can
wait at once. The bells close when the job ends, or once the last wait
on them is back. Past the shutdown deadline a step that would wait
fails as interrupted, and what already waits is rung.

## Backpressure, in both directions

- The kernel's accept queue, ahead of all of Kino's own. The client
  listener (TCP or unix) asks for the largest backlog there is, which
  the kernel clamps, as Kino listens, to `net.core.somaxconn` on Linux
  (4096 by default since 5.4; a container's own counts) or
  `kern.ipc.somaxconn` on macOS (128), where Rust's standard library
  alone would leave 128 and Linux would drop the SYNs of a connect
  burst past it, their clients retrying a second later. While Kino is
  not accepting (at `max_connections`, or with its accept loop held up),
  up to that many clients finish their handshake and wait: overload
  shows as a slow first byte rather than a slow connect, a health check
  that only opens a TCP connection passes for longer, and the drain
  resets every connection still in the queue. The control plane's TCP
  listener keeps a queue of 128, so a probe of a stuck control thread
  fails at connect.
- Bounded request queue between tokio and Ruby. When it stays full past
  `queue_timeout`, the client gets an immediate 503 rather than waiting.
- Request bodies stream through a bounded(8) channel: hyper is only polled
  as fast as Ruby consumes (inbound backpressure costs nothing extra).
  Bodyless requests (most GETs) spawn no forwarder task at all.
- Response bodies stream through a bounded(8) channel the other way: a
  slow client makes `write_chunk` block—with the GVL released. A gone
  client (hyper dropped the body: the client hung up, the connection
  broke, an h2 stream was reset) disconnects the channel, and the next
  write raises `Kino::ClientGone`, which the worker loop takes as the
  normal end of the request.
- Giving up on a stream (a body that raised, a crashed worker, shutdown
  past its deadline) never waits for room in that channel: the error
  frame goes in if it fits, and either way a flag on the `Responder`
  makes the body end in an error once hyper drains it, so a client that
  stopped reading holds no thread. Only a body that returned finishes
  its stream, so a truncated response never ends cleanly. A write still
  waiting for room sees the flag within a tick and raises
  `Kino::ClientGone`. The abort borrows the request handle no more than
  a waiting write or read on another thread does, so it runs beside
  them; only what needs the handle to itself (a hijack, accepting or
  switching a WebSocket) raises then instead of panicking.

## Failure handling

Three parties can answer a client, coordinated by an atomic
first-claimant-wins flag on the per-request `Responder`:

1. The app, via the worker loop (normal path; `StandardError` is rescued
   in Ruby and becomes a clean 500).
2. The crash path: a worker thread that dies of a hard crash (any
   `Exception`, a `StandardError` that `Thread#raise` sends into it
   while it waits for its next request, or its thread killed, which
   no rescue sees but its loop's `ensure` does) 500s its in-flight
   requests on its way out, via the
   slot's in-flight list—not when GC eventually notices—and closes
   its WebSockets with 1011. The requests that waited in its lane go to the
   other workers' lanes, or, with no room in any, wait for some on the
   I/O runtime as a new arrival does (up to `queue_timeout`, then the
   503 a new arrival gets, counted in `rejected`). That wait starts
   `queue_timeout` over, by design, so behind workers that keep crashing
   a client can wait longer than `queue_timeout`. Only the threads Kino
   kills itself, past the shutdown deadline, and those the process kills
   as it exits, end no crash. Then it is replaced on a slot from the
   pool's bank while its siblings serve on: in `:threaded` mode by the
   pool, in `:ractor` mode by its ractor's main thread, which asks the
   ractor's supervisor thread (on the main ractor) for the slot. The
   supervisor reads those reports and `Ractor#monitor`'s end notice from
   one port (a report has a class of its own: the notice is a Symbol up
   to Ruby 4.0 and an Array from 4.1), and respawns a ractor that dies
   as a whole. A worker that keeps crashing within a second of starting,
   before it served a request, is respawned with a doubling delay (0.1 s
   up to 5 s), each thread paced by its own crashes.

   A crashed worker's slot is retired under its lane lock (the
   dispatcher skips it) and goes back to the bank, which hands it to a
   later worker only once it is quiet: no socket still owed its close,
   no hijacked connection open, its last worker's handle let go of it
   (so a late end never touches its successor), not quarantined. Crashes
   therefore do not grow the slot table, and with lanes a request that
   arrives while the replacement starts waits for a worker, up to
   `queue_timeout`. Each slot carries a
   generation that moves on whenever a worker leaves it, and a waker, a
   retirement or a quarantine names the generation it meant, so none of
   them can reach the worker that took the slot over.
3. A `Drop` guard on `RequestCtx` as the universal backstop (GC of an
   abandoned handle, teardown races). The Drop path never touches the Ruby
   API, so it is safe from any thread.

With `request_timeout` configured, the tokio front-end can additionally
answer with a 504 on its own when the response head misses the deadline;
the worker keeps running, and its late response goes nowhere harmlessly:
the front-end has stopped listening (the oneshot receiver is dropped),
and the worker's claim makes the Drop backstop a no-op.

Client aborts are handled the same way in reverse: hyper drops the request
future, and a Rust `Drop` guard keeps the in-flight counter honest (a
plain decrement after an `.await` would never run).

## Graceful shutdown

`stop_accepting` → every live connection switches to graceful shutdown
(finish the in-flight request, answer with `Connection: close` on
HTTP/1 or GOAWAY on h2, take nothing new—so drains converge instead
of chasing chatty keep-alive clients) → say goodbye to every native
WebSocket (its worker runs `on_shutdown`, then a 1001 follows, the
sockets taking turns over a second) → drain until
queue + in-flight reach zero and every socket's `on_close` has reached
its worker, or the deadline passes → `close_queue` (idle workers see
Disconnected and exit; the bridges of hijacked connections that
switched protocols close, so an app serving one on its worker reads
EOF) → join workers (a crash they died of is not raised again) → past
deadline: abort remaining clients (a 500, or a connection abort
mid-stream), interrupt blocked workers, reap stragglers → close the
other hijack bridges (a long poll still unanswered gets a 503, and
shutdown waits up to 100 ms for the bridges to end and hyper to take
their answers) → tear down the tokio runtime → stop the control plane
→ remove the pidfile. Idempotent, and one at a time: a call made
during a shutdown waits for it. A second INT/TERM force-exits.
`Server.run` (and so the `kino` executable and the Rack handler)
returns only once the shutdown has ended, and raises its error if a
step failed, so the process exits non-zero.

Process exit can then take about a second more, in Ruby's VM teardown,
when an app Ractor's main thread and another of its threads are both
blocked at exit (an event loop and a thread feeding it), or a worker
Ractor is still stuck past the deadline. Ending such app loops in an
`at_exit` hook avoids it.

## Unix sockets

A `unix://` bind, the client listener's or the control plane's, tells a
live socket from a stale one by a connect that never waits. One that is
answered, or on Linux turned away by a full accept queue, refuses the
boot with "socket is in use"; one that is refused means a crashed
server left the file, which is unlinked and bound over. A path that
holds anything but a socket is refused, never unlinked. Shutdown removes
the socket file.

Nothing locks the path, which leaves two limits. On macOS a connect to
a full accept queue is refused as one to a stale file is, so a second
server booted on the path of a live one whose queue is full takes the
path over (on Linux it is refused). And two servers booted at the same
instant on a stale path may both bind it, the first left serving a
socket no path names. A `pidfile` the two share refuses the second boot
in both cases.

## Intake deadlines

Before hyper serves a connection, Kino waits for enough of its client's
first bytes for hyper to choose a codec, as hyper-util's version sniff
has no timer: it waits for good on a connection that sends nothing, or
stops inside the 24-byte h2 preface. On TCP and unix connections that is
a nonblocking `recv` with `MSG_PEEK` of up to 24 bytes, enough once they
part from the preface (HTTP/1) or hold all of it (h2), or once the
connection ends. It is due 15 s after the accept, and past that the
connection closes unanswered (no 408), as at hyper's header timeout;
once the drain begins it closes at once unless enough is there. Then
hyper's own timer gives the head 15 s from when hyper starts reading it,
so a head is whole at most 30 s after the accept. That holds with
`http2 false` too, where the wait is what lets the drain close a fresh
connection at once: hyper's HTTP/1 codec keeps one to its header timeout.

A TLS handshake is due 10 s after the accept, and one whose client has
sent nothing closes at the drain. Then Kino serves the codec the client
chose by ALPN, with no sniff: for `http/1.1` the first byte is due 15 s
after the accept; for `h2`, whose codec has no preface timer either,
Kino reads the 24-byte preface itself within those 15 s and replays it.
Without ALPN it reads until the bytes tell HTTP/1 from h2. With
`http2 false` only HTTP/1 is offered. A client must speak what it
negotiated: the codec ends one that chose h2 and sends HTTP/1, or the
reverse, serving it nothing.

A connection whose first bytes are in by the accept, as they mostly
are, pays one peek; one whose are not pays a timer and the readiness
wait hyper's first read would have made. Once hyper serves an h2
connection, the pings under [HTTP/2](#http2) take over.

A client that ends its side (a FIN, `shutdown(SHUT_WR)`) once its
request is sent is taken as gone, as nginx takes one (its 499): hyper's
HTTP/1 codecs keep half-close off, so an end read while a request is
served closes the connection unanswered and drops the request's future,
and a request still waiting for room in the queue or a lane leaves it.
One already queued runs, and its answer goes nowhere; an answer written
before hyper reads the end still goes out. A connection that ends
before a whole request closes at once with no app call.

## HTTP/2

Plaintext connections go through hyper-util's auto builder, which picks
h2 by the 24-byte preface sniff (prior-knowledge h2c) and HTTP/1.x
otherwise; TLS connections get the codec ALPN chose,
or without ALPN the one the preface tells (see
[Intake deadlines](#intake-deadlines)); `http2 false` pins the HTTP/1
codec and skips the sniff entirely. Streams multiplex into the same
bounded queue as keep-alive requests, so h2 concurrency is bounded by
workers × threads exactly as h1's is, and the bounded body channels give
per-stream backpressure for free: while the forwarder blocks, hyper
withholds WINDOW_UPDATE and the client stalls.

**Pings.** hyper's h2 side has no idle timer, so a peer that is gone (a
phone off the network, a dropped NAT mapping) would hold its connection
and its `max_connections` permit for good. Kino turns on hyper's
keep-alive: a connection that has sent no request and no request body
for 30 s gets a PING, and one that leaves it unanswered for 20 s is
closed, its open streams with it, during a drain too. A client that
answers keeps its connection, with or without a stream open: unlike an
idle HTTP/1 connection, which hyper closes at its 15 s header deadline,
an idle HTTP/2 one stays as long as its client likes, bounded by
`max_connections` or by a fronting proxy's own idle timeout.

The env bridge fills SERVER_NAME/SERVER_PORT/HTTP_HOST from the request
URI's `:authority` (h2 requests carry no Host header), through the same
host LRU the Host-header path uses, keyed by the authority bytes and,
when they name no port, the local port, which becomes SERVER_PORT (a
process may serve a host on several ports); SERVER_PROTOCOL is the
interned "HTTP/2". h2 trailer frames are dropped (Rack has no trailer
surface), and the h2 codec itself rejects connection-ish headers before
they can reach the env.

The upload path needed one h2-shaped fix: `read_body` drains every
already-queued chunk in a single native call (one GVL round-trip and
one Ruby string per 64 KB read), because a body arriving as 16 KB DATA
frames otherwise paid one crossing per frame—that alone took h2
uploads from half of h1's throughput to parity. A knob sweep over
hyper's h2 codec (frame size, adaptive windows, window sizes) moved
nothing after that, so hyper's defaults stay.

SETTINGS_MAX_CONCURRENT_STREAMS is derived from slot capacity
(workers × threads, clamped to [8, 1024]) rather than hyper's flat 200:
a smart balancer sees the server's true admission, and a hostile client
cannot multiply one connection into hundreds of queued requests—the
h2 analogue of h1's one-request-per-connection shape. The codec's own
abuse bounds ship as hyper/h2 defaults and were reviewed: 16 KB header
lists, 20 pending remote resets then GOAWAY (rapid reset), per-second
reset-churn and empty-frame budgets.

Low-cardinality header values (UA, accept-*, sec-ch-*, sec-fetch-*)
are interned in an LRU of frozen strings—the env-side analogue of
HPACK's wire dedup, and the practical form of it: hyper does not expose
HPACK table indices, so the cache keys on value bytes and works for
HTTP/1 too. Cookie and authorization are deliberately excluded
(per-user cardinality, secret lifetime).

Deferred until benchmarks justify it: slot-aware flow-control window
grants (a memory/abuse lever, not a throughput one—the knob sweep
showed windows don't gate upload throughput).

## WebSockets

What an app sees, with every limit and close code, is in
[websockets.md](websockets.md); this is how it works inside.

**Claiming the connection.** hyper hands out one upgrade per request.
For an HTTP/1.1 request with an `Upgrade` header, the request's handler
takes it and spawns a task that holds it until the app claims the
connection, as a native socket or through `rack.hijack`, or answers
plainly (the task then ends); every other request pays one header
lookup.

**One worker per socket.** A native socket belongs to the worker slot
that accepted it: its callbacks run on that worker's thread, in order,
between its requests, so per-worker app state needs no lock. Each
socket runs as two tokio tasks. The reader hands the stream to
tungstenite's `WebSocketContext` (read side only, a 4 KiB read
buffer), which unmasks, joins fragments and enforces the message cap;
Kino answers pings and echoes closes itself and maps how reading ended
to its close code and cause. The writer drains the socket's queue,
bounded in frames and in bytes (counted from the write that queues a
data frame until the writer has written it), in vectored writes
straight from the queued frames, so a socket holds no write buffer.
Ruby holds only the socket's Integer id, so no TypedData crosses a
ractor, and a write is a lookup in one id-to-socket table.

**Inbox and bell.** A worker blocks on one queue only (waiting on two
would need a select, which loses wakeups), so its lane carries two kinds
of item: a request, or a bell. A socket's messages go into its worker's
bounded inbox, and one deduplicated bell goes into the lane to wake the
worker if it is parked. The take loop reads its inbox first, then a due
wake, then its lane, then steals. After eight events in a row, or one
wake, the worker owes its lane a look, so a socket that keeps sending
cannot hold requests or the wake hook up. A stealer that finds a
sibling's bell rings it again for its owner, so events never change
workers. A bell that meets a full lane is dropped: that worker is busy,
and it checks its inbox on every take and every 50 ms tick while
parked. A full inbox holds up the readers that feed it.

**Inbound credit.** The inbox is one FIFO per worker, so each socket
counts its unhandled messages: the reader takes a credit before it
reads a message and gets it back when Ruby is done with it (once
`on_message` returns or raises) or when an abandoned worker's inbox
drops it. Out of credit, the reader waits. The reader of a socket whose
worker was abandoned never waits, so it still reads the client's answer
to the 1011.

**Pings.** Each socket keeps the time its client was last heard from:
the reader's stream stamps it whenever a read brings bytes, and the
writer's when a write gets through after the connection made it wait
(only the client reading frees that room). A timer in the socket's task
queues each ping as a control frame outside the byte budget, and the
socket is dropped like an abort once the client has been silent for two
intervals and for one since the ping was written. A reader waiting on
its worker counts its client as heard from, and the task polls its
reader before its timer, so a pong that arrived as the deadline came
still counts.

**Flow control.** Each socket also counts its data frames from queue to
write (`ws.pending`). What the full queue refuses sets a flag bit in
that same counter; whoever sees the count at zero with the bit set
clears it and tells the worker: the writer after a batch, or the
refuser, which wakes the writer with an empty frame when the queue
emptied in between. One atomic holds both, so exactly one of them sees
the other. The writer never waits on the worker: a full inbox gets the
event from a task of its own.

**Handshakes.** A handshake goes to the live worker with the fewest
open sockets (ties keep the hot-lane order). Accepting registers the
socket and holds its 101 while `on_open` runs; the 101 then goes out
with the frames `on_open` queued behind it. Any other answer to an
accepted handshake (a 500, a `request_timeout` 504, the Drop backstop's
500) ends the socket and closes the connection.

**Topics and the fan-out thread.** `publish` copies topic and body into
a queue bounded at 65,536 publishes and 64 MiB, and returns. One native
thread takes publishes in order and, under the topic registry's read
lock, builds one frame per distinct envelope and queues refcounted
clones of it to every subscriber, never waiting for room: a full
subscriber is skipped or dropped through an out-of-band abort, never
through its own queue. The socket table and the topic registry are
never locked together, so a write never waits behind a fan-out. A
subscribe marks its socket subscribed under the registry lock and then
checks it is not gone; an unregistering socket marks itself gone and
then forgets its subscriptions only if it is subscribed. One of the two
sees the other's mark, so a racing subscribe gives up or is undone, and
a socket that never subscribed closes without the registry lock. A
Ruby thread that changes the registry while a fan-out holds it waits
with the GVL released: one blocked with the GVL held would stop its
ractor's other threads and, unable to reach a VM barrier, every ractor
at the next GC.

**The wake hook.** It rides the same bell: a wake sets a flag on the
slot and rings, so wakes before the hook runs coalesce into one take.
The waker names its slot's generation, so it wakes nothing once its
worker has left the slot. A park right after a wake-only take keeps the
worker's place in the hot-lane order, so periodic wakes do not reorder
dispatch.

**The hijack bridge.** A full hijack creates a Unix socket pair: Ruby
gets one end as a real IO, the bridge keeps the other. The bridge reads
the app's response head (httparse; at most 16 KiB and 100 headers, else
a 502) and replays it through hyper, which keeps TLS working. On a 101
it waits for hyper's upgrade and copies bytes both ways; any other head
must carry a Content-Length (all digits; repeated values must agree),
and the bridge streams that many bytes as the body, with
`Connection: close`. An answer that has no body (a 204, a 304, the
answer to a HEAD) needs no Content-Length. A hijacked request leaves
`in_flight` when the app takes it over: the hijack and the end of the
request's handler race on one flag on the responder, so the count drops
once. A 101 answered with a callable takes the same bridge, its head
handed over rather than written.

The drain closes the bridges in two steps, through one level on the
server that only rises. A switched bridge closes with the queues: its
exchange is over, and an app serving it on its worker must see EOF for
that worker to stop. Every other bridge closes once the workers have
stopped, so an app still on its worker gets to answer, and one still
unanswered then gets a 503. So does one whose app's end closes without
a word once requests have drained: a leaving worker Ractor's objects
are collected as it ends (on Ruby 4.1 at once, on 4.0 by the next GC).
Before that, such a close gets a 502. Once the workers have stopped,
the runtime may not have seen what an app wrote, so the bridge reads it
straight from the socket: an answer whose head had not gone out goes
out from memory if it is whole within the first MiB read, else the
request gets the 503; a body under way takes what is there, and is cut
if that falls short. Each bridge counts as running until hyper has
taken its answer (the count rides in the response's extensions and in a
streamed body, which hyper drops in the poll that writes them), and
teardown waits up to 100 ms for the count to reach zero. The
`max_connections` permit, and on an I/O shard the connection's load
count, travel inside the connection's stream wrapper, so an upgraded
connection keeps both for as long as it lives.

**Lifecycle.** Each slot counts the sockets whose `on_close` its worker
is still owed; the drain waits for those counts to reach zero. The
drain's goodbye runs in each socket's own task: told to leave with its
turn in the spread (evenly spaced by socket id), it hands the worker a
shutdown event when the handler has `on_shutdown`, waits for the
worker's word that it ran (or for the slot to be abandoned), and queues
the 1001 once both that and its turn have come. An abandoned slot's
events are dropped from then on; a slot handed to a new worker closes
whatever the last one left. A retirement that races a new socket closes
it, and the worker stays until that close has reached it.

**Counters.** The WebSocket counters are process-wide relaxed atomics
on paths sockets take anyway: the writer adds each batch's frames and
bytes, the reader's stream each read's bytes. Each socket keeps why it
ends in one atomic, and the first cause recorded stands: whoever asks
for the end records it before waking the socket's task, and the task
records what it saw itself. The registration counts the close under
that cause as it unregisters, then stops counting it open, so a
snapshot that reads none open has read every close. A streamed response
counts as cut once, on its responder's cut flag, unless its head says
it carries no body or never went out because Kino had answered first.

## Timer waits: `Kino.sleep`

MRI's `sleep` parks the thread on the VM timer, whose wakeups inside
non-main ractors are coarse (how coarse is environment-dependent; see
[benchmarks](benchmarks.md#why-io-lags-in-ractor-mode-on-linux)).
`Kino.sleep` releases the GVL and waits on the OS clock directly, chunked
at the interrupt tick so `Thread#kill` and shutdown stay responsive.

## Why tokio (researched June 2026)

- **tokio + hyper**: the bottleneck is the Ruby dispatch boundary, not raw
  I/O throughput; what matters is HTTP correctness, keep-alive, TLS, and
  h2 (since shipped, via hyper-util's protocol-auto builder)—hyper's
  territory. Cross-platform out of the box.
- **monoio**: thread-per-core io_uring looks great in echo-server
  benchmarks, but hyper only works through its poll-io compat layer
  (forfeiting io_uring on the hot path), and the share-nothing advantage
  is spent the moment requests fan into an MPMC queue toward Ruby.
- **compio**: completion-based, cross-platform, production-proven—but no
  first-class HTTP server story yet, and completion-model owned-buffer
  semantics would leak into the request lifecycle design.
- **ntex**: the strongest alternative—unlike monoio/compio it has a
  first-class HTTP/1.1 + HTTP/2 server stack (TechEmpower top tier) plus
  an io_uring runtime ("neon") on Linux today. Rejected as the default
  for now: its thread-per-core, `Rc`-based `!Send` worker model is
  exactly what our Send-ctx-into-MPMC dispatch opts out of; its own
  request/response/body types would force a conversion seam through
  `Responder` and the streaming path; neon is Linux-only (ntex-on-tokio
  elsewhere forfeits the io_uring win and just trades hyper's
  battle-tested h1 for a less-deployed one); and the realistic gain is
  confined to syscall-bound /plaintext-class traffic—the Ruby boundary,
  not the front-end, is where Kino's time goes. Worth a contained
  feature-flag spike if the Linux plaintext ceiling ever matters
  competitively.
- **io_uring path**: tokio ships in-tree io_uring as an unstable feature
  (file ops as of 1.52; network expected to follow). The `server` module
  isolates the runtime, so adopting it later is a contained change—and
  would deliver most of ntex/neon's win without the type seam.

## Versioning of risky dependencies

magnus is used for everything except the GVL-release primitives and the
`rb_ext_ractor_safe` flag, which go straight to rb-sys (magnus wraps
neither). magnus's lazy TypedData class cache is force-resolved at init
on the main ractor, so no worker ractor ever races its first resolution;
the only symbols the crate creates are made during `server_start`, also
on the main ractor.
