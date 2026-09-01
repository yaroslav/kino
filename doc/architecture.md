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
each connection stays for its lifetime. Ruby never touches a socket. Each
request becomes a Rust-side `RequestCtx` pushed to a bounded flume MPMC
queue; Ruby workers pull from it.

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
- Experimental `lanes true` replaces the one shared queue with a small
  private queue per worker slot (awake-preferring dispatch, work
  stealing); see [benchmarks](benchmarks.md#lane-dispatch-experimental-lanes-true).

## The Rust ↔ Ruby boundary

- **No native (TypedData) handle crosses a ractor boundary.** Worker
  ractors receive plain integers (server id, worker ids) plus the
  Ractor-shareable app; native state lives in a global Rust-side registry
  keyed by those ids. The per-request handle
  (`Kino::Native::Request`, a TypedData object) is created *inside* the
  worker ractor by the take calls (`take_one`/`take_batch`), so its
  ownership is correct by construction.
- **Blocking discipline:** every blocking native call goes through
  `rb_thread_call_without_gvl` (rb-sys; magnus doesn't wrap it) so a
  blocked worker holds no VM lock. Waits poll an atomic interrupt flag
  between bounded `recv_timeout` ticks; the unblock function (UBF) just
  sets the flag. `flume::Selector` lost wakeups under sustained load
  (workers went permanently deaf to a non-empty queue after ~100k
  requests) and is not used anywhere.
- **Fast path:** when a request is already queued, `take_one` takes it
  with `try_recv` while still holding the GVL—the release/reacquire pair
  (two scheduler round-trips) is skipped entirely. Under load this is the
  common case.
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

## Backpressure, in both directions

- Bounded request queue between tokio and Ruby. When it stays full past
  `queue_timeout`, the client gets an immediate 503 rather than waiting.
- Request bodies stream through a bounded(8) channel: hyper is only polled
  as fast as Ruby consumes (inbound backpressure costs nothing extra).
  Bodyless requests (most GETs) spawn no forwarder task at all.
- Response bodies stream through a bounded(8) channel the other way: a
  slow client makes `write_chunk` block—with the GVL released.

## Failure handling

Three parties can answer a client, coordinated by an atomic
first-claimant-wins flag on the per-request `Responder`:

1. The app, via the worker loop (normal path; `StandardError` is rescued
   in Ruby and becomes a clean 500).
2. The supervisor: each worker ractor has a supervisor thread blocked in
   `Ractor#value`. A hard crash (any `Exception`) wakes it; it immediately
   500s the crashed ractor's in-flight requests via a `Weak<Responder>`
   side table—not when GC eventually notices—and respawns the ractor
   with fresh slots.
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
of chasing chatty keep-alive clients) → drain until queue + in-flight
reach zero or the deadline passes → `close_queue` (idle workers see
Disconnected and exit) →
join workers → past deadline: abort remaining clients (a 500, or a
connection abort mid-stream), interrupt blocked workers, reap
stragglers → tear down the tokio runtime. Idempotent;
a second INT/TERM force-exits.

## HTTP/2

One connection builder serves every protocol: hyper-util's auto builder
picks h2 by ALPN on TLS connections, by the 24-byte preface sniff on
plaintext (prior-knowledge h2c), and HTTP/1.x otherwise; `http2 false`
pins the HTTP/1 codec and skips the sniff entirely. Streams multiplex
into the same bounded queue as keep-alive requests, so h2 concurrency
is bounded by workers × threads exactly as h1's is, and the bounded
body channels give per-stream backpressure for free: while the
forwarder blocks, hyper withholds WINDOW_UPDATE and the client stalls.

The env bridge fills SERVER_NAME/SERVER_PORT/HTTP_HOST from the request
URI's `:authority` (h2 requests carry no Host header), through the same
host LRU the Host-header path uses, keyed by the authority bytes;
SERVER_PROTOCOL is the interned "HTTP/2". h2 trailer frames are dropped
(Rack has no trailer surface), and the h2 codec itself rejects
connection-ish headers before they can reach the env.

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
  (file ops as of 1.52; network expected to follow). `server.rs` isolates
  the runtime, so adopting it later is a contained change—and would
  deliver most of ntex/neon's win without the type seam.

## Versioning of risky dependencies

magnus is used for everything except the GVL-release primitives and the
`rb_ext_ractor_safe` flag, which go straight to rb-sys (magnus wraps
neither). magnus's lazy TypedData class cache is force-resolved at init
on the main ractor, so no worker ractor ever races its first resolution;
the only symbols the crate creates are made during `server_start`, also
on the main ractor.
