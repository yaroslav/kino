# Benchmarks: methodology and analysis

The result tables live in the [README](../README.md#benchmarks). This
document is the part that doesn't fit in a README: how the numbers were
produced, what they do and don't mean, and the investigations behind the
odd-looking columns.

The interesting question is not which server is faster—it's what a
Ractor-based dispatch model buys, and what it costs, relative to the
battle-tested approaches (Puma's forked workers, Falcon's
fiber-per-request). Puma is the reference point throughout because it's
the deployment most apps run today.

## Methodology

- Primary hardware: AWS **c7a.2xlarge**—8 dedicated cores of AMD EPYC
  9R14 (Genoa), 16 GB RAM, Amazon Linux 2023, kernel 6.18. A realistic
  app-server size, deliberately: nobody provisions a 32-core box per
  app process.
- Toolchain built on the box via mise: Ruby 4.0.5 (**YJIT enabled**,
  `RUBY_YJIT_ENABLE=1` for every server), Rust 1.96, Kino compiled in
  the release profile.
- Load generator: wrk 4.2 on the same host, 8-second windows, 64
  connections (`bench/run.sh 8 64`). Same-host load generation costs
  both sides CPU equally; we verified the generator was not the
  bottleneck by A/B-ing against single-threaded ab, which capped Kino's
  plaintext 26-37% lower while leaving Puma's number unchanged.
- Identical app for every server (`bench/bench_app.rb`), Ractor-shareable
  so Kino's `:ractor` mode can run it unmodified.
- Topology: each server at its shipped defaults—Puma 8 forked workers
  × 3 threads (24 slots) vs Kino 8 workers in one process (× 1 thread
  in ractor modes, the default since 0.1.1; × 3 threads in threaded
  mode). Equal-topology numbers (Kino at 8×3) are in the studies below.
- The headline tables also carry an io-tuned column (`workers 32,
  threads 1`)—not a default, labeled as such—because the /io rows are
  a slot-count story (see below).
- The dataset spans four identical c7a.2xlarge boxes: the original
  measurements, a re-measure at the 0.1.1 defaults, the headline sweep,
  and a final full re-validation (every table re-run from scratch).
  Equal-config throughput reproduced across boxes within ~1-2%.
- **Memory is reported as PSS (proportional set size), not RSS.** A Puma
  cluster forks N workers that share the Ruby VM and gem code
  copy-on-write; summing each worker's RSS counts those shared pages up
  to N times and overstates the cluster's real footprint. PSS divides
  every shared page across the processes mapping it, so it reflects the
  unique physical memory the cluster occupies—the only fair basis for
  comparing one process against a fork-per-core cluster. We read it from
  `/proc/<pid>/smaps_rollup` over the whole process tree, cross-checked
  against `ps` (RSS) and `smem` (PSS). Kino serves from one process, so
  its RSS ≈ PSS; the correction only moves Puma. (`bench/studies.sh`
  reports both columns.)
- Follow-up studies (`bench/studies.sh`): CPU tuning, topology sweep,
  /io worker scaling, logging costs, and memory—run in the same session
  as the headline tables.
- The harness waits for the port to be genuinely free between targets.
  This matters: falcon binds with `SO_REUSEPORT`, so a leftover instance
  silently splits traffic with the next server and poisons every number
  after it (we learned this the hard way).
- Secondary data point: macOS (MacBook Pro, M1 10-core), where every
  server converges near the loopback ceiling (~42-49k plaintext) and
  differences compress; its table is at the end of this document.
  Earlier published numbers from Docker-on-Mac are retired—real
  hardware contradicted several of that environment's findings, noted
  inline below where the conclusion changed.

## Reading the headline tables

These tables all run the **tiny synthetic Ractor-shareable app**. The real
Rails app is not Ractor-shareable and runs only in threaded fallback—a
separate story with separate numbers, in [its own section](#rails).

- **Plaintext/10k**: Kino's tokio front-end clears the fork cluster by
  1.5-2.1× (lanes plaintext 250,222 vs Puma 118,176 = 2.12×; the
  smallest margin is threaded /10k at 1.50×). At the old 3-thread
  topology the cross-ractor handoff showed up as ractor trailing
  threaded on trivial handlers; the 1-thread default reverses that
  (ractor 230k vs threaded 217k) and lanes widen it (250k).
- **CPU (recursive fib)**: ractor mode does **5.8× its own GVL-bound
  threaded mode** (77,999 vs 13,429)—that's the entire point of
  ractors—and beats the fork cluster outright: +34% with stock
  defaults (+22% with lanes, 70,885 vs 58,006). Even the io-tuned
  `workers 32` topology stays ahead of the cluster on CPU (66,100).
- **Memory (PSS)**: after the full endpoint battery, the tiny app costs
  Kino **148 MB** in ractor mode (107 MB threaded) against the 8-worker
  cluster's **1,068 MB**—~7-10× lighter, because a trivial app is almost
  all private per-worker heap that copy-on-write can't share. The real
  Rails app narrows this to ~4× (its framework *is* shared CoW); both are
  in [Memory under load](#memory-under-load-and-the-glibc-arena-footgun).
- **I/O (5 ms wait)**: all dispatch models tie within ~4% at equal slot
  counts, so the default columns show the ractor modes behind on /io
  (8 slots vs the cluster's 24), and the `workers 32` column shows the
  same engine winning (+25%, +34% via `Kino.sleep`) once it has more
  slots than the cluster. The lever is slot count, and Kino slots are
  cheap: see [below](#why-io-lags-in-ractor-mode-on-linux).

## CPU-bound tuning

On real hardware, Kino's stock defaults lead the cluster on pure
CPU—and the old tuning recipe is now obsolete. Same-session studies
run:

| config | /cpu req/s |
|---|---:|
| Puma cluster (reference) | 58,189 |
| Kino `workers 8, threads 3` (the default before 0.1.1) | 67,394 |
| Kino `workers 8, threads 1, tokio_threads 1` (the old recipe) | 68,600 |
| Kino `workers 8, threads 1`, tokio auto (**the default**) | **77,999** |

The `threads 1` half of the old recipe became the default; the
`tokio_threads 1` half now *costs* −12% on /cpu (and still costs
plaintext: 108,523 vs 230k). Don't pin tokio threads. **The recipe's
history is an environment story**: in the earlier Docker-on-Mac runs it
was worth +12%, because tokio threads and wake churn competed for
oversubscribed virtualized cores; on dedicated cores the same pin
starves the I/O front-end instead. If you deploy into a
constrained/virtualized environment, measure there.

Two findings that survived the environment change:

**Ruby executes at identical per-core speed in ractors and forks.** We
briefly believed parallel ractors paid a ~24% execution tax; that probe
compared 8 busy ractors against a *single* busy thread, which also
compares all-core clocks against single-core boost. The controlled probe
(8 forked processes vs 8 ractors, same all-core clock): forks 8,973
fib/s/core, ractors 8,918—identical. `GC.disable` changes nothing (fib
barely allocates). The VM is innocent. Never compare parallel against
single-threaded baselines without controlling for clocks.

**The GVL ceiling is absolute.** Threaded mode posts the same ~13k /cpu
whatever the topology—24 threads in one process serialize on one lock.
Parallelism for CPU-bound Ruby comes from ractors or forks, nothing else.

## Why /io lags in ractor mode on Linux

On bare metal the gap is small at equal slot counts: ractor /io 4,530
vs threaded 4,709 (−4%, both at 8×3). In Docker it was −18%, and a
pure-Ruby probe there measured
`sleep(0.005)` waking +2.3-2.8 ms late inside ractors vs +1.8 ms on the
main ractor—non-main-ractor timer wakeups are coarser in Ruby 4.0, but
how much that costs depends heavily on the kernel/virtualization stack.
A follow-up probe showed `IO.select`-style waits are tighter than
`sleep` inside ractors, so real I/O readiness suffers less than timers.

**Mitigation 1—`Kino.sleep`:** releases the GVL and waits on the OS
clock directly (chunked, so `Thread#kill`/shutdown stay responsive). The
`/io_native` endpoint (same 5 ms wait via `Kino.sleep` when available)
erases the remaining ractor gap on this box: 4,721 vs 4,530 plain sleep.

**Mitigation 2—add workers; they're nearly free.** The headline tables
show default ractor-mode /io at 1,552: that's 8 slots (the 1-thread
default) against the cluster's 24, because wait-bound throughput is
simply `slots ÷ effective wait`. Kino's slots cost ~a thread each, not
a forked process: the `workers 32, threads 1` column measured **5,888
/io (+25% over the 24-thread cluster's 4,693) and 6,274 /io_native
(+34%)**, still one small process, and still +14% ahead of the cluster
on pure CPU. Its cost is the CPU-light rows (183k plaintext vs 230k at
8×1: 32 ractors oversubscribe 8 cores). A fork cluster buying the same
32 slots pays for them in full copies of the app; Kino pays in
scheduler churn only where the cores are already saturated.

## The ractor-pool-wrapper comparison

A reasonable first experiment for anyone curious about ractors is a Rack
wrapper that ships each request to a ractor pool on whatever server they
already run. `bench/ractor_wrapper.rb` is that experiment, benchmarked on
Puma and Falcon—not as a comparison of those servers, but to measure
what the Rack-level hop itself costs (c7a.2xlarge, same session):

| endpoint   | Kino :ractor (8×3) | Puma + wrapper | Falcon + wrapper |
|------------|-------------------:|---------------:|-----------------:|
| /plaintext |            193,826 |         19,480 |           99,776 |
| /cpu (fib) |             68,061 |         17,755 |           48,721 |
| /io (5 ms) |              4,530 |          1,454 |            1,549 |

Inside the Rack contract, the wrapper must reduce the env to a shareable
subset, copy it to the worker ractor, copy the response back, and hold a
server thread for the round trip—that's the 10× gap in the Puma
column, and it would be the same for any server in that position. The
Falcon numbers mostly show its per-core forks doing the work (the
per-fork ractor adds little), while `Port#receive` blocking its event
loop is what limits the I/O endpoints—ractors and fiber schedulers
don't compose yet, which is a Ruby-level limitation, not a Falcon one.
Our conclusion: ractor dispatch needs to live at the server layer, below
the Rack contract—which is the experiment this gem exists to run.

## Rails

Rails is **not Ractor-shareable**, so Kino can only serve it in
`:threaded` fallback—this whole section is one GVL-bound Kino process,
never ractor mode. The example app (`examples/rails-hello`, edge Rails,
production mode, 8 workers × 5 threads) on the same box:

| | req/s | RSS | PSS |
|---|---:|---:|---:|
| Kino `:threaded` (one process) |  2,637 |  97 MB | **92 MB** |
| Puma cluster (8 workers) | 12,138 | 794 MB | **389 MB** |

This is the honest version of the Rails story. In threaded mode Kino is
one GVL-bound process, so the fork cluster outruns it ~4.6× by using all
8 cores—at ~4× the memory by PSS. The metric matters here: Puma's RSS
(794 MB) counts the shared Rails framework once per worker; PSS (389 MB)
counts it once, and that is the fair figure (the README's headline used
to read 8× off RSS). Preloading barely moves it—389 MB with
`preload_app!` vs 400 MB without—because Ruby's GC dirties most heap
pages, breaking copy-on-write, so even a preloaded cluster keeps a
private heap per worker. Rails-on-Ractors is interesting precisely
because it would close the throughput gap at the one-process memory
cost; the upstream blockers are documented in
[rails-on-ractors.md](rails-on-ractors.md).

## YJIT × Ractors gotcha (found the hard way)

Plain `def` methods get the full YJIT speedup inside worker ractors
(5.8× in our probe—YJIT + Ractors compose fine in Ruby 4.0). But a
**self-referential lambda** (`fib = ->(x) { ... fib.call(x - 1) ... }`)
runs *slower* with YJIT than without when shared across parallel
ractors. Keep hot-path code in methods (which real apps do anyway); our
own /cpu benchmark was a victim of this pattern until it wasn't.

## Rust-side allocator: mimalloc

The native extension's global allocator is **mimalloc**, unconditionally.
It covers all Rust-side allocations—request/response buffers, hyper,
tokio, channels—not the Ruby heap. The decision came from a three-way
shoot-out (measured in the earlier Docker-on-Linux environment, one
container session):

| allocator | /plaintext | /10k | /cpu |
|-----------|-----------:|-----:|-----:|
| system (glibc) | 131,978 | 114,121 | 45,690 |
| **mimalloc** | **145,229** | 113,907 | 45,351 |
| jemalloc | 134,632 | 111,341 | 47,273 |

mimalloc won plaintext by ~10% with everything else flat, with no
downside measured. For the record, jemalloc inside a Ruby extension also
needs its `disable_initial_exec_tls` build flag just to load (dlopen +
initial-exec TLS = `cannot allocate memory in static TLS block`)—one
more reason to prefer mimalloc in dlopen'd extensions.

## Memory under load (and the glibc arena footgun)

All figures are **PSS** (see [Methodology](#methodology)) after the full
endpoint battery (8 s each of /plaintext, /10k, /cpu, /io—a "warmed
production process", not a fresh boot, which measures ~26 MB for every
Kino mode). RSS is shown alongside so the copy-on-write correction is
visible.

### The tiny synthetic app

| config | RSS | PSS |
|---|---:|---:|
| Kino :ractor 8×1 (default) | 151 | **148** |
| Kino lanes 8×1 | 137 | **135** |
| Kino :ractor 8×3 | 171 | **169** |
| Kino :threaded 8×3 (`MALLOC_ARENA_MAX=2`) | 109 | **107** |
| Kino :threaded 8×3 (no arena cap) | 668 | **666**¹ |
| Puma cluster 8×3 | 1,213 | **1,068** |

The tiny app is ~7× lighter than the cluster in ractor mode, ~10× in
arena-capped threaded mode. RSS ≈ PSS for every Kino row (one process,
nothing to share) and within ~12% for Puma here: a trivial app has almost
no shared state, so Puma's footprint is ~1,051 MB of *private* per-worker
heap plus only ~18 MB shared (which RSS counts 8×). This is the case where
copy-on-write does **not** rescue the cluster—there is nothing to
share—so the RSS and PSS numbers nearly agree. (The old "80 MB / 15×"
figure was a lighter, plaintext-only load; the honest full-battery ractor
figure is ~148 MB, i.e. ~7×.)

¹ Not a leak: glibc malloc arena bloat. One 8-second /10k round takes
threaded mode from ~70 MB to ~670 MB and it never returns—24 threads
churning 10 KB strings through one process heap is the textbook glibc
arena-fragmentation case (the reason Rails ops set `MALLOC_ARENA_MAX=2`;
Heroku ships that default). With the cap the same battery ends at 107 MB
PSS, throughput unchanged. Ractor mode sidesteps the worst of it without
any env tweak—objects live in per-ractor heaps.

### Rails (threaded fallback)

Here copy-on-write **does** matter, which is exactly why PSS is mandatory:

| config | RSS | PSS |
|---|---:|---:|
| Kino :threaded (one process) |  97 |  **92** |
| Puma cluster 8×3 (preload) | 794 | **389** |

Puma serves the same Rails framework from 8 forks that share it
copy-on-write; RSS counts that shared framework once per worker (794 MB),
PSS counts it once (389 MB). The fair ratio is **~4×**, not the ~8× a
naive RSS sum reports—this is the correction that prompted the whole
re-measure. Preload barely helps (389 vs 400 MB without): Ruby's GC
dirties most heap pages, breaking copy-on-write, so even a preloaded
cluster keeps a large private heap per worker. That is why "CoW should
make a fork cluster nearly free" is only half true—it shares the code,
not the live object heap.

## Run-to-run variance (a.k.a. "is this a regression?")

Rule of thumb from chasing this twice: never compare numbers from
different sessions; interleave A/B rounds in one session instead. The
Docker-on-Mac environment swung ±10% on /cpu between sessions with the
VM's mood; the dedicated c7a box is far steadier (same-session repeats
land within ~1-2%), but the discipline stays—every comparative claim in
these docs comes from same-session pairs. Cross-box repeatability got
its own test: the dataset was measured across four identical
c7a.2xlarge boxes, and equal-config throughput numbers matched within
~1-2% (loaded-memory measurements swing more with heap-growth
timing—treat them as ballpark). The same discipline caught the recurring
threaded-plaintext fluke twice: once 28% low on an earlier box, and again
in the final re-validation (170k, where three interleaved re-runs put it
back at 217k). Suspect cells get re-measured, not published.

## Topology notes

Measured on c7a.2xlarge, plaintext, ractor mode, same session (three
interleaved rounds, medians): `8×3` (workers×threads) = 198,478, `8×1`
= **229,966 (+16%)**, `16×1` = 214,391. Threads inside one ractor share
its lock, so every request handled by a 3-thread ractor pays a lock
handoff that a 1-thread ractor doesn't (`perf` in the earlier Docker
sessions attributed ~10% of cycles to
`rb_native_mutex_unlock`/`thread_sched_wakeup_next_thread` at 8×3; the
gain reproduced on two separate boxes, +16-17% each). **This is why
`threads` defaults to 1 in ractor mode since 0.1.1** (/cpu gains +16%
the same way: 77,999 vs 67,394). The trade-off is /io at low worker
counts: 1,552 at 8×1 vs 4,530 at 8×3—threads-per-ractor exist for
handlers that block on I/O. If yours wait a lot, raise `workers`
instead (32×1 beats even the 24-slot cluster, see above); slots are
cheap. (16×1 being worse than 8×1 on plaintext also says the shared
MPMC queue is *not* the bottleneck—8 extra parked consumers just add
scheduler churn.)

## What profiling tried and rejected

A perf profile of saturated plaintext showed ~20% of cycles in futex
wakeups: at saturation the queue oscillates around empty, so nearly every
request parks its worker and pays two wake round-trips (tokio firing the
channel signal + the GVL reacquire). The textbook fix—a bounded
busy-poll before parking—measured **worse** (-13% on both modes): with
workers + tokio threads oversubscribing the cores, spinners steal exactly
the CPU the event loop needs. Parking is the cheaper evil when threads
outnumber cores; the fix that did land is the fused
`respond_and_take` call (answer the previous request and block for the
next in one FFI crossing) plus the opt-in `batch` directive for grabbing
several queued requests per visit (default 1—values above 1 add
head-of-line blocking behind slow handlers and stretch effective queue
depth, which is also why it's a config knob and not a hardcoded win).

## Lane dispatch (experimental: `lanes true`)

The shared-queue design pays a futex wake per request at saturation
(every worker parks on an empty queue between requests). Lane mode gives
each worker a small private queue; the dispatcher prefers *awake* lanes—so
a hot worker keeps taking without ever being woken—with two
safeguards: lane depth is capped at 4, and workers steal from siblings
before parking (plus on every park tick), so a slow request can't strand
its lane's backlog.

Same-session A/B on c7a.2xlarge, ractor mode at the default topology
(8×1):

| endpoint | shared queue | lanes | delta |
|----------|-------------:|------:|------:|
| /plaintext | 229,534 | **250,222** | **+9%** |
| /10k | 178,083 | 189,862 | +7% |
| /cpu | **77,999** | 70,885 | −9% |
| /io | 1,552 | 1,551 | flat |

Lanes' margin shrank with the move to 1-thread workers (at the old 8×3
it was +21% plaintext: 240,193 vs 199,032 in the same session)—most of
the futex pain lanes were built to avoid came from thread handoffs
inside each ractor, and the new default removes those for everyone. At
the default, lanes still post the fastest plaintext/10k of any Kino
configuration, but plain shared-queue now takes /cpu. It stays opt-in because overload semantics differ from the
shared queue (`queue_depth` doesn't apply; capacity is lanes × 4 with
brief dispatcher retries up to `queue_timeout` before the 503), and
crash semantics, stealing fairness, and drain behavior have spec
coverage but not production mileage. (On loopback-bound macOS, lanes
lose a few percent instead; see the secondary table below.)

## Logging costs

Measured at full plaintext saturation (one log line per request—rates
that no real deployment logs at; treat these as worst-case ceilings, not
typical costs):

| case (8×3, same session) | req/s |
|---|---:|
| threaded, no logging | 219,168 |
| threaded, `log_requests true` (native access log) | 193,998 (−11%) |
| ractor, access log off / on | 197,596 / 181,050 (−8%) |
| app logs 1 line/req via shared `::Logger` (file) | **62,961** |
| app logs 1 line/req via `Kino::Logger` (file) | **149,519 (2.4×)** |

The shared-`::Logger` cost is the mutex: 24 worker threads serialize
through one lock plus a write syscall per line. `Kino::Logger` hands the
formatted line to a lock-free channel and returns—the remaining cost vs
not logging at all is Ruby-side formatting, which no device can remove.
(In the Docker environment the same comparison showed 8.5×—overlay-fs
write latency punished the synchronous logger far harder than this box's
NVMe does. The ranking is environment-independent; the multiple is not.)

One trade-off worth knowing: the sink **never blocks** request threads,
so at absurd rates against a slow disk it drops lines once its 8192-line
buffer is full, while `::Logger` writes every line by strangling
throughput instead. Pick your failure mode; at sane logging rates
neither happens.

Puma comparison note: request logging is opt-in there too (`--quiet` is
the default, `-v/--log-requests` enables it)—Kino's default-off
`log_requests` matches the ecosystem's standard behavior.

## HTTP/2

`bench/h2.sh`, Docker/Linux only (the load generator runs inside the
container). Every lane is measured with h2load so the generator is
identical everywhere: h2 lanes run 8 connections × 8 concurrent
streams, h1 lanes 64 connections — the same total in-flight. Servers
without native h2 get the standard pattern instead: nginx terminating
h2 and proxying HTTP/1.1 upstream over keep-alive. One labeled run
(kino ractor 8×3, falcon `--count 8`, puma `-w 8 -t 3:3`, 5 s/lane);
re-run the whole script for close calls, per the variance section.

| target (h2 unless noted) | /plaintext | /10k | /big-cookie | /upload (64 KB) |
|---|---:|---:|---:|---:|
| kino h2c | 191,563 | 140,628 | 169,961 | 26,484 |
| kino h1 cleartext (same boot) | 127,843 | 106,333 | 118,509 | 26,511 |
| kino h2 TLS | 169,469 | 109,963 | 154,316 | 21,776 |
| kino h1 TLS (same boot) | 108,908 | 85,009 | 99,674 | 20,953 |
| falcon TLS (native h2) | 59,371 | 37,806 | 49,666 | 18,997 |
| nginx h2 → puma h1 | 80,498 | 66,448 | 102,633 | 1,551 |
| nginx h2 → kino h1 | 106,371 | 59,513 | 76,135 | 1,485 |

What the numbers say:

- **Native h2 beats h1 on the same server by ~30–50%** on
  response-dominated lanes: the same 64 in-flight requests ride 8
  connections instead of 64, so frames batch into fewer, larger
  syscalls. The `/big-cookie` lane (a ~2 KB cookie per request) shows
  HPACK on top of that: the cookie crosses the wire once per
  connection, not once per request.
- **Native h2 beats proxied h2 by ~60%** with the *same backend*: the
  nginx→kino-h1 lane is the proxy-cost control, and the extra hop,
  re-parse, and re-serialize cost ~60k req/s on /plaintext.
- **Uploads run at h1 parity** — but only after a fix this lane
  caught: h2 delivers bodies as 16 KB DATA frames, and `read_body`
  originally crossed the GVL once per chunk, halving upload
  throughput. It now drains every queued chunk per crossing
  (doc/architecture.md), and a knob sweep over hyper's h2 codec
  (frame size, adaptive/bigger windows) moved nothing afterwards.
  nginx's h2 upload collapse (~1.5k) is its default-config
  request-body flow control; tune `http2_body_preread_size`/buffering
  before drawing conclusions there.
- falcon lands at roughly a third of kino-h2-TLS on fast handlers and
  behind on uploads. Single-run caveat: the nginx lanes showed ±20%
  swings between runs (puma's big-cookie beating its 10k here is that
  noise, not a signal); the kino-vs-kino and kino-vs-proxy ratios were
  stable across three runs.

## Hot-path notes

For the curious, the dispatch-path work behind the numbers: a try-pop
fast path skips the GVL release when a request is already queued,
bodyless requests spawn no body-forwarder task, `TCP_NODELAY`, a frozen
(Ractor-shareable) cache of env keys + common header names built once at
init, response headers read in place across the FFI boundary, ahash for
per-request lookups, SmallVec for header joins. Details in
[architecture.md](architecture.md).

## Secondary data point: macOS

MacBook Pro (M1 10-core), ab with keep-alive, 10 workers × 3 threads.
Everything converges near the loopback ceiling and differences compress;
useful mainly as a sanity check that the ranking holds on a second OS:

| endpoint    | Kino :ractor | + lanes | Kino :threaded | Puma (cluster) |
|-------------|-------------:|--------:|---------------:|---------------:|
| /plaintext  |       48,441 |  44,000 |         49,352 |         44,594 |
| /10k        |       45,840 |  42,362 |         46,482 |         42,890 |
| /cpu (fib)  |       43,827 |  41,426 |         10,076 |         34,161 |
| /io (5 ms)  |        4,943 |   4,883 |          4,890 |          4,780 |
| /io_native  |        4,758 |   4,844 |          4,848 |          4,805 |

Notes from this environment: lanes lose a few percent (the loopback
stack, not dispatch, is the ceiling); macOS loopback occasionally stalls
entire benchmark windows under back-to-back runs (the harness sleeps
between endpoints to reduce this; treat any isolated collapsed cell as
suspect and re-run); the ractor /cpu margin over the cluster (+28%) is
wider than on Linux because macOS Puma forks pay a higher per-fork cost.