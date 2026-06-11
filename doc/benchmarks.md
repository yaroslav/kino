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
- Topology held equal: Puma 8 forked workers × 3 threads vs Kino
  8 workers × 3 threads in one process.
- Follow-up studies (`bench/studies.sh`): CPU recipe, topology sweep,
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

- **Plaintext/10k**: Kino's tokio front-end clears the fork cluster by
  1.4-2× (lanes plaintext 241,501 vs Puma 117,838 = 2.05×; the smallest
  margin is threaded /10k at 1.44×). The cross-ractor handoff shows up
  as ractor (201k) trailing threaded (218k) on trivial handlers—nothing
  in them needs parallel Ruby—and lane dispatch reverses that (241k).
- **CPU (recursive fib)**: ractor mode does **5× its own GVL-bound
  threaded mode** (66,735 vs 13,298)—that's the entire point of
  ractors—and beats the fork cluster outright: +15% with stock
  defaults, +21% with lanes (70,373 vs 58,207).
- **Memory**: serving the same loaded bench app, Kino held **57 MB
  (ractor) / 50 MB (threaded)** where the 8-worker cluster held
  **1,078 MB**—a fork per core pays one full copy of the VM, the app,
  and its YJIT-compiled code per worker. On the Rails hello-world:
  Kino 97 MB vs cluster 797 MB.
- **I/O (5 ms wait)**: all dispatch models tie within ~4% at equal slot
  counts; the lever that matters is slot count, see
  [below](#why-io-lags-in-ractor-mode-on-linux).

## CPU-bound tuning

On real hardware, Kino's stock defaults already lead the cluster on
pure CPU—same-session studies run:

| config | /cpu req/s |
|---|---:|
| Puma cluster (reference) | 58,376 |
| Kino `workers 8, threads 3`, tokio auto (the default at the time) | 68,257 |
| Kino `workers 8, threads 1, tokio_threads 1` (recipe) | 68,629 |

The tuned recipe is a wash (+0.5%)—and it still costs plaintext
(112,815 vs ~200k) and /io (1,532, 8 slots): on this hardware there is
no reason to use it. **This is a finding that changed with the
environment**: in the earlier Docker-on-Mac runs the recipe was worth
+12%, because tokio threads and wake churn competed for oversubscribed
virtualized cores. If you deploy into a constrained/virtualized
environment, the recipe may still pay; measure there.

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

On bare metal the gap is small: ractor /io 4,527 vs threaded 4,715
(−4%). In Docker it was −18%, and a pure-Ruby probe there measured
`sleep(0.005)` waking +2.3-2.8 ms late inside ractors vs +1.8 ms on the
main ractor—non-main-ractor timer wakeups are coarser in Ruby 4.0, but
how much that costs depends heavily on the kernel/virtualization stack.
A follow-up probe showed `IO.select`-style waits are tighter than
`sleep` inside ractors, so real I/O readiness suffers less than timers.

**Mitigation 1—`Kino.sleep`:** releases the GVL and waits on the OS
clock directly (chunked, so `Thread#kill`/shutdown stay responsive). The
`/io_native` endpoint (same 5 ms wait via `Kino.sleep` when available)
erases the remaining ractor gap on this box: 4,714 vs 4,527 plain sleep.

**Mitigation 2—add workers; they're nearly free.** Wait-bound
throughput is simply `slots ÷ effective wait`, and Kino's slots cost ~a
thread each, not a forked process: `workers 32, threads 1` measured
**5,922 /io (+27% over the 24-thread cluster's 4,672) and 6,254
/io_native (+34%)**, still one small process. A fork cluster buying the
same 32 slots pays for them in full copies of the app.

## The ractor-pool-wrapper comparison

A reasonable first experiment for anyone curious about ractors is a Rack
wrapper that ships each request to a ractor pool on whatever server they
already run. `bench/ractor_wrapper.rb` is that experiment, benchmarked on
Puma and Falcon—not as a comparison of those servers, but to measure
what the Rack-level hop itself costs (c7a.2xlarge, same session):

| endpoint   | Kino :ractor | Puma + wrapper | Falcon + wrapper |
|------------|-------------:|---------------:|-----------------:|
| /plaintext |      201,472 |         19,425 |          100,624 |
| /cpu (fib) |       66,735 |         17,106 |           49,083 |
| /io (5 ms) |        4,527 |          1,447 |            1,549 |

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

The example app (`examples/rails-hello`, edge Rails, production mode,
8 workers × 5 threads) on the same box:

| | req/s | RSS under load |
|---|---:|---:|
| Kino `:threaded` (one process) | 2,298 | **97 MB** |
| Puma cluster (8 workers) | 11,923 | 797 MB |

This is the honest version of the Rails story: in threaded mode Kino is
one GVL-bound process, so the fork cluster outruns it ~5× by using all
8 cores—at 8× the memory. Rails-on-Ractors is interesting precisely
because it would close that throughput gap at the one-process memory
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

## Run-to-run variance (a.k.a. "is this a regression?")

Rule of thumb from chasing this twice: never compare numbers from
different sessions; interleave A/B rounds in one session instead. The
Docker-on-Mac environment swung ±10% on /cpu between sessions with the
VM's mood; the dedicated c7a box is far steadier (same-session repeats
land within ~1-2%), but the discipline stays—every comparative claim in
these docs comes from same-session pairs.

## Topology notes

Measured on c7a.2xlarge, plaintext, ractor mode, same session: `8×3`
(workers×threads) = 199,470, `8×1` = **232,469 (+17%)**, `16×1` =
214,284. Threads inside one ractor share its lock, so every request
handled by a 3-thread ractor pays a lock handoff that a 1-thread ractor
doesn't (`perf` in the earlier Docker sessions attributed ~10% of cycles
to `rb_native_mutex_unlock`/`thread_sched_wakeup_next_thread` at 8×3;
the +17% reproduces exactly on real hardware). Threads-per-ractor exist
for handlers that block on I/O; if yours don't, run `threads 1` and let
workers = cores do the parallelism. (16×1 being worse than 8×1 also says
the shared MPMC queue is *not* the bottleneck—8 extra parked consumers
just add scheduler churn.)

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

Same-session A/B on c7a.2xlarge (ractor mode):

| endpoint | shared queue | lanes | delta |
|----------|-------------:|------:|------:|
| /plaintext | 201,472 | **241,501** | **+20%** |
| /10k | 156,635 | 183,564 | +17% |
| /cpu | 66,735 | 70,373 | +5% |
| /io | 4,527 | 4,530 | flat |

On this hardware lanes make ractor mode the fastest Kino configuration
outright—+11% over threaded mode's plaintext, where the shared queue
trails it. (On loopback-bound macOS, lanes lose a few percent instead;
see the secondary table below.) It stays opt-in for now because overload
semantics differ from the shared queue (`queue_depth` doesn't apply;
capacity is lanes × 4 with brief dispatcher retries up to
`queue_timeout` before the 503), and crash semantics, stealing fairness,
and drain behavior have spec coverage but not production mileage.

## Logging costs

Measured at full plaintext saturation (one log line per request—rates
that no real deployment logs at; treat these as worst-case ceilings, not
typical costs):

| case (8×3, same session) | req/s |
|---|---:|
| threaded, no logging | 217,113 |
| threaded, `log_requests true` (native access log) | 193,200 (−11%) |
| ractor, access log off / on | 198,624 / 183,565 (−8%) |
| app logs 1 line/req via shared `::Logger` (file) | **62,962** |
| app logs 1 line/req via `Kino::Logger` (file) | **150,810 (2.4×)** |

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