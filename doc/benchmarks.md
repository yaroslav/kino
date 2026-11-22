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

Every table in the README, and every figure here that is not marked
with an older date, comes from one **2026-11 re-measurement**:

- Hardware: AWS **c8a.2xlarge**—8 cores of AMD EPYC 9R45 (one thread
  per core), 16 GB RAM, Amazon Linux 2023, kernel 6.18. A realistic
  app-server size, deliberately: nobody provisions a 32-core box per
  app process.
- Ruby: **4.0.9** (the released Ruby, and the primary number
  everywhere), and a development build of **Ruby 4.1** (`4.1.0dev`,
  master f2d62c677e of 2026-11-21), whose figures are kept apart in
  [their own section](#ruby-41-development-build). **YJIT enabled**
  (`RUBY_YJIT_ENABLE=1`) for every server. Kino is compiled in the
  release profile against each Ruby.
- Versions: Kino 0.7.2 plus the unreleased changes in the changelog,
  Puma 8.0.2, Falcon 0.57.0, rack 3.2.7, wrk 4.2.0, h2load from nghttp2
  1.59.0, nginx 1.30.7. The Ruby 4.0.9 runs used a Kino build one
  revision behind the Ruby 4.1 runs; the two differ in how the build
  detects Ruby 4.1 and in comments, not in the request path.
- Identical app for every server (`bench/bench_app.rb`), Ractor-shareable
  so Kino's `:ractor` mode can run it unmodified.
- Topology: **every server at 8 workers**. Puma forks 8 workers × 3
  threads (24 slots); Kino runs 8 workers in one process (× 1 thread in
  ractor modes, the default since 0.1.1; × 3 threads in threaded mode).
  The main table also carries an io-tuned column (`workers 32, threads
  1`)—not a default, labeled as such—because the /io rows are a
  slot-count story (see below).
- Load generator: wrk on the same host, 8 threads, 64 connections,
  8-second windows (`wrk -d 8s -c 64 -t 8`), with a 2-second pause
  between endpoints. **No CPU pinning**: the server and wrk share all 8
  cores, and wrk took about 1.4 of them at 410k req/s.
- **Three boots, medians.** A pass boots each server once, runs its
  endpoints in a fixed order (/plaintext, /10k, /cpu, /io, /io_native)
  and stops it; the next server starts only when the port is free.
  Three passes run, with the server order rotated by one place each
  time, and every figure is the median of the three. No run reported
  here had a non-2xx response or a socket error.
- The commands, on the 8-core box:

  ```sh
  # the main table
  bundle exec ruby bench/kino_server.rb ractor 9293 8 1        # Kino :ractor
  bundle exec ruby bench/kino_server.rb ractor-lanes 9293 8 1  # + lanes
  bundle exec ruby bench/kino_server.rb ractor 9293 32 1       # workers 32
  bundle exec ruby bench/kino_server.rb threaded 9293 8 3      # Kino :threaded
  bundle exec puma -q -w 8 -t 3:3 -p 9293 bench/config.ru      # Puma cluster
  wrk -d 8s -c 64 -t 8 http://127.0.0.1:9293/plaintext         # and the other endpoints

  # the ractor-wrapper table
  bundle exec ruby bench/kino_server.rb ractor 9293 8 3
  RACTOR_POOL=8 bundle exec puma -q -t 10:10 -p 9293 bench/wrapped.ru
  RACTOR_POOL=1 bundle exec falcon serve --bind http://127.0.0.1:9293 --count 8 --config bench/wrapped.ru

  # the memory table's threaded row
  MALLOC_ARENA_MAX=2 bundle exec ruby bench/kino_server.rb threaded 9293 8 3

  # HTTP/2: 5 s per lane, 8 connections x 8 streams
  bench/h2.sh 5 8 8
  ```

  `bench/run.sh 8 64` runs the same endpoints and servers in one go,
  but boots every Kino variant at 3 threads; the main table's Kino
  columns are the commands above. The Rails commands are in
  [Rails](#rails).
- **Memory is reported as PSS (proportional set size), not RSS.** A Puma
  cluster forks N workers that share the Ruby VM and gem code
  copy-on-write; summing each worker's RSS counts those shared pages up
  to N times and overstates the cluster's real footprint. PSS divides
  every shared page across the processes mapping it, so it reflects the
  unique physical memory the cluster occupies—the only fair basis for
  comparing one process against a fork-per-core cluster. We read it from
  `/proc/<pid>/smaps_rollup`, summed over the whole process tree, with
  RSS from `ps` beside it. Kino serves from one process, so its
  RSS ≈ PSS; the correction only moves Puma.
- Memory is sampled after the endpoint battery (/plaintext, /10k, /cpu,
  /io, 8 s each), on boots of its own for the memory table. A
  methodology trap worth recording: a single-endpoint warmup understates
  memory badly (ractor mode reads 100 MB after /plaintext alone and
  158 MB after the battery)—memory is only comparable after the full
  battery, and /io numbers are only comparable at equal slot counts.
- The harness waits for the port to be genuinely free between targets.
  This matters: falcon binds with `SO_REUSEPORT`, so a leftover instance
  silently splits traffic with the next server and poisons every number
  after it (we learned this the hard way).
- **Older figures.** The 2026-09 tables (a c7a.2xlarge, Ruby 4.0.6) are
  replaced. Absolute numbers are now about twice those for every server,
  Puma included (291,409 against 142,094 on /plaintext): the instance
  type, the Ruby and Kino all changed, so compare columns within one
  run, never across dates. A few studies were not re-run in 2026-11 and
  keep their earlier figures, each marked where it stands: the
  `tokio_threads` half of the CPU recipe, the per-core execution probe,
  the topology sweep, sharded I/O, logging costs, header interning, the
  allocator shoot-out and the macOS table.
- Secondary data point: macOS (MacBook Pro, M1 10-core), where every
  server converges near the loopback ceiling (~42-49k plaintext) and
  differences compress; its table is at the end of this document.
  Earlier published numbers from Docker-on-Mac are retired—real
  hardware contradicted several of that environment's findings, noted
  inline below where the conclusion changed.

## Reading the headline tables

These tables all run the **tiny synthetic Ractor-shareable app**. The real
Rails app's numbers were measured in threaded mode, before Rails could
run on Ractors—a separate story, in [its own section](#rails).

- **Plaintext/10k**: ractor mode clears the fork cluster by 1.5-1.6×
  (465,055 vs Puma's 291,409 on /plaintext = 1.60×; 375,470 vs 251,357
  on /10k = 1.49×), and lanes by 1.6-1.8× (525,155 = 1.80×; 409,085 =
  1.63×). Threaded mode is ahead by less: 1.35× on /plaintext, 1.13×
  on /10k. Ractor mode leads threaded on these trivial handlers
  (465k vs 394k), and lanes widen it (525k).
- **CPU (recursive fib)**: ractor mode does **5.8× its own GVL-bound
  threaded mode** (118,095 vs 20,187)—that's the entire point of
  ractors—and beats the fork cluster: +14% with stock defaults (+8%
  with lanes, 111,751 vs 103,653). The io-tuned `workers 32` topology
  does not, on Ruby 4.0.9: 83,713, 19% behind the cluster.
- **Memory (PSS)**: after the full endpoint battery, the tiny app costs
  Kino **167 MB** in ractor mode (110 MB threaded, arena-capped)
  against the 8-worker cluster's **1,171 MB**—~7-11× lighter, because
  a trivial app is almost all private per-worker heap that
  copy-on-write can't share. The Rails hello app comes to ~6.7×; both
  are in [Memory under load](#memory-under-load-and-the-glibc-arena-footgun).
- **I/O (5 ms wait)**: the dispatch models tie within ~2-3% at equal slot
  counts (24 slots: threaded 4,738, Puma 4,729, ractor 8×3 4,629), so
  the default columns show the ractor modes behind on /io (8 slots vs
  the cluster's 24), and the `workers 32` column shows the same engine
  winning (+21%, +32% via `Kino.sleep`) once it has more slots than the
  cluster. The lever is slot count, and Kino slots are cheap: see
  [below](#why-io-lags-in-ractor-mode-on-linux).
- **Spread between boots**: most cells' three boots sit within 4% of
  each other. The exceptions on Ruby 4.0.9 are `workers 32` on the
  light rows (/plaintext 238,040-290,583, /10k 202,415-241,150, one
  boot of three well above the other two) and /cpu in the ractor
  columns (117,391-125,831 at the default, 105,994-113,008 with
  lanes).

## CPU-bound tuning

On real hardware, Kino's stock defaults lead the cluster on pure
CPU—and the old tuning recipe is obsolete:

| config | /cpu req/s |
|---|---:|
| Puma cluster (reference) | 103,653 |
| Kino `workers 8, threads 3` (the default before 0.1.1) | 108,063 |
| Kino `workers 8, threads 1`, tokio auto (**the default**) | **118,095** |

(The 8×3 row is from the ractor-wrapper run, the other two from the
main table's: the same box and build, separate runs.)

The `threads 1` half of the old recipe became the default. Its
`tokio_threads 1` half was last measured in 2026-09 (c7a.2xlarge, Ruby
4.0.6) and not re-run since: there it *cost* −12% on /cpu (68,600 vs
77,999 at the default) and more on plaintext (108,523 vs 230k). Don't
pin tokio threads. **The recipe's history is an environment story**: in
the earlier Docker-on-Mac runs it was worth +12%, because tokio threads
and wake churn competed for oversubscribed virtualized cores; on
dedicated cores the same pin starves the I/O front-end instead. If you
deploy into a constrained/virtualized environment, measure there.

Two findings that survived the environment change:

**Ruby executes at identical per-core speed in ractors and forks.** We
briefly believed parallel ractors paid a ~24% execution tax; that probe
compared 8 busy ractors against a *single* busy thread, which also
compares all-core clocks against single-core boost. The controlled probe
(8 forked processes vs 8 ractors, same all-core clock, on the earlier
c7a.2xlarge box): forks 8,973 fib/s/core, ractors 8,918—identical.
`GC.disable` changes nothing (fib barely allocates). The VM is
innocent. Never compare parallel against single-threaded baselines
without controlling for clocks.

**The GVL ceiling is absolute.** Threaded mode posts ~20k /cpu (20,187
at 8×3)—24 threads in one process serialize on one lock.
Parallelism for CPU-bound Ruby comes from ractors or forks, nothing else.

## Why /io lags in ractor mode on Linux

On bare metal the gap is small at equal slot counts: ractor /io 4,629
vs threaded 4,738 (−2%, both at 8×3). In Docker it was −18%, and a
pure-Ruby probe there measured
`sleep(0.005)` waking +2.3-2.8 ms late inside ractors vs +1.8 ms on the
main ractor—non-main-ractor timer wakeups are coarser in Ruby 4.0, but
how much that costs depends heavily on the kernel/virtualization stack.
A follow-up probe showed `IO.select`-style waits are tighter than
`sleep` inside ractors, so real I/O readiness suffers less than timers.

**Mitigation 1—`Kino.sleep`:** releases the GVL and waits on the OS
clock directly (chunked, so `Thread#kill`/shutdown stay responsive). The
`/io_native` endpoint is the same 5 ms wait via `Kino.sleep` when
available. On this box it is worth +2% at the 8×1 default (1,578 vs
1,553 with plain sleep) and +9% at `workers 32` (6,264 vs 5,721).

**Mitigation 2—add workers; they're nearly free.** The headline tables
show default ractor-mode /io at 1,553: that's 8 slots (the 1-thread
default) against the cluster's 24, because wait-bound throughput is
simply `slots ÷ effective wait`. Kino's slots cost ~a thread each, not
a forked process: the `workers 32, threads 1` column measured **5,721
/io (+21% over the 24-thread cluster's 4,729) and 6,264 /io_native
(+32%)**, still one small process (79 MB PSS after the battery). On
Ruby 4.0.9 it pays on the other rows: 245,612 plaintext against 465,055
at 8×1, and 83,713 /cpu against 118,095, which puts it 16-19% behind
the cluster on /plaintext, /10k and /cpu. On the Ruby 4.1 development
build most of that cost is gone (463,223 plaintext against 512,945 at
8×1, /cpu level with 8×1, and ahead of the cluster on every row); see
[Ruby 4.1](#ruby-41-development-build). A fork cluster buying the same
32 slots pays for them in full copies of the app.

## The ractor-pool-wrapper comparison

A reasonable first experiment for anyone curious about ractors is a Rack
wrapper that ships each request to a ractor pool on whatever server they
already run. `bench/ractor_wrapper.rb` is that experiment, benchmarked on
Puma and Falcon—not as a comparison of those servers, but to measure
what the Rack-level hop itself costs (same box, Ruby 4.0.9):

| endpoint   | Kino :ractor (8×3) | Puma + wrapper | Falcon + wrapper |
|------------|-------------------:|---------------:|-----------------:|
| /plaintext |            412,687 |         40,675 |          241,390 |
| /cpu (fib) |            108,063 |         33,322 |           94,417 |
| /io (5 ms) |              4,629 |          1,509 |            1,564 |

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

These are `:threaded` measurements from before Rails could run on
Ractors, so this whole section is one GVL-bound Kino process; ractor-mode
Rails is not benchmarked yet. The app is the controller-only Rails hello
app, kept as the bench subject in `bench/rails_hello.ru` (edge Rails,
production mode, 8 workers × 5 threads), on the same box, Ruby 4.0.9:

| | req/s | RSS | PSS |
|---|---:|---:|---:|
| Kino `:threaded` (one process) |  7,660 | 102 MB | **96 MB** |
| Puma cluster (8 workers, preload) | 34,688 | 971 MB | **640 MB** |
| Puma cluster (8 workers, no preload) | 35,048 | 963 MB | **784 MB** |

This is the honest version of the Rails story. In threaded mode Kino is
one GVL-bound process, so the fork cluster outruns it ~4.5× by using all
8 cores—at ~6.7× the memory by PSS. The metric matters here: Puma's RSS
(971 MB) counts the shared Rails framework once per worker; PSS (640 MB)
counts it once, and that is the fair figure. Preloading saves 144 MB
(640 MB with `preload_app!`, 784 MB without) at the same throughput
(the boots' ranges overlap); the rest stays private, as the
[memory section](#rails-threaded-fallback) shows. Rails-on-Ractors is
interesting precisely because it could close the throughput gap at the
one-process memory cost; what runs today is documented in
[rails-on-ractors.md](rails-on-ractors.md).

How it was run, from `examples/rails-hello` (its bundle), with
`RAILS_ENV=production RACK_ENV=production WORKERS=8`: Kino through
`Kino::Server.run` on `bench/rails_hello.ru` with `workers: 8, threads:
5, mode: :threaded`; Puma as `puma -q -w 8 -t 5:5 --preload
bench/rails_hello.ru`, and with `-C bench/puma_no_preload.rb` in place
of `--preload`. wrk as above on `/` for 8 seconds, then RSS and PSS;
three rotated boots, medians. `bench/rails_mem.sh`, which loads with
`ab` for 5 seconds instead, reads the same memory: 99 MB for Kino and
646 MB for the preloaded cluster.

Two things to know before comparing or reproducing:

- **The bundle moved since 2026-09**, so these figures are not
  like-for-like with the earlier ones (2,731 and 15,422 req/s, 95 MB
  and 405 MB PSS). The hello app now bundles edge Rails at ff00282
  (8.2.0.alpha), rack from its main branch, and an i18n fork with
  Ractor support.
- **Ruby 4.1 needs a re-resolved lockfile.** The lockfile in the repo
  does not install on the Ruby 4.1 development build as it is; a
  re-resolved one adds source gems for sqlite3, nokogiri and herb (and
  mini_portile2 to build them), with no version changes. The Ruby 4.1
  Rails figures [below](#ruby-41-development-build) were measured with
  that lockfile, the 4.0.9 ones with the repo's.

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

All figures are **PSS** in MB (see [Methodology](#methodology)) after
the full endpoint battery (8 s each of /plaintext, /10k, /cpu, /io—a
"warmed production process", not a fresh boot). RSS is shown alongside
so the copy-on-write correction is visible. Ruby 4.0.9; the Ruby 4.1
figures are [further down](#ruby-41-development-build).

### The tiny synthetic app

| config | RSS | PSS |
|---|---:|---:|
| Kino :ractor 8×1 (default) | 170 | **167** |
| Kino lanes 8×1² | 171 | **169** |
| Kino :ractor 32×1² | 81 | **79** |
| Kino :threaded 8×3 (`MALLOC_ARENA_MAX=2`) | 113 | **110** |
| Kino :threaded 8×3 (no arena cap) | 147 | **144**¹ |
| Puma cluster 8×3 | 1,324 | **1,171** |

The tiny app is ~7× lighter than the cluster in ractor mode, ~11× in
arena-capped threaded mode. RSS ≈ PSS for every Kino row (one process,
nothing to share) and within ~13% for Puma here: a trivial app has
almost no shared state. This is the case where copy-on-write does
**not** rescue the cluster—there is little to share—so the RSS and PSS
numbers nearly agree.

¹ **The arena balloon is gone.** Through 2026-09 this row was the
textbook glibc arena-fragmentation case (the reason Rails ops set
`MALLOC_ARENA_MAX=2`; Heroku ships that default): one 8-second /10k
round took uncapped threaded mode from ~70 MB to ~670 MB and it never
returned. A 2026-11 A/B on this box, three boots per arm, shows where
it went:

| :threaded 8×3, no arena cap, PSS after | /plaintext | /10k | /cpu | /io | of which glibc arenas |
|---|---:|---:|---:|---:|---:|
| Kino's main branch before these changes | 86 | 737 | 724 | **724** | 645 |
| Kino now | 86 | 171 | 147 | **144** | 75 |

The older tree still balloons on /10k (566-738 MB across its boots);
the current one does not (142-158 MB). What changed between the two is
that a threaded worker now lets the threads waiting for the GVL run
between two requests once it has held it for a millisecond (see the
changelog). A second A/B isolates it: the same current build with only
that pass switched off balloons again (761 MB [732-763] after /io,
673 MB of it in glibc arenas), and with the pass on it ends at 155 MB
[148-165]. The likely reading, not tested further: without the pass a
worker holds the GVL for Ruby's whole 100 ms slices and grows its own
malloc arena by about one GC cycle's garbage, which glibc keeps; with
it, the same garbage spreads thinly over every thread. The cap still helps: with `MALLOC_ARENA_MAX=2` the same
run ends at 112 MB, about 30 MB less, and the older tree at 134 MB.
Throughput with the cap moves a few percent either way (/10k 285,724
vs 270,283, /cpu 18,402 vs 19,842). Ractor mode reads the same on both
trees (158 and 156 MB).

² Sampled in the main table's passes rather than the memory table's:
the same battery and sampling point, other boots. Those passes read 164
MB for the default ractor row and 1,137 MB for Puma.

### Rails (threaded fallback)

Here copy-on-write **does** matter, which is exactly why PSS is mandatory:

| config | RSS | PSS |
|---|---:|---:|
| Kino :threaded (one process) | 102 |  **96** |
| Puma cluster 8×5 (preload) | 971 | **640** |
| Puma cluster 8×5 (no preload) | 963 | **784** |

Puma serves the same Rails framework from 8 forks that share it
copy-on-write; RSS counts that shared framework once per worker (971 MB),
PSS counts it once (640 MB). The fair ratio is **~6.7×**, not the ~9.5×
a naive RSS sum reports. Preload saves 144 MB of PSS (640 vs 784
MB)—worth turning on—but it shares the code, not the live object heap:
of the preloaded cluster's pages about 370 MB are shared and about
600 MB are private to one worker or another (it boots at about 124 MB
PSS and grows under load). That is why "CoW should make a fork cluster
nearly free" is only half true.

## Run-to-run variance (a.k.a. "is this a regression?")

The 2026-11 tables answer this with three rotated boots per server and
medians (see [Methodology](#methodology)); what follows is the
experience behind that, from the 2026-06 and 2026-09 c7a.2xlarge runs.

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

Measured in 2026-09 on c7a.2xlarge (Ruby 4.0.6; the sweep was not
re-run in 2026-11), plaintext, ractor mode, same session (three
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
cheap. The 2026-11 figures for the two ends, from separate runs on the
c8a.2xlarge (Ruby 4.0.9), keep the shape: plaintext 412,687 at 8×3 and
465,055 at 8×1 (+13%), /cpu 108,063 and 118,095 (+9%), /io 4,629 and
1,553. (16×1 being worse than 8×1 on plaintext also says the shared
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
a hot worker keeps taking without ever being woken (the rules are in
[architecture](architecture.md#lane-dispatch)).

The main table's two ractor columns are the A/B: ractor mode at the
default topology (8×1), the same passes on the c8a.2xlarge, Ruby 4.0.9:

| endpoint | shared queue | lanes | delta |
|----------|-------------:|------:|------:|
| /plaintext | 465,055 | **525,155** | **+13%** |
| /10k | 375,470 | **409,085** | +9% |
| /cpu | **118,095** | 111,751 | −5% |
| /io | 1,553 | 1,566 | flat |

On the Ruby 4.1 development build the same A/B reads +19% on
/plaintext (608,283 vs 512,945), +15% on /10k (515,785 vs 447,144) and
−12% on /cpu (112,276 vs 128,097).

At the old 8×3 topology the margin was wider (+21% plaintext, 240,193
vs 199,032, in an earlier same-session A/B on a c7a.2xlarge)—much of
the futex pain lanes were built to avoid came from thread handoffs
inside each ractor, and the 1-thread default removes those for everyone. At
the default, lanes post the fastest plaintext/10k of any Kino
configuration, but plain shared-queue takes /cpu. It stays opt-in because overload semantics differ from the
shared queue (`queue_depth` doesn't apply; capacity is lanes × 4 with
brief dispatcher retries up to `queue_timeout` before the 503), and
crash semantics, stealing fairness, and drain behavior have spec
coverage but not production mileage. (On loopback-bound macOS, lanes
lose a few percent instead; see the secondary table below.)

## Sharded I/O (`io_shards true`)

`bench/studies.sh 8 64 shards`, ractor 8×3, on the 2026-09 c7a.2xlarge
box (Ruby 4.0.6; not re-measured in 2026-11):

| case (/plaintext) | req/s |
|---|---:|
| shared tokio runtime (baseline) | 193,461 |
| io_shards, default shard count | 195,436 |
| io_shards, `io_threads 2` | 170,509 |
| io_shards, `io_threads 4` | 196,298 |
| io_shards, `io_threads 8` | 199,657 |

On 8 cores the shards buy +1-3%, best at `io_threads 8` (the
half-the-cores default is close behind; 2 shards choke on accept
handoff). The design removes work-stealing and cross-thread wakeups,
so the win scales with scheduler contention—expect more on boxes with
more cores and connections, and measure on your own core count.

## Logging costs

Measured at full plaintext saturation (one log line per request—rates
that no real deployment logs at; treat these as worst-case ceilings, not
typical costs). These are the 2026-09 c7a.2xlarge figures (Ruby 4.0.6),
not re-measured in 2026-11:

| case (8×3, same session, 2026-09) | req/s |
|---|---:|
| threaded, no logging | 213,166 |
| threaded, `log_requests true` (native access log) | 173,501 (−19%) |
| ractor, access log off / on | 191,152 / 163,777 (−14%) |
| app logs 1 line/req via shared `::Logger` (file) | **90,175** |
| app logs 1 line/req via `Kino::Logger` (file) | **154,653 (1.7×)** |

The shared-`::Logger` cost is the mutex: 24 worker threads serialize
through one lock plus a write syscall per line. `Kino::Logger` hands the
formatted line to a lock-free channel and returns—the remaining cost vs
not logging at all is Ruby-side formatting, which no device can remove.
(The multiple moves with the environment: 2.4× on the 2026-06 box,
1.7× on the 2026-09 one, 8.5× under Docker, where overlay-fs write
latency punished the synchronous logger hardest. The ranking is
environment-independent; the multiple is not.)

One trade-off worth knowing: the sink **never blocks** request threads,
so at absurd rates against a slow disk it drops lines once its 8192-line
buffer is full, while `::Logger` writes every line by strangling
throughput instead. Pick your failure mode; at sane logging rates
neither happens.

Puma comparison note: request logging is opt-in there too (`--quiet` is
the default, `-v/--log-requests` enables it)—Kino's default-off
`log_requests` matches the ecosystem's standard behavior.

## HTTP/2

`bench/h2.sh`, Linux only (on macOS run it under Docker; the numbers
below are from the c8a.2xlarge box, Ruby 4.0.9). Every lane is
measured with h2load so the generator is identical everywhere: h2
lanes run 8 connections × 8 concurrent streams, h1 lanes 64
connections—the same total in-flight. Servers without native h2 get
the standard pattern instead: nginx terminating h2 and proxying
HTTP/1.1 upstream over keep-alive. Kino ractor 8×3, falcon `--count 8`,
puma `-w 8 -t 3:3`, 5 s per lane; the script ran three times
(`bench/h2.sh 5 8 8`) and each cell is the median.

| target (h2 unless noted) | /plaintext | /10k | /big-cookie | /upload (64 KB) |
|---|---:|---:|---:|---:|
| kino h2c | 424,717 | 288,231 | 378,434 | 71,106 |
| kino h1 cleartext (same boot) | 300,667 | 252,243 | 249,953 | 72,902 |
| kino h2 TLS | 355,281 | 217,269 | 324,092 | 52,635 |
| kino h1 TLS (same boot) | 225,264 | 164,538 | 196,416 | 47,940 |
| falcon TLS (native h2) | 100,399 | 123,305 | 112,884 | 49,162 |
| nginx h2 → puma h1 | 185,917 | 148,375 | 150,235 | 1,199 |
| nginx h2 → kino h1 | 245,741 | 162,915 | 121,263 | 1,224 |

What the numbers say:

- **Native h2 beats h1 on the same server by +41% cleartext and +58%
  over TLS** on /plaintext: the same 64 in-flight requests ride 8
  connections instead of 64, so frames batch into fewer, larger
  syscalls. The `/big-cookie` lane (a ~2 KB cookie per request) shows
  HPACK on top: the cookie crosses the wire once per connection, not
  once per request, and h2 holds 89-91% of its bare-plaintext
  throughput where h1 holds 83-87%.
- **Native h2 beats proxied h2 by +45%** with the *same backend*: the
  nginx→kino-h1 lane is the proxy-cost control, and the extra hop,
  re-parse, and re-serialize cost ~110k req/s on /plaintext.
- **Uploads run ahead of h1 over TLS (+10%) and 2% behind it in
  cleartext**—after a fix this lane caught: h2 delivers bodies as
  16 KB DATA frames, and `read_body` originally crossed the GVL once
  per chunk, halving upload throughput. It now drains every queued
  chunk per crossing (doc/architecture.md), and a knob sweep over
  hyper's h2 codec (frame size, adaptive/bigger windows) moved nothing
  afterwards.
- **nginx's upload collapse (~1.2k) is not HTTP/2 flow control.** It
  is nginx spooling the request body to a temporary file: the 64 KB
  upload is larger than the default `client_body_buffer_size`, so
  every request logs "a client request body is buffered to a temporary
  file". A diagnostic on the nginx→kino-h1 lane, with `bench/h2.sh`'s
  nginx config plus one change per row:

  | nginx config | /upload req/s | bodies spooled |
  |---|---:|---|
  | as in `bench/h2.sh` | 1,219 | all |
  | the same, HTTP/1.1 clients (64 connections) | 1,215 | all |
  | `http2_body_preread_size 1m` | 1,224 | all |
  | `client_body_buffer_size 1m` | 52,131 | none |
  | `proxy_request_buffering off` | 50,828 | none |

  An HTTP/1.1 client collapses the same way, and the h2 preread knob
  changes nothing, so h2 is not the cause. **The fix** is either to
  hold the body in memory (`client_body_buffer_size` at least as large
  as your uploads) or not to buffer it at all (`proxy_request_buffering
  off`); both bring the lane to 51-52k, level with Kino's own h2 TLS
  uploads. Moving the temp path to tmpfs did not help (1,226), and
  `sendfile on` did while still spooling every body (45,269).
- falcon lands at 28-57% of kino-h2-TLS on the fast handlers (28% on
  /plaintext) and 7% behind on uploads. Its lanes are the noisiest
  here: its three /10k runs spread from 86,705 to 125,634.
- **Header-value interning** (user-agent, accept-*, sec-ch-*: one
  frozen string instead of a fresh allocation per request) was
  measured in 2026-09, when it landed, with a realistic 11-header
  browser set on /plaintext,
  using the within-boot header cost (bare vs with-headers) as the
  drift-resistant metric: over h2 that cost fell from ~16% to ~12-13%
  (~+3-5% throughput on the headers lane); h1's smaller header cost
  stayed within noise. The effect the tiny app understates: 6-8 fewer
  string allocations per request is GC pressure a real app feels more
  than this one does.

## Ruby 4.1 (development build)

Ruby 4.0.9 is the released Ruby and the primary number in every
section above. This section is the same measurement on a **development
build** of Ruby 4.1 (`4.1.0dev`, master f2d62c677e of 2026-11-21, YJIT
on): the same box, harness, passes and medians, with Kino built against
that Ruby. Ruby 4.1 is not released; treat these as a preview that can
move.

### What changes

- **Every ractor column rises.** Ruby 4.1 collects garbage per Ractor.
  On the light rows (/plaintext, /10k) the default ractor column gains
  +10% and +19%, lanes +16% and +26%. On /cpu the default column's
  median is +8% and lanes' is flat, and both overlap the range of the
  4.0.9 boots.
- **`workers 32` gains about 90% on the light rows** (+89% on
  /plaintext, +91% on /10k) and +53% on /cpu, where it now equals the
  8×1 default. On 4.0.9 the io-tuned topology cost 16-19% against the
  cluster everywhere but /io; on 4.1 it is ahead of the cluster on
  every row.
- **`:ractor` PSS falls to 52 MB** (from 167 MB), **14× lighter than
  the cluster**, whose own PSS falls from 1,171 to 746 MB. Threaded
  mode goes the other way: 110 to 150 MB arena-capped, 155 to 227 MB
  uncapped.
- **`:threaded` and Puma lose 2-13%** on /plaintext, /10k and /cpu
  (threaded −4%, −3%, −2%; Puma −7%, −13%, −4%). The wait-bound rows
  do not move.

These are measurements, not attributions: nothing here isolates why
threaded mode and Puma slow down, or why their memory moves.

### Main table on Ruby 4.1

| endpoint    | Kino :ractor | + lanes | :ractor, `workers 32` | Kino :threaded | Puma (cluster) |
|-------------|-------------:|--------:|----------------------:|---------------:|---------------:|
| /plaintext  |      512,945 | **608,283** |        463,223 |        378,380 |        269,662 |
| /10k        |      447,144 | **515,785** |        388,306 |        275,927 |        218,884 |
| /cpu (fib)  |      128,097 | 112,276 |    **128,109** |         19,739 |         99,379 |
| /io (5 ms)  |        1,558 |   1,542 |      **6,102** |          4,702 |          4,728 |
| /io_native  |        1,576 |   1,581 |      **6,314** |          4,738 |          4,730 |

Against the cluster on Ruby 4.1: ractor mode is 1.9-2.0× ahead on
/plaintext and /10k (lanes 2.3-2.4×, threaded 1.3-1.4×) and +29% on
/cpu (+13% with lanes), 6.5× its own threaded mode. `workers 32` is
+29% on /io and +33% on /io_native.

### Memory on Ruby 4.1 (PSS, MB, tiny app)

| config | RSS | PSS | 4.0.9 PSS |
|---|---:|---:|---:|
| Kino :ractor 8×1 (default) | 54 | **52** | 167 |
| Kino lanes 8×1² | 54 | **51** | 169 |
| Kino :ractor 32×1² | 110 | **108** | 79 |
| Kino :threaded 8×3 (`MALLOC_ARENA_MAX=2`) | 152 | **150** | 110 |
| Kino :threaded 8×3 (no arena cap)² | 229 | **227** | 155 |
| Puma cluster 8×3 | 879 | **746** | 1,171 |

² From the main table's passes, on both Rubies (so the 4.0.9 uncapped
threaded figure here is that run's 155 MB, not the 144 MB of the arena
A/B above).

The ratio to the cluster is ~14× in ractor mode and ~5× in arena-capped
threaded mode.

### Ractor-pool wrapper on Ruby 4.1

| endpoint   | Kino :ractor (8×3) | Puma + wrapper | Falcon + wrapper |
|------------|-------------------:|---------------:|-----------------:|
| /plaintext |            445,252 |         38,198 |          201,168 |
| /cpu (fib) |            118,739 |         36,701 |           88,088 |
| /io (5 ms) |              4,656 |          1,526 |            1,563 |

Kino at 8×3 gains +8% and +10% on /plaintext and /cpu. The Puma
wrapper moves −6% and +10%, the Falcon wrapper −17% and −7%.

### HTTP/2 on Ruby 4.1

| target (h2 unless noted) | /plaintext | /10k | /big-cookie | /upload (64 KB) |
|---|---:|---:|---:|---:|
| kino h2c | 485,189 | 376,586 | 467,071 | 81,048 |
| kino h1 cleartext (same boot) | 333,545 | 287,251 | 308,467 | 77,958 |
| kino h2 TLS | 368,885 | 275,102 | 355,013 | 54,234 |
| kino h1 TLS (same boot) | 231,119 | 191,489 | 214,085 | 47,107 |
| falcon TLS (native h2) | 116,003 | 118,664 | 120,601 | 50,876 |
| nginx h2 → puma h1 | 175,436 | 130,576 | 137,531 | 1,259 |
| nginx h2 → kino h1 | 249,922 | 181,448 | 185,498 | 1,195 |

Native h2 over h1 on /plaintext is +45% cleartext and +60% over TLS,
and +48% over the nginx hop. Uploads are +15% over h1 with TLS and +4%
in cleartext. The nginx upload diagnostic reproduces: 1,232 req/s as
configured, 53,665 with `client_body_buffer_size 1m`, 54,259 with
`proxy_request_buffering off`.

### Rails on Ruby 4.1

| | req/s | RSS | PSS |
|---|---:|---:|---:|
| Kino `:threaded` (one process) |  7,903 | 109 MB | **104 MB** |
| Puma cluster (8 workers, preload) | 35,060 | 1,113 MB | **835 MB** |
| Puma cluster (8 workers, no preload) | 34,288 | 1,112 MB | **958 MB** |

The cluster serves ~4.4× the throughput at ~8.0× the memory by PSS.
Measured with the re-resolved lockfile described under
[Rails](#rails).

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