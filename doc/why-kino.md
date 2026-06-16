# Why Kino is a Ractor server

What makes this server ractor-specific, why do Ractors work fast here
when they are slow everywhere else, and which Rust parts deserve the
credit. Companion to [architecture.md](architecture.md), which describes
the same machinery piece by piece.

## The problem Kino exists to solve

Ractors forbid sharing mutable Ruby objects. That single rule breaks
every conventional server design. Puma hands env Hashes, socket objects,
and request state between threads freely; with Ractors, a Ruby object
created on the accepting side cannot be given to a worker—`Ractor#send`
deep-copies it, and sockets cannot cross at all.

We measured what the "obvious" workaround costs. The ractor-pool wrapper
experiment (reduce the env to a shareable subset, copy it to a worker
over a `Ractor::Port`, copy the response back) runs at **19.5k req/s
where Kino does 194k** on the same hardware—see the
[wrapper comparison](benchmarks.md#the-ractor-pool-wrapper-comparison).
Copying at the Rack layer eats the entire ractor dividend. Dispatch has
to live below the Rack contract.

## The trick: shared state lives below the FFI line

Kino's answer is that **the request never exists as a Ruby object until
it is already inside the worker ractor**. The whole request—method, URI,
headers, body stream, response channel—lives in native memory as a Rust
`RequestCtx`. The only things that cross ractor boundaries in Ruby are
what Ractor law allows for free: integers (server id, worker ids) and
the frozen, shareable app. When a worker calls `take_one`, the env Hash
and the `Kino::Native::Request` handle are *constructed inside that
worker's ractor*—ownership is correct by construction, nothing is
copied, nothing is shared.

Put differently: Ractors cannot have shared mutable memory in Ruby, so
Kino moves all shared mutable state into Rust, where `Send`/`Sync` rules
apply instead of ractor isolation. Ruby sees only ids and frozen
objects; Rust sees one queue and one registry.

## The Rust parts to thank, in order of credit

1. **`rb_ext_ractor_safe(true)`** (lib.rs)—one line, and the
   precondition for everything: without it, *any* native call from a
   non-main ractor raises `Ractor::UnsafeError`. The spec suite keeps a
   canary test on this.
2. **The registry** (registry.rs)—global Rust-side state keyed by
   `u64`. This is the "shared memory" Ractors legally cannot have:
   workers address everything by integer, and no TypedData object ever
   crosses a boundary.
3. **The flume MPMC queue**—the cross-ractor work distributor. One
   queue, async send from tokio, blocking receive from any ractor's
   thread. Ruby has no ractor-safe equivalent that does not copy
   (Ports copy every message); a Rust channel moves a
   `Box<RequestCtx>` pointer.
4. **`gvl.rs`**—`rb_thread_call_without_gvl` plus the atomic-flag UBF
   idiom. A worker blocking on the queue releases its per-ractor lock,
   stays interruptible (Thread#kill, shutdown) via bounded
   `recv_timeout` ticks, and costs nothing when the queue has work: the
   `try_recv` fast path skips the lock release entirely.
5. **The frozen env-string cache** (env_strings.rs)—exploits the one
   sharing channel Ractors *do* allow: frozen objects. Env keys, 44
   common header names, methods, LRU'd host and peer-address values,
   built once on the main ractor and read by every worker forever.
   Without this, each ractor would allocate every key on every request.
6. **The Responder** (response.rs)—responses travel back as Rust types
   through a oneshot/frame channel into hyper, never as shared Ruby
   objects. Its first-claimant atomic is also what makes crash recovery
   work: the supervisor (on the main ractor) can 500 a dead ractor's
   in-flight requests through `Weak` references into native memory.
   That cross-ractor cleanup would be impossible if request state were
   Ruby-side.
7. **tokio + hyper owning all I/O**—sockets are exactly the kind of
   unshareable object that poisons ractor designs; here Ruby never
   touches one.

## Why it is fast, by the numbers

With the dispatch cost eliminated, Ractors deliver the thing they were
built for—a lock per ractor instead of one GVL—and each layer is
visible in the [benchmarks](benchmarks.md): `/cpu` at 78.0k req/s in
ractor mode vs **13.4k threaded (5.8×, the GVL ceiling)**, beating the
fork cluster's CPU parallelism by +34% while holding **~148 MB against
the cluster's ~1,068 MB** (by PSS, on the bench app), because eight
ractors share one VM, one Rust front-end, one queue, and one JIT, where
eight forks each pay full price.

The cleanest proof of the design is the threaded fallback itself: it
reuses ~95% of the same machinery, because the Rust core is
dispatch-agnostic. Ractors are not what makes Kino's engine fast—they
are what the engine finally makes *usable*.
