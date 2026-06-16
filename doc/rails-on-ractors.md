# Rails on Kino, and the state of Rails-on-Ractors

`examples/rails-hello` runs edge Rails on Kino.

## What works as of Kino 0.1.0: `:threaded` mode

Rails 8.2.0.alpha boots and serves with `mode :threaded` (see the
example's `kino.rb`; just `bundle exec kino` in that directory). Measured
on the hello-world (c7a.2xlarge, 8 cores, production mode, 8×5):
~2.6k req/s in 92 MB PSS, single process. The 8-worker Puma cluster
reaches ~12.1k by parallelizing across forks, at 389 MB PSS (794 MB RSS,
but its forks share the framework copy-on-write, so PSS is the fair
figure)—Rails-on-Ractors is interesting precisely because it could offer
that ~4.6× parallelism at ~1/4th of the memory.

Pair it with production-style Rails settings: eager load, no code
reloading, database pool ≥ workers × threads, logger to stdout or another
thread-safe device.

## What doesn't (yet): `:ractor` mode—with the exact blockers

`examples/rails-hello/ractor_probe.rb` re-tests this against whatever
Rails is bundled; when it prints SUCCEEDED *and* a 200 ractor-mode
response (sharing the app is only the first blocker), flip
`mode :ractor`. As of June 2026 (rails main):

1. **Sharing the booted app fails.**
   `Ractor.make_shareable(Rails.application)` raises
   `Ractor::IsolationError: Proc's self is not shareable` at
   `railties/lib/rails/application/finisher.rb:151`—a routes-reloader
   hook lambda Rails registers at boot (in the development path) whose
   `self` is the unshareable application instance.
2. **Calling the app from a worker ractor fails** even where
   `make_shareable` succeeds on a class:
   `Ractor::IsolationError: can not get unshareable values from instance
   variables of classes/modules from non-main Ractors (@instance from
   HelloApp)`—`Rails.application` is a class-level ivar read on the
   dispatch path.
3. **Every workaround is closed by one VM rule** (verified on Ruby 4.0.5):
   class instance variables are main-ractor-only—non-main ractors can
   neither read them (when the value is unshareable) nor set them, **even
   inside a `Ruby::Box`** (`RUBY_BOX=1`). Box isolates constant tables,
   but Ractor isolation still applies to box-local classes:

   ```
   Ractor::IsolationError: can not set instance variables of
   classes/modules by non-main Ractors
   ```

   So booting a private Rails per worker ractor (boxed or not) dies on
   the first class-ivar write, and sharing one boot dies on reads.
