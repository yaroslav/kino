# Rails on Kino, and the state of Rails-on-Ractors

Rails main partially runs on Ractors: `Rails.application.ractorize!`
makes a booted production app shareable, and Kino serves it from worker
Ractors. `examples/rails-hello` is a single-file Rails app that does
exactly that. Its Ractor setup follows Shopify's
[ractor-safe-rails](https://github.com/Shopify/ractor-safe-rails)
template. The blockers listed here in June 2026 (unshareable boot-time
procs, class-level state read from worker Ractors) were resolved
upstream by `ractorize!`.

## How to run it

```sh
cd examples/rails-hello
bundle exec kino                   # mode :ractor, from kino.rb
bundle exec kino --mode threaded   # the same app on plain threads
bundle exec kino --check           # reports the app Ractor-shareable
```

Keep `bundle exec`: the example needs the Gemfile's Rails main, not an
installed Rails release. `config.ru` calls `HelloApp.ractorize!` and
runs `HelloApp.instance` (running the class works too, but `--check`
flags its class-level ivars). Puma in single mode serves it
(`bundle exec puma config.ru`); in cluster mode it hangs, because the
Ractor-safe logger's writer thread does not survive fork.

## What works

Verified on the example in `:ractor` mode:

- Controllers and routes, including the root route and a posts
  resource.
- Active Record on SQLite: reads, inserts, and a table that persists
  across restarts.
- Rendering plain text and JSON.
- Validations: an over-long post body returns 422 with the validation
  message.
- Error pages: unknown paths return 404, a request missing a required
  parameter returns 400, and the server keeps serving.
- Concurrent reads from several worker Ractors, with no Ractor errors.

## What the example sets, and why

Most of these fixed a concrete failure. The first block is gems, the
rest is app code.

**Gems** (`Gemfile`)

- **Rails main.** `ractorize!` exists only there.
- **rack main.** Boot fails in `ractorize!` with `rack/ractorize not
  available` because no released rack ships that file.
- **Shopify's i18n `ractor_support` branch.** Released i18n keeps its
  config in class variables, which worker Ractors cannot read: every
  request answers 500 with `can not access class variables from
  non-main Ractors`.

**Settings** (`app.rb` and `config.ru`)

- **`ENV["RAILS_ENV"] ||= "production"`.** The ractorized app only boots
  in production.
- **`ActiveSupport::Ractors.unshareable_proc_action = :raise`.** Without
  it, boot fails making a boot-time proc shareable (`Proc's self is not
  shareable`).
- **`ActiveSupport::TaggedLogging.ractor_logger($stdout)`.** A plain
  `Logger` holds a `Monitor`, which `ractorize!` cannot make shareable.
- **`config.i18n.fallbacks = true`.** Without it, every render reads
  `I18n.fallbacks` from a class variable and answers 500.
- **`require "i18n/ractorize"` after `ractorize!`.** It makes I18n's
  backend shareable. Without it, any `I18n.t` from a worker, such as a
  validation message, answers 500 with an isolation error on
  `I18n::Config`.
- **`config.cache_store = :null_store`.** The default cache store is
  not Ractor-safe, so this protects anyone who adds `Rails.cache`.
- **`config.public_file_server.enabled = false`.** Carried over from
  the template; not needed by the example.
- **Routes through `routes.append` inside the app class, and controllers
  defined before `initialize!`.** Boot freezes the route set, so a
  later `routes.draw` fails, and the eager-loading boot looks up each
  route's controller.
- **`render body: payload.to_json, content_type: "application/json"`
  instead of `render json:`.** The JSON renderer is defined with an
  unshareable block, so worker Ractors cannot call it.
- **`Post.define_attribute_methods` in the main Ractor.** Without it the
  first boot passes, but after a restart over existing rows every
  `/posts` request fails with an isolation error on a generated
  attribute-methods constant.
- **`max_connections=64` in the database URL.** Worker Ractors send
  every database call through one main-Ractor thread; a checkout that
  waits on the pool blocks the check-ins that would free a connection,
  and requests time out. Keep it at least workers × threads.
- **No SQLite busy timeout.** See the limitations below.
- **`Rails.backtrace_cleaner.remove_filters!`.** Ruby 4.0.9 crashes when
  a Ractor-shareable proc calls `String#sub` with a regex from many
  Ractors at once. Rails' backtrace cleaner does exactly that on
  concurrent errors, so the example clears its filters. Logged
  backtraces keep full paths.

## What does not work yet

As of Rails main and Ruby 4.0.9:

- **Database work does not scale with workers.** Rails runs every worker
  Ractor's database call on one main-Ractor thread, through
  ractor-dispatch's single executor thread. Concurrent SQLite write
  transactions from several Ractors also contend. The example sets no
  busy timeout, so the loser fails fast with a 500 (`database is
  locked`) instead of stalling every worker while it waits.
- **Action Cable, the default cache store, the Solid gems and the async
  Active Job adapter are not Ractor-safe yet.** Apps that need them stay
  on `:threaded`.
- **Built-in renderers such as `render json:` fail from worker
  Ractors.** `Renderers.add` defines them with a plain block. The
  example renders JSON with `render body:`.
- **Attribute methods must be defined in the main Ractor at boot.** Call
  `define_attribute_methods` on each model; a worker cannot define them.
- **Puma cluster mode hangs.** The Ractor-safe logger starts a writer
  thread at boot, and it does not survive fork. Puma single mode works.
- **Production only.** The ractorized app only boots in production (the
  example defaults `RAILS_ENV` to production).
- **The regex crash above** is a Ruby VM bug; the backtrace filters stay
  off until it is fixed.
- **Branch gems.** The example needs rack main and the i18n branch until
  their Ractor support is released.

## Threaded mode

`:threaded` runs any Rails app, including the parts above. Pair it with
production-style settings: eager load, no code reloading, a database
pool of at least workers × threads, and a logger on stdout or another
thread-safe device. The Rails numbers in the
[README](../README.md#rails) and [benchmarks](benchmarks.md#rails) were
measured in `:threaded` mode on the controller-only hello app
(`bench/rails_hello.ru`), before Rails could run on Ractors; ractor-mode
Rails is not benchmarked yet.
