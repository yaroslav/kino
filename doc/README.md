# Extra documentation

This folder holds the guides the README links to for detail, and the
architectural decisions, discussions, and benchmark results.

Almost all content here is written by agents (Claude Code or Codex).

## Guides

- [configuration.md](configuration.md): every server option, the config
  file and the CLI, the modes, `kino --check`, timeouts and intake
  limits, `on_error`, the lifecycle hooks, sharded I/O and HTTP/2.
- [operations.md](operations.md): signals and shutdown, stats and the
  control plane, logging, the elastic pool and stuck-worker quarantine.
- [streaming.md](streaming.md): Rack 3 compliance and streaming
  responses.
- [websockets.md](websockets.md): native WebSockets, topics,
  `rack.hijack` and Action Cable.
- [worker-loop.md](worker-loop.md): fibers inside Kino workers
  (`worker_loop`) and inline connections.
- [rails-on-ractors.md](rails-on-ractors.md): Rails on Kino, and the
  state of Rails-on-Ractors.

## Design and measurements

- [why-kino.md](why-kino.md): why Kino is a Ractor server, and which
  Rust parts deserve the credit.
- [architecture.md](architecture.md): how the machinery works, piece by
  piece.
- [benchmarks.md](benchmarks.md): the methodology and analysis behind
  the README's tables.
