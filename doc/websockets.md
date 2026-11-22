# WebSockets (experimental)

Kino serves WebSockets two ways:

- **Native WebSockets** run the protocol in Rust: the handshake,
  framing, pings, the close handshake and topic fan-out. Your handler's
  callbacks run on the worker that accepted the socket. An idle socket
  costs Ruby nothing: no thread, no fiber. Needs `lanes true`.
- **`rack.hijack`** hands the connection to libraries that bring their
  own WebSocket stack, such as Action Cable. Works in both modes, with
  or without lanes.

This guide covers what an app sees; [architecture.md](architecture.md#websockets)
explains how it works inside.

## Native WebSockets

### Accepting a handshake

Lane dispatch routes each socket's events to its own worker, so turn it
on:

```ruby
# kino.rb
lanes true
```

An app accepts a handshake by setting `env["rack.upgrade"]` to a handler
when `env["rack.upgrade?"]` is `:websocket` (the env keys and callback
names iodine and Agoo use):

```ruby
module Chat
  def self.on_open(ws) = ws.subscribe("chat")
  def self.on_message(ws, data) = Kino::WebSocket.publish("chat", data)
  def self.on_close(ws) = nil
end

app = Ractor.shareable_proc do |env|
  if env["rack.upgrade?"] == :websocket
    env["rack.upgrade"] = Chat
    [101, {}, []]
  else
    [200, {"content-type" => "text/plain"}, ["hello"]]
  end
end
```

Kino answers with a 101 that carries the response's headers (a
subprotocol, cookies); the status is ignored. `after_request_complete`
fires once the 101 is out, with status 101. `rack.upgrade?` is set only
on a valid handshake (HTTP/1.1, version 13) to a server with lanes;
anywhere else, whatever the app returns goes out as is, so answer 426
or 404 there. Compare it to `:websocket` rather than testing it for
truth: the convention has other values (iodine sets `:sse`), and a later
Kino may set them too.

Kino does not check `Origin`. A browser lets any page open a WebSocket
to your server and sends your cookies along, so accept handshakes only
from the origins you serve (Action Cable checks for you):

```ruby
if env["rack.upgrade?"] == :websocket && ALLOWED_ORIGINS.include?(env["HTTP_ORIGIN"])
```

### Callbacks

The handler is any object with `on_open(ws)`, `on_message(ws, data)`,
`on_drained(ws)`, `on_shutdown(ws)` and `on_close(ws)`, each optional:
a module that serves every socket, or an object per socket that keeps
its own state (`env["rack.upgrade"] = ChatSession.new(user)`). The
handler stays on the worker that accepted the socket, so in `:ractor`
mode it need not be shareable, and it may change its state without a
lock.

- Callbacks run on the worker thread that accepted the socket, one
  event at a time and in order, between that worker's requests, never
  alongside them. New sockets go to the worker that owns the fewest, and
  a socket stays on its worker. That affinity is how Kino runs sockets
  today, not a promise for every mode it may add: keep a socket's state
  in its handler, and reach other sockets through their handles rather
  than through one worker's globals.
- Under a `worker_loop` they run alongside the worker's requests and
  other sockets' callbacks instead, each socket's still in order (see
  [worker-loop.md](worker-loop.md#websockets)).
- A busy socket cannot hold up its worker: after eight events in a row,
  a waiting request goes next, then a due wake. Nor can it hold up the
  worker's other sockets: Kino reads at most 16 of a socket's messages
  ahead of its worker, so another socket's message waits behind at most
  16 of the busy one's.
- `data` is a UTF-8 String for a text message and a binary String for
  a binary one.
- `on_open` runs before the client gets the 101, so what it writes or
  subscribes to is in place when the client sees the socket open. It
  counts toward `request_timeout`.
- `on_drained` runs once the socket's outgoing queue has emptied after
  it was too full for something (see [Flow control](#flow-control)).
- `on_shutdown` runs when the server starts to drain, before Kino closes
  the socket with 1001: what it writes reaches the client first. Use it
  to tell a client where to reconnect, or for a protocol's own goodbye.
  The drain waits for it, so keep it short.
- `on_close` runs once for each socket that opened, on its worker,
  whatever ended the socket, unless the worker itself is gone (see
  [Lifecycle](#lifecycle-and-close-codes)). It gets no close code or
  reason.
- A callback that raises is logged and its socket is dropped (its
  `on_close` follows, unless `on_close` itself raised); the worker keeps
  serving. When `on_open` raises, the client gets a 500 instead of the
  101. So does a handshake whose request body another of the app's
  threads is still reading when Kino accepts it or sends its 101. Action
  Cable keeps a connection open when a channel raises; to get that,
  rescue inside the callback. The log line names the socket and the
  callback:

  ```
  kino[4213] worker-2: websocket #42 on_message · RuntimeError: boom (chat.rb:7:in 'Chat.on_message')
  ```

  The `on_error` hook gets the exception too (for `on_open`, once the
  client has its 500), with a frozen Hash in place of the Rack env:
  `{"kino.callback" => "on_message", "kino.websocket" => 42}`. It is not
  a Rack env, so an error tracker should check for `"kino.callback"`
  before building a `Rack::Request` from it.

### The socket

`ws` is a handle to the socket: frozen, shareable across Ractors, and
equal (`==`, `eql?`, `hash`) to every other handle with the same id. Its
methods work from any thread or Ractor, not only inside the socket's
callbacks, and each returns at once. Being frozen, a handle takes no
instance variables; keep per-socket state in the handler, or in a Hash
keyed by `ws`. `Kino::WebSocket[id]` returns the handle for socket `id`,
or nil once the socket is gone (by the time its `on_close` runs, it is).

- `ws.write(text)` and `ws.write_binary(bytes)` queue one frame. They
  return false once the socket is gone, or when its queue is full
  ([Limits](#limits)); the socket stays open, and what to do about a
  slow client is the app's call.
- `ws.write_raw(frames)` queues bytes that already form complete
  frames. Server frames are not masked, so a frame built once can go to
  many sockets.
- `ws.close(code = 1000)` queues a close frame after what is already
  queued; the client has 5 seconds to answer it. The first close
  stands, and closing again returns true. It returns false, and queues
  no close, once the socket is gone or while its queue is full. A code
  no endpoint may send (anything but 1000-1003, 1007-1011 and
  3000-4999) raises `ArgumentError`.
- `ws.abort` drops the connection now, queued frames and all.
- `ws.pending` is how many data frames (writes and publishes) are queued
  and not yet written, a `ws.write_raw` counting once; 0 once the socket
  is gone.
- `ws.id` is an Integer, unique in the process.

### Flow control

A write never waits: when the client reads too slowly and the queue is
full, `ws.write` returns false and the frame is not sent. An app that
streams more than a client can take (a log tail, a file, an LLM's tokens
to a slow phone) pauses there and resumes in `on_drained`:

```ruby
class Download
  def initialize(chunks) = @chunks = chunks # Strings to send, in order

  def on_open(ws) = on_drained(ws)

  # Writes until the queue is full; on_drained comes back once the
  # client has caught up, also to retry a close the full queue refused.
  def on_drained(ws)
    while (chunk = @chunks.first)
      return unless ws.write_binary(chunk)

      @chunks.shift
    end
    ws.close
  end
end
```

`on_drained` follows a refused write or close, or a publish skipped
under `when_full: :skip`; it runs once per streak, however many were
refused, and not on a socket that is closing. `ws.pending` reads the
queue without waiting, for apps that would rather slow down first.

## Topics

`Kino::WebSocket.publish(topic, body, binary: false)` sends `body` to
every socket subscribed to `topic`, from any Ractor or thread, and
returns at once. One native fan-out thread takes publishes in order and
builds one frame per distinct envelope, shared by every subscriber that
uses it: publishing to ten thousand sockets costs Ruby one call.

```ruby
ws.subscribe("room:7")                                     # each message as is
ws.subscribe("room:7", prefix: %({"identifier":"room-7","message":), suffix: "}")
ws.subscribe("ticker", when_full: :skip)                   # a slow client misses ticks
ws.unsubscribe("room:7", prefix: %({"identifier":"room-7","message":))  # that envelope
ws.unsubscribe("room:7")                                   # every envelope
Kino::WebSocket.publish("room:7", %({"text":"hi"}))
Kino::WebSocket.publish("quotes", packed, binary: true)    # a binary frame
```

- A subscriber gets `prefix + body + suffix` in one frame. Subscribing
  again with the same envelope keeps one subscription and takes the new
  `when_full`; a socket's subscriptions end with it. A subscription made
  in `on_open` gets every publish made after the client sees the socket
  open.
- `binary: true` decides the frame, never the String's encoding. A text
  publish raises `ArgumentError` unless `body` is valid UTF-8, and skips
  envelopes that are not; in a binary publish, body and envelope are
  bytes.
- `ws.unsubscribe(topic)` ends every subscription the socket has to
  `topic`; with `prefix:`, only those made with that prefix. It returns
  false when there was none.
- When a publish finds a subscriber's queue full, `when_full` decides:
  `:close`, the default, drops the socket as too slow (its `on_close`
  runs); `:skip` leaves that message out for that socket only and its
  `on_drained` runs once the queue has emptied. Skip suits tickers and
  dashboards, close suits protocols that cannot lose a message. Either
  way the fan-out never waits for a socket.
- `publish` returns false only when the fan-out queue is full (65,536
  messages, or 64 MiB); the message is not sent and nothing waits for
  room, so log it, drop it, or try again later.
- A subscribe or unsubscribe made during a fan-out waits for it, but
  lets the caller's other threads, and other Ractors, run meanwhile.
- Publishing is asynchronous: a `ws.write` made after a publish can
  reach the client first, and an unsubscribe right after it can keep it
  from the socket.

## Background work on a worker

Socket handlers often need work that no client message starts: deliver
an update a receiver thread fetched, flush a buffer.
`Kino::Worker.on_wake` runs a block on the worker's own thread, between
its requests and socket events and never alongside them, so the block
can use what the worker's handlers use without a lock:

```ruby
# kino.rb
lanes true
after_worker_boot Ractor.shareable_proc { |_worker_id|
  updates = Thread::Queue.new
  waker = Kino::Worker.on_wake { MyApp.deliver(updates.pop) until updates.empty? }
  Thread.new do
    loop do
      updates << MyApp.next_update  # blocks on a port, a socket, a queue
      waker.wake
    end
  end
}
```

- Call `on_wake` on a worker thread, in `after_worker_boot` or while
  serving. It sets that worker's hook (replacing an earlier one) and
  returns a `Kino::Worker::Waker`.
- `waker.wake` returns at once and works from any thread or Ractor (a
  Waker is frozen and shareable). Wakes that arrive before the hook runs
  coalesce. It returns false once the worker has exited or is retiring,
  and always without lanes; a later worker on the same slot is not woken
  by the old Waker.
- A hook that raises is logged and the worker keeps serving; `on_error`
  gets `{"kino.callback" => "on_wake"}` in place of the Rack env. A hook
  that wakes its own worker alternates with waiting requests, so it
  cannot starve them.
- Under a `worker_loop` it runs alongside requests and socket
  callbacks, never twice at once (see
  [worker-loop.md](worker-loop.md#the-wake-hook)).
- Without lanes Kino warns once and the hook never runs. There is no
  worker-side timer: for periodic work, start a thread that sleeps and
  calls `waker.wake`.

## Limits

- `websocket_max_message` (default 1 MiB) is the largest message a
  client may send, all fragments together; a bigger one closes the
  socket with 1009. A fragmented message can hold about twice that
  while a slow sender trickles it in.
- A socket may have 256 outgoing frames waiting, or 4 MiB, whichever
  fills first. Past either, writes return false and a publish drops or
  skips the socket, as its `when_full` says. A frame of any size fits
  an empty queue.
- A socket's worker may have 16 of its messages waiting or in
  `on_message`; at that many Kino stops reading the socket until a
  callback returns, and TCP pushes back on that client. A worker's
  sockets share an inbox of 1,024 events, which holds up their reading
  the same way when it fills. Nothing is dropped.
- `websocket_max` (default none) is how many native sockets may be open
  at once; a handshake past it gets a 503. Without it, only
  `max_connections` bounds sockets, and they can take every connection
  slot from plain requests.
- `websocket_ping_interval` (default 30 seconds) is how long a client
  may be silent before Kino pings it; one silent for twice that is
  dropped and its `on_close` runs. Any frame from the client breaks the
  silence, and so does a client taking what Kino writes; a client held
  up by its own slow worker never counts as silent. On macOS, which
  wakes blocked writers in batches, an interval below about half a
  second can drop a client that only reads, slowly. `nil` or 0 turns
  pings off. Kino answers the client's pings itself.
- Upgraded connections, native or hijacked, keep their
  `max_connections` slot while they stay open.

## Lifecycle and close codes

- A graceful drain says goodbye to every native socket when it starts:
  each socket's worker runs `on_shutdown` (if the handler has one), then
  Kino closes the socket with 1001, spread over a second (never more
  than half the drain's deadline) so clients do not all reconnect at
  once. The drain then waits, within `shutdown_timeout`, for each
  `on_close`; a socket still open at the deadline is cut without it.
- When a worker crashes, or is quarantined as wedged, its sockets close
  with 1011 and their `on_close` does not run: the handlers went with
  the worker. A request or handshake that arrives while the replacement
  starts waits for a worker, up to `queue_timeout`. A worker stuck in a
  callback or its wake hook reads as busy in `worker_status`, so
  `quarantine_timeout` covers it (under a `worker_loop`, by how long its
  thread stays away from its loop: see
  [worker-loop.md](worker-loop.md#stuck-workers-and-quarantine)).
- The elastic pool does not retire a worker that owns open sockets, so
  with long-lived sockets a pool that grew rarely shrinks back. A socket
  that opens on a worker the pool has just decided to retire is closed
  with 1001, and its `on_close` runs before the worker leaves.
- A client that vanished without closing (a dropped network, a laptop
  gone to sleep) is dropped after two silent ping intervals, without a
  close frame, so it does not keep its socket or its connection slot.
- Handshakes that arrive while the server drains, that reach a worker
  whose sockets are being closed, or that would pass `websocket_max`
  get a 503 with `connection: close`, and `on_open` does not run.

The close codes Kino sends:

- 1000: `ws.close` without a code.
- 1001: the server drains, or the socket's worker retires.
- 1002: the client broke the protocol (an unmasked frame, a bad opcode).
- 1007: a text message that is not valid UTF-8.
- 1009: a message larger than `websocket_max_message`.
- 1011: the socket's worker crashed or was quarantined.

When the client closes first, Kino echoes its code (1000 if it sent
none, 1002 for one no endpoint may send).

## `rack.hijack` and Action Cable

On HTTP/1.1 requests that carry an `Upgrade` header, in both modes and
with or without lanes, the env has `rack.hijack?` set to true and a
`rack.hijack` callable (Rack 3). No other request has them; an HTTP/2
request never does: h2 multiplexes streams on one connection, so no
request has a connection of its own to hand over.

- **Full hijack**: `env["rack.hijack"].call` returns a real socket (one
  end of a Unix socket pair), which nio4r can watch. The app writes its
  own response there, from any thread, now or later; Kino replays it
  through hyper, so TLS keeps working. On a 101 it then joins the two
  connections. Any other answer must carry a Content-Length (a
  handshake a library rejects with its own 400, say), unless it has no
  body (a 204, a 304, the answer to a HEAD): Kino sends it and closes
  the connection. One whose body is chunked, or runs until the close,
  or whose Content-Length is not one plain number, gets a 502 instead.
  The request body stays in `rack.input`, to read before or after the
  hijack. While another of the app's threads waits to read the body,
  the call raises an `IOError`. The returned triple is ignored, and its
  body is still closed. Kino does not set `rack.hijack_io` (Rack 2).
  `after_request_complete` does not fire.
- **Partial hijack**: a callable in the `rack.hijack` response header
  gets the response stream once the head is out, as a Rack 3 streaming
  body does, and `after_request_complete` fires once it returns. As for
  any [streamed response](streaming.md#streaming-responses), a write
  raises `Kino::ClientGone` once the client has gone.
- **A 101 with a callable**: a 101 answered with a partial hijack, or
  with a Rack 3 streaming body (`[101, headers, ->(io) { }]`), goes out
  with the response's headers, and the callable gets the switched
  connection as a socket. A partial hijack closes it when done (Rack
  leaves that to the app); a streaming body's is closed once the body
  returns.

Each hijacked connection keeps its `max_connections` slot and holds
three descriptors: the client's socket and both ends of the pair. With
many open, the process runs out of descriptors at about a third of
`ulimit -n` connections, before the default `max_connections` (80% of
it): raise the open-file limit, or set `max_connections` to a third of
it. Out of descriptors, a hijack raises an `IOError` in the app.

A worker whose hijacked connections are still open is not retired by
the elastic pool. A graceful drain does not wait for hijacked
connections: a switched one closes once requests and native sockets
are done, so an app serving it on its worker thread reads EOF and can
let the worker stop; the others close once the workers have stopped, so
an app still on its worker can answer first. A long poll still
unanswered then gets a 503. `request_timeout`, when set, also bounds
how long a hijacked request may take to write its head: keep it above
your long poll's interval.

Action Cable runs on Kino unmodified this way, in `:threaded` mode
(lanes not needed):

```ruby
# config/kino.rb
mode :threaded
threads 5
```

It does not run inside Ractors yet (see [Rails on
Ractors](rails-on-ractors.md#live-posts), whose example streams new
posts over native WebSockets instead).

## Stats

The WebSocket counters cover the whole process, as topics do. In
`server.stats` and `GET /stats` they sit under `websockets`, and
`GET /metrics` has each one as the series in parentheses.

- `open` (`kino_websockets_open`, a gauge): native WebSockets open now.
- `opened` (`kino_websockets_opened_total`): handshakes the app accepted
  that Kino took as sockets.
- `closed` (`kino_websockets_closed_total{cause="..."}`): sockets
  closed, by why. Every socket opened closes once, under the first
  cause recorded:
  - `normal`: a close frame from either side, or the app's `ws.abort`.
  - `going_away`: Kino's 1001 (a drain, a retiring worker), or a socket
    still open when the server stopped.
  - `protocol`: the client broke the protocol (1002, or 1007).
  - `too_big`: a message past `websocket_max_message` (1009).
  - `slow`: dropped because a publish found its queue full.
  - `dead_peer`: silent for two `websocket_ping_interval`s.
  - `client_gone`: the connection ended without a close frame.
  - `crash`: Kino's 1011, or a callback raised.
  - `handshake_failed`: the 101 never went out (`on_open` raised, or the
    request was answered otherwise first).
- `handshakes_refused`
  (`kino_websocket_handshakes_refused_total{cause="..."}`): accepted
  handshakes Kino answered with a 503, by why: `websocket_max`,
  `draining` (counted so even when `websocket_max` was reached too) or
  `worker_closing`. These never count as opened.
- `messages_received` and `messages_sent`
  (`kino_websocket_messages_received_total`,
  `kino_websocket_messages_sent_total`): messages from and to clients;
  a `ws.write_raw` counts once, and pings, pongs and closes not at all.
- `messages_skipped` (`kino_websocket_messages_skipped_total`):
  publishes left out under `when_full: :skip`, one per subscriber.
- `writes_refused` (`kino_websocket_writes_refused_total`): the app's
  writes and closes refused by a full queue.
- `publishes_refused` (`kino_websocket_publishes_refused_total`):
  publishes refused by a full fan-out queue.
- `inbound_pauses` (`kino_websocket_inbound_pauses_total`): times a
  socket's reading paused at 16 messages ahead of its worker.
- `received_bytes` and `sent_bytes`
  (`kino_websocket_received_bytes_total`,
  `kino_websocket_sent_bytes_total`): bytes read and written, frame
  headers and control frames included.

These are the server's own, at the top level:

- `hijacked_connections` (`kino_hijacked_connections`, a gauge):
  connections taken over by a full hijack, or switched by a 101 with a
  callable, and still open. A request leaves `in_flight` when the app
  takes it over and counts here instead.
- `wakes` (`kino_wakes_total`): wake hook runs handed to workers (under
  a `worker_loop`, every wake its loop takes).
- Each `worker_status` row has `websockets_open`
  (`kino_worker_websockets_open{worker="N"}`) and `hijacked_connections`
  (`kino_worker_hijacked_connections{worker="N"}`) for its slot.

## Not supported

- WebSockets over HTTP/2 (RFC 8441). Browsers open WebSockets over
  HTTP/1.1, also when the page itself came over h2.
- permessage-deflate compression.
- `rack.hijack` on HTTP/2. For plain streaming, return a Rack 3
  streaming body, which works on both protocols.
- Action Cable in `:ractor` mode.
