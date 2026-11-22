# Rack 3 and streaming responses

What Kino's Rack 3 support covers, and how a streamed response behaves
when its client is slow or gone. How the body channels work inside is
in [architecture.md](architecture.md#backpressure-in-both-directions).

## Rack 3 compliance

The spec suite runs every test app under `Rack::Lint` over real sockets:
streaming request bodies (forward-only `rack.input`), enumerable and
callable (full-duplex stream) response bodies, lowercase and multi-value
headers, HEAD/204 semantics. `rack.hijack`, full and partial, is offered
on HTTP/1.1 requests that ask to upgrade (see
[WebSockets](websockets.md#rackhijack-and-action-cable)).

## Streaming responses

A body that does not respond to `to_ary` streams: each chunk goes out
as the body yields it (chunked unless the app set `content-length`),
and a Rack 3 streaming body, one that responds to `call` but not
`each`, writes to the stream it is called with. A client slower than
the app makes the next write wait for it, with the GVL released.

Once the client has gone (it hung up, or its connection broke), the
body's next write raises `Kino::ClientGone`, an `IOError`, so an endless
body such as a server-sent event feed or an LLM token stream ends there
and frees its worker. A response that can carry no body (a HEAD
request, a 204 or 304, one already answered with a `request_timeout`
504) ends its body the same way. Kino closes the body and runs
`after_request_complete` as for any finished response, and logs
nothing; an `ensure` in the body is the place to clean up:

```ruby
class Ticks
  def each
    loop do
      yield "data: #{Time.now.to_i}\n\n"
      Kino.sleep(1)
    end
  ensure
    Kino::Log.info("ticker stopped")
  end
end

app = ->(env) { [200, {"content-type" => "text/event-stream"}, Ticks.new] }
```

A body that raises anything else mid-stream is logged and reported to
`on_error`, and its client sees the connection cut rather than a
response that looks complete. Cutting a stream short never waits on a
client that stopped reading, so such a client cannot hold up the worker
or a shutdown.
