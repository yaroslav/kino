//! `Kino::Native::Request`: the opaque per-request handle. Created INSIDE
//! the worker ractor by the take calls (queue/worker.rs), so the wrapping Ruby
//! object is owned by the ractor that will use it, and never crosses
//! ractor boundaries.

use std::cell::{Cell, RefCell, RefMut};
use std::sync::Arc;

use bytes::Bytes;
use magnus::r_hash::ForEach;
use magnus::value::ReprValue;
use magnus::{Error, RArray, RHash, RString, Ruby, Value};

use crate::gvl;
use crate::response::{full_body, BodyFrame, Cut, Responder};
use crate::server::Peer;

pub struct RequestCtx {
    pub method: http::Method,
    pub uri: http::Uri,
    pub version: http::Version,
    pub headers: http::HeaderMap,
    pub peer: Peer,
    pub https: bool,
    /// Request body, streamed from hyper through a bounded channel: hyper is
    /// only polled as Ruby consumes, so inbound backpressure is free. None
    /// when no byte can ever arrive (most GETs): bodyless requests skip the
    /// channel allocation entirely.
    pub body_rx: Option<flume::Receiver<Bytes>>,
    /// When a frame is bigger than read_body's max_len, the rest waits here.
    pub leftover: Cell<Option<Bytes>>,
    /// The owning worker slot (set at admit time, queue/worker.rs); its interrupt
    /// flag makes blocked body reads/writes interruptible like the queue pop.
    pub slot: Option<Arc<crate::slot::WorkerSlot>>,
    /// The server's zero-copy roots: large response bodies are pinned
    /// here and ride to hyper without a copy (pin.rs).
    pub pin_slab: Arc<crate::pin::PinSlab>,
    pub responder: Arc<Responder>,
    /// When this request entered the queue, for the queue-wait histogram.
    /// Stamped at ctx creation; read once at admit (queue/worker.rs).
    pub enqueued_at: std::time::Instant,
    /// Whether the access log wants timing: decided at intake, read on
    /// the way out, so an idle log costs nothing per request.
    pub timed: bool,
    /// Queue wait, stamped at admit (queue/worker.rs): the log's `wait`.
    pub wait: std::time::Duration,
    /// When a worker took the request; elapsed at the response head it is
    /// the log's `ruby`.
    pub admitted_at: std::time::Instant,
    /// GC pause and objects allocated during the app call, when the
    /// worker measured them (Request#timing). A Cell: another thread may wait.
    pub gc: Cell<Option<(std::time::Duration, u64)>>,
    /// HTTP/1.1 with an Upgrade header: ws::accept or rack.hijack claims the connection.
    pub upgrade: Option<crate::upgrade::Upgrade>,
    pub hijacked: bool,
    /// Whose in-flight count a hijack leaves.
    pub server_id: u64,
}

impl RequestCtx {
    /// A request as it joins the queue: its wait starts now.
    pub fn new(
        parts: http::request::Parts,
        peer: Peer,
        server: &crate::registry::ServerInner,
        responder: Arc<Responder>,
        body_rx: Option<flume::Receiver<Bytes>>,
        upgrade: Option<crate::upgrade::Upgrade>,
    ) -> crate::registry::BoxedCtx {
        let now = std::time::Instant::now();
        Box::new(RequestCtx {
            method: parts.method,
            uri: parts.uri,
            version: parts.version,
            headers: parts.headers,
            peer,
            https: server.config.tls.is_some(),
            body_rx,
            leftover: Cell::new(None),
            slot: None,
            pin_slab: server.pin_slab.clone(),
            responder,
            enqueued_at: now,
            timed: server.access_log.is_some(),
            wait: std::time::Duration::ZERO,
            admitted_at: now,
            gc: Cell::new(None),
            upgrade,
            hijacked: false,
            server_id: server.id,
        })
    }

    /// A plain answer: the task holding the connection for a claim ends now, not at GC.
    pub fn decline_upgrade(&mut self) {
        self.upgrade = None;
    }

    pub fn offers_websocket(&self) -> bool {
        self.upgrade
            .as_ref()
            .and_then(crate::upgrade::Upgrade::websocket)
            .is_some()
    }

    /// The timing this request carries to the access log.
    fn timing(&self) -> crate::access_log::Timing {
        crate::access_log::Timing {
            wait: self.wait,
            ruby: self.admitted_at.elapsed(),
            gc: self.gc.get(),
        }
    }
}

/// Attach the request's timing to a response head when the access log
/// wants it; the extension is the one allocation an idle log skips.
fn timed(
    ctx: &RequestCtx,
    builder: hyper::http::response::Builder,
) -> hyper::http::response::Builder {
    if ctx.timed {
        builder.extension(ctx.timing())
    } else {
        builder
    }
}

impl Drop for RequestCtx {
    /// Universal backstop: whatever kills this request (Ruby exception that
    /// escaped the worker rescue, ractor death, GC of an abandoned handle),
    /// the client gets a 500/abort instead of a hung connection. Never
    /// touches the Ruby API: safe under `free_immediately`.
    fn drop(&mut self) {
        self.responder.give_up(Cut::Crash);
    }
}

/// Block on a channel operation with the GVL released, waking on the
/// slot's interrupt flag. `attempt` performs one bounded tick and returns
/// Some when done; None on timeout. Ok(None) = interrupted. Every Ruby
/// handle is created by `admit` (queue/worker.rs), which always sets the slot.
fn block_on<T>(
    slot: &Option<Arc<crate::slot::WorkerSlot>>,
    attempt: impl FnMut() -> Option<T>,
) -> Result<Option<T>, Error> {
    let slot = slot.as_ref().expect("slot set by admit");
    gvl::interruptible(&slot.life.interrupted, attempt)
}

fn interrupted_error(ruby: &Ruby) -> Error {
    Error::new(
        ruby.exception_runtime_error(),
        "Kino: request interrupted during shutdown",
    )
}

fn body_too_large_error(ruby: &Ruby) -> Error {
    Error::new(
        ruby.exception_runtime_error(),
        "Kino: request body exceeded max_body_size",
    )
}

fn body_timeout_error(ruby: &Ruby) -> Error {
    Error::new(
        ruby.exception_runtime_error(),
        "Kino: request body read timed out",
    )
}

/// A write nothing reaches any more (hyper dropped the body, or Kino gave the stream
/// up): Kino::ClientGone, an IOError, ends the body's write loop.
fn client_gone_error(ruby: &Ruby) -> Error {
    use magnus::Module;

    let class = ruby
        .class_object()
        .const_get::<_, magnus::RModule>("Kino")
        .and_then(|kino| kino.const_get::<_, magnus::ExceptionClass>("ClientGone"))
        .unwrap_or_else(|_| ruby.exception_io_error());
    Error::new(class, "Kino: the response stream is closed")
}

/// Why what needs the request to itself cannot happen now.
pub(crate) const BUSY: &str =
    "another of the app's threads is waiting on this request (reading its body or writing its response)";

fn busy_error(ruby: &Ruby) -> Error {
    Error::new(ruby.exception_runtime_error(), format!("Kino: {BUSY}"))
}

fn invalid_response(ruby: &Ruby, e: impl std::fmt::Display) -> Error {
    Error::new(
        ruby.exception_runtime_error(),
        format!("invalid response: {e}"),
    )
}

#[magnus::wrap(class = "Kino::Native::Request", free_immediately)]
pub struct Request(pub RefCell<RequestCtx>);

impl Request {
    /// None while another thread holds the request shared, waiting with the GVL released
    /// in read_body or write_chunk, where borrow_mut would panic.
    fn exclusive(&self) -> Option<RefMut<'_, RequestCtx>> {
        self.0.try_borrow_mut().ok()
    }

    /// While another thread waits on the request, the upgrade is declined at GC instead.
    fn decline_upgrade(&self) {
        if let Some(mut ctx) = self.exclusive() {
            ctx.decline_upgrade();
        }
    }
}

/// Build the complete Rack env for a request. Static keys and common
/// values come from the frozen, Ractor-shareable cache (env_strings);
/// only genuinely dynamic strings are allocated. Called by `admit`
/// (queue/worker.rs) with the GVL held, before the ctx is wrapped.
pub fn build_env(ruby: &Ruby, ctx: &RequestCtx) -> Result<RHash, Error> {
    use crate::env_strings;

    let s = env_strings::get();
    // Pre-sized: a dozen static keys plus one per header lands near 30
    // entries, which a default Hash reaches only via the ar-to-st
    // migration and a rehash per doubling.
    let env = ruby.hash_new_capa(32);
    // Opaque -> live RString for this thread's Ruby handle.
    let live = |o: magnus::value::Opaque<RString>| ruby.get_inner(o);

    let method = match s.methods.get(ctx.method.as_str()) {
        Some(cached) => live(*cached),
        None => ruby.str_new(ctx.method.as_str()),
    };
    env.aset(live(s.request_method), method)?;
    env.aset(live(s.script_name), live(s.empty))?;
    env.aset(
        live(s.path_info),
        ruby.str_from_slice(ctx.uri.path().as_bytes()),
    )?;
    let query = match ctx.uri.query() {
        Some(q) if !q.is_empty() => ruby.str_from_slice(q.as_bytes()),
        _ => live(s.empty),
    };
    env.aset(live(s.query_string), query)?;
    let protocol = match ctx.version {
        http::Version::HTTP_10 => s.http10,
        http::Version::HTTP_2 => s.http2,
        _ => s.http11,
    };
    env.aset(live(s.server_protocol), live(protocol))?;
    env_strings::set_addr_env(ruby, env, ctx.peer.remote.ip())?;
    // The scheme is the transport's to know, not the app config's.
    let scheme = if ctx.https { s.https } else { s.http };
    env.aset(live(s.rack_url_scheme), live(scheme))?;

    // rack.errors: frozen shareable singleton. rack.input: ditto when
    // there is no body (most GETs); the worker only builds a real
    // Input when bytes can actually arrive.
    if let Some(errors) = *s.errors_stream.read() {
        env.aset(live(s.rack_errors), ruby.get_inner(errors))?;
    }
    if !body_possible(ctx) {
        if let Some(null_input) = *s.null_input.read() {
            env.aset(live(s.rack_input), ruby.get_inner(null_input))?;
        }
    }

    for name in ctx.headers.keys() {
        let key = if *name == http::header::CONTENT_TYPE {
            live(s.content_type)
        } else if *name == http::header::CONTENT_LENGTH {
            live(s.content_length)
        } else {
            match s.header_names.get(name.as_str()) {
                Some(cached) => live(*cached),
                None => ruby.str_new(&env_strings::cgi_name(name.as_str())),
            }
        };

        // Single-value fast path (the overwhelming majority); the value
        // cache turns low-cardinality repeats (UA, accept-*, sec-ch-*)
        // into a lookup of one frozen string instead of an allocation.
        let mut values = ctx.headers.get_all(name).iter();
        let first = values.next().map(|v| v.as_bytes()).unwrap_or(b"");
        match values.next() {
            None => env_strings::set_value_env(ruby, env, key, name.as_str(), first)?,
            Some(second) => {
                let sep: &[u8] = if name == http::header::COOKIE {
                    b"; "
                } else {
                    b", "
                };
                // Joined multi-value headers (cookies, mostly) are
                // nearly always short: stay on the stack.
                let mut joined = smallvec::SmallVec::<[u8; 256]>::new();
                joined.extend_from_slice(first);
                joined.extend_from_slice(sep);
                joined.extend_from_slice(second.as_bytes());
                for v in values {
                    joined.extend_from_slice(sep);
                    joined.extend_from_slice(v.as_bytes());
                }
                env.aset(key, ruby.str_from_slice(&joined))?;
            }
        }
    }

    // SERVER_NAME/SERVER_PORT/HTTP_HOST, best source first: the request
    // URI's authority (every h2 request via :authority; also h1
    // absolute-form, where it beats Host per RFC 9112), then the Host
    // header (HTTP/1.1 requires it), then the accepted socket's local
    // address. Runs after the header loop so an authority-derived
    // HTTP_HOST wins over a literal host header a proxy forwarded.
    if let Some(authority) = ctx.uri.authority() {
        let local_port = ctx.peer.local.port();
        env_strings::set_authority_env(ruby, env, authority.as_str(), || {
            (
                authority.host().to_string(),
                authority.port_u16().unwrap_or(local_port),
            )
        })?;
    } else {
        match ctx.headers.get(http::header::HOST) {
            Some(host) => {
                let local_port = ctx.peer.local.port();
                env_strings::set_host_env(
                    ruby,
                    env,
                    host.as_bytes(),
                    || match std::str::from_utf8(host.as_bytes()) {
                        Ok(h) => split_host_port(h, local_port),
                        Err(_) => (
                            String::from_utf8_lossy(host.as_bytes()).into_owned(),
                            local_port,
                        ),
                    },
                )?;
            }
            None => {
                let ip = ctx.peer.local.ip();
                let port = ctx.peer.local.port();
                env_strings::set_host_env(ruby, env, format!("\0{ip}:{port}").as_bytes(), || {
                    (ip.to_string(), port)
                })?;
            }
        }
    }

    Ok(env)
}

/// Bodyless requests carry no channel at all; a present channel that is
/// disconnected and empty (EOF already reached) also has nothing left.
fn body_possible(ctx: &RequestCtx) -> bool {
    let leftover = ctx.leftover.take();
    let left = leftover.is_some();
    ctx.leftover.set(leftover);
    left || ctx
        .body_rx
        .as_ref()
        .is_some_and(|rx| !(rx.is_empty() && rx.is_disconnected()))
}

/// Complete response in one shot: status, the Rack headers Hash (iterated
/// here, no intermediate array on the Ruby side), full body. Free function
/// shared by the `send_simple` binding and the fused respond_and_take path
/// (queue/worker.rs).
pub fn respond_simple(
    ruby: &Ruby,
    request: &Request,
    status: u16,
    headers: RHash,
    body: RString,
) -> Result<bool, Error> {
    request.decline_upgrade();
    let ctx = request.0.borrow();
    let builder = timed(&ctx, build_head(status, headers)?);
    let bytes = body_bytes(&ctx, body);
    let response = builder
        .body(full_body(bytes))
        .map_err(|e| invalid_response(ruby, e))?;
    Ok(ctx.responder.send_response(response))
}

/// Body bytes for hyper: large bodies are pinned in place (zero-copy),
/// everything else is copied out of the Ruby string, which may be
/// mutated or collected the moment we return.
fn body_bytes(ctx: &RequestCtx, body: RString) -> Bytes {
    if let Some(bytes) = crate::pin::pinned_bytes(&ctx.pin_slab, body) {
        return bytes;
    }
    Bytes::copy_from_slice(unsafe { body.as_slice() })
}

impl Request {
    /// Accept a handshake (ws::accept), its 101 carrying `headers` for switch_protocols: the
    /// socket id, false for Kino's 503 refusal, nil when it is no handshake or there are no lanes.
    pub fn upgrade_websocket(
        ruby: &Ruby,
        rb_self: &Request,
        headers: RHash,
        on_shutdown: bool,
    ) -> Result<magnus::Value, Error> {
        let head = build_head(101, headers)?
            .body(())
            .map_err(|e| invalid_response(ruby, e))?;
        let extra: Vec<_> = head
            .headers()
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect();
        let mut ctx = rb_self.exclusive().ok_or_else(|| busy_error(ruby))?;
        Ok(match crate::ws::accept(&mut ctx, &extra, on_shutdown) {
            crate::ws::Accept::Opened(id) => ruby.into_value(id),
            crate::ws::Accept::Refused => ruby.qfalse().as_value(),
            crate::ws::Accept::Declined => ruby.qnil().as_value(),
        })
    }

    /// The accepted handshake's 101, once on_open ran; false when the request was answered
    /// otherwise first, which closes the connection.
    pub fn switch_protocols(ruby: &Ruby, rb_self: &Request) -> Result<bool, Error> {
        let ctx = rb_self.exclusive().ok_or_else(|| busy_error(ruby))?;
        Ok(ctx.responder.switch())
    }

    /// rack.hijack: the fd of the app's end of the bridge.
    pub fn hijack(ruby: &Ruby, rb_self: &Request) -> Result<i32, Error> {
        rb_self.hijacked_fd(ruby, None)
    }

    /// A 101 with a callable (a partial hijack, or a Rack 3 streaming body): sends it with
    /// `headers` and takes the connection over; the fd of the app's end.
    pub fn hijack_switched(ruby: &Ruby, rb_self: &Request, headers: RHash) -> Result<i32, Error> {
        let head = build_head(101, headers)?
            .body(full_body(Bytes::new()))
            .map_err(|e| invalid_response(ruby, e))?;
        rb_self.hijacked_fd(ruby, Some(head))
    }

    /// The fd of the app's end of a bridge that took the connection over (hijack::take).
    fn hijacked_fd(
        &self,
        ruby: &Ruby,
        head: Option<crate::response::HyperResponse>,
    ) -> Result<i32, Error> {
        use std::os::fd::IntoRawFd;

        let taken = match self.exclusive() {
            Some(mut ctx) => crate::hijack::take(&mut ctx, head),
            None => Err(crate::hijack::Refused::Busy),
        };
        let io = taken.map_err(|refused| {
            Error::new(
                ruby.exception_io_error(),
                format!("Kino: rack.hijack failed: {refused}"),
            )
        })?;
        Ok(io.into_raw_fd())
    }

    pub fn hijacked(_ruby: &Ruby, rb_self: &Request) -> bool {
        rb_self.0.borrow().hijacked
    }

    /// The worker's measurements around the app call, for the access log's
    /// breakdown: the GC pause in nanoseconds and the objects allocated.
    /// Called only when the access log is on.
    pub fn set_timing(_ruby: &Ruby, rb_self: &Request, gc_nanos: u64, allocs: u64) {
        let gc = std::time::Duration::from_nanos(gc_nanos);
        rb_self.0.borrow().gc.set(Some((gc, allocs)));
    }

    /// Next chunk of the request body, at most `max_len` bytes; nil at EOF.
    /// Blocks (GVL released) until the client sends more.
    pub fn read_body(
        ruby: &Ruby,
        rb_self: &Request,
        max_len: usize,
    ) -> Result<Option<RString>, Error> {
        let max_len = max_len.max(1);

        // Shared across the wait: other threads may abort or answer the request meanwhile.
        let ctx = rb_self.0.borrow();
        let mut chunk = match ctx.leftover.take() {
            Some(bytes) => bytes,
            None => {
                let Some(body_rx) = ctx.body_rx.clone() else {
                    return Ok(None); // bodyless request: EOF from the start
                };
                let outcome = block_on(&ctx.slot, || {
                    match body_rx.recv_timeout(crate::queue::TICK) {
                        Ok(bytes) => Some(Some(bytes)),
                        Err(flume::RecvTimeoutError::Timeout) => None,
                        Err(flume::RecvTimeoutError::Disconnected) => Some(None),
                    }
                })?;
                match outcome {
                    Some(Some(bytes)) => bytes,
                    Some(None) => {
                        // Disconnected: a clean EOF, unless the forwarder
                        // abandoned the body (too large, or the client stalled).
                        use crate::response::BodyAbandon;
                        return match ctx.responder.body_abandoned() {
                            Some(BodyAbandon::Oversize) => Err(body_too_large_error(ruby)),
                            Some(BodyAbandon::TimedOut) => Err(body_timeout_error(ruby)),
                            None => Ok(None), // EOF
                        };
                    }
                    None => return Err(interrupted_error(ruby)),
                }
            }
        };

        if chunk.len() > max_len {
            ctx.leftover.set(Some(chunk.split_off(max_len)));
            return Ok(Some(ruby.str_from_slice(&chunk)));
        }

        // Greedy drain: whatever the forwarder has already queued rides
        // out in this same crossing, up to max_len. A body arriving as
        // small DATA frames (HTTP/2 caps them at 16 KB unless negotiated
        // higher) then costs one GVL round-trip and one Ruby string, not
        // one per frame. Non-blocking: a chunk that has not arrived yet
        // is next call's business, as are EOF and abandonment (try_recv's
        // Disconnected included; the recv above maps them to errors).
        let mut assembled: Option<RString> = None;
        if let Some(body_rx) = ctx.body_rx.clone() {
            let mut total = chunk.len();
            while total < max_len {
                let Ok(mut more) = body_rx.try_recv() else {
                    break;
                };
                if more.len() > max_len - total {
                    ctx.leftover.set(Some(more.split_off(max_len - total)));
                }
                total += more.len();
                let out = *assembled.get_or_insert_with(|| {
                    let out = ruby.str_buf_new(max_len);
                    out.cat(&chunk[..]);
                    out
                });
                out.cat(&more[..]);
            }
        }
        Ok(Some(match assembled {
            Some(out) => out,
            None => ruby.str_from_slice(&chunk),
        }))
    }

    /// Start a streaming response: head goes out now, chunks follow via
    /// write_chunk, terminated by finish. Once Kino answered, the first write ends the body.
    pub fn send_headers(
        ruby: &Ruby,
        rb_self: &Request,
        status: u16,
        headers: RHash,
    ) -> Result<bool, Error> {
        rb_self.decline_upgrade();
        let ctx = rb_self.0.borrow();
        let builder = timed(&ctx, build_head(status, headers)?);
        let bodiless = crate::framing::bodiless(status, ctx.method == http::Method::HEAD);
        ctx.responder
            .send_stream_head(builder, bodiless)
            .map_err(|e| invalid_response(ruby, e))
    }

    /// One body chunk. Blocks (GVL released) when the client is slower than
    /// the app: this is the outbound backpressure. Raises ClientGone once the
    /// client is gone or Kino gave the stream up.
    pub fn write_chunk(ruby: &Ruby, rb_self: &Request, chunk: RString) -> Result<(), Error> {
        let ctx = rb_self.0.borrow();
        let Some(sender) = ctx.responder.body_sender() else {
            if ctx.responder.stream_cut() {
                return Err(client_gone_error(ruby));
            }
            return Err(Error::new(
                ruby.exception_runtime_error(),
                "Kino: response stream not started or already finished",
            ));
        };
        let bytes = body_bytes(&ctx, chunk);
        let mut pending = Some(Ok(BodyFrame::data(bytes)));

        let outcome = block_on(&ctx.slot, || {
            let frame = pending.take().expect("frame consumed twice");
            match sender.send_timeout(frame, crate::queue::TICK) {
                Ok(()) => Some(true),
                Err(flume::SendTimeoutError::Timeout(_)) if ctx.responder.stream_cut() => {
                    Some(false)
                }
                Err(flume::SendTimeoutError::Timeout(frame)) => {
                    pending = Some(frame); // client slow; keep waiting
                    None
                }
                Err(flume::SendTimeoutError::Disconnected(_)) => Some(false),
            }
        })?;
        match outcome {
            Some(true) => Ok(()),
            // Gone, or given up (and counted then).
            Some(false) => {
                ctx.responder.cut_stream(crate::response::Cut::ClientGone);
                Err(client_gone_error(ruby))
            }
            None => Err(interrupted_error(ruby)),
        }
    }

    /// Clean end of a streaming response.
    pub fn finish(_ruby: &Ruby, rb_self: &Request) -> Result<(), Error> {
        rb_self.0.borrow().responder.finish_stream();
        Ok(())
    }

    /// Give up on this request: canned 500 if the head wasn't sent, abort
    /// the connection if mid-stream. Used by the worker's rescue path. It
    /// borrows no more than threads waiting on the request do, so a waiting write fails.
    pub fn abort(_ruby: &Ruby, rb_self: &Request) -> Result<(), Error> {
        rb_self.decline_upgrade();
        if let Ok(ctx) = rb_self.0.try_borrow() {
            ctx.responder.give_up(Cut::Crash);
        }
        Ok(())
    }
}

/// Build the response head straight from the Rack headers Hash.
/// Values are Strings or Arrays of Strings (Rack 3); each Array element
/// becomes a repeated header. Name/value bytes are borrowed in place from
/// rooted Ruby strings: safe because the GVL is held and `builder.header`
/// copies the bytes immediately.
fn build_head(status: u16, headers: RHash) -> Result<hyper::http::response::Builder, Error> {
    let mut builder = Some(hyper::Response::builder().status(status));
    headers.foreach(|name: Value, value: Value| {
        let name = coerce_str(name)?;
        let take = |b: &mut Option<hyper::http::response::Builder>, v: RString| {
            let next = b
                .take()
                .expect("builder always present")
                .header(unsafe { name.as_slice() }, unsafe { v.as_slice() });
            *b = Some(next);
        };
        if let Some(values) = RArray::from_value(value) {
            for i in 0..values.len() {
                take(&mut builder, coerce_str(values.entry(i as isize)?)?);
            }
        } else {
            take(&mut builder, coerce_str(value)?);
        }
        Ok(ForEach::Continue)
    })?;
    Ok(builder.expect("builder always present"))
}

/// Rack SPEC says header names and values are Strings, but real apps set
/// booleans, numbers, and symbols, and Puma serves them via to_s. Match
/// that: Strings pass through untouched (the hot path), anything else pays
/// one to_s call back into Ruby.
fn coerce_str(value: Value) -> Result<RString, Error> {
    match RString::from_value(value) {
        Some(s) => Ok(s),
        None => value.funcall("to_s", ()),
    }
}

pub(crate) fn split_host_port(host: &str, default_port: u16) -> (String, u16) {
    match host.rsplit_once(':') {
        Some((name, port)) if !name.is_empty() => match port.parse() {
            Ok(p) => (name.to_string(), p),
            Err(_) => (host.to_string(), default_port),
        },
        _ => (host.to_string(), default_port),
    }
}

/// A minimal ctx for pure-Rust tests (no Ruby involved). Dropping it
/// fires the Drop-500 backstop into a dropped receiver, which is a no-op.
#[cfg(test)]
pub fn test_ctx() -> crate::registry::BoxedCtx {
    let (head_tx, _head_rx) = tokio::sync::oneshot::channel();
    let (parts, ()) = http::Request::new(()).into_parts();
    let peer = Peer {
        remote: "127.0.0.1:40000".parse().expect("static addr"),
        local: "127.0.0.1:9292".parse().expect("static addr"),
    };
    let server = crate::testing::TestServer::shared_queue().build();
    let responder = Arc::new(Responder::new(head_tx));
    RequestCtx::new(parts, peer, &server, responder, None, None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_port_split() {
        assert_eq!(
            split_host_port("example.com:8080", 80),
            ("example.com".into(), 8080)
        );
        assert_eq!(
            split_host_port("example.com", 80),
            ("example.com".into(), 80)
        );
    }

    #[test]
    fn host_port_split_edge_cases() {
        // Bracketed IPv6 (the RFC-correct Host form) keeps the brackets.
        assert_eq!(split_host_port("[::1]:8080", 80), ("[::1]".into(), 8080));
        // Out-of-range or non-numeric port: whole input + default port.
        assert_eq!(
            split_host_port("example.com:99999", 80),
            ("example.com:99999".into(), 80)
        );
        assert_eq!(
            split_host_port("example.com:", 80),
            ("example.com:".into(), 80)
        );
        // Empty name (":8080") falls back whole.
        assert_eq!(split_host_port(":8080", 80), (":8080".into(), 80));
    }

    #[test]
    fn body_possible_reflects_channel_and_leftover_state() {
        // test_ctx carries no channel: bodyless by construction.
        let ctx = test_ctx();
        assert!(!body_possible(&ctx));

        // A leftover chunk means there is still body to read.
        let ctx = test_ctx();
        ctx.leftover.set(Some(bytes::Bytes::from_static(b"tail")));
        assert!(body_possible(&ctx));

        // A live sender means bytes may still arrive.
        let (body_tx, body_rx) = flume::bounded(1);
        let mut ctx = test_ctx();
        ctx.body_rx = Some(body_rx);
        assert!(body_possible(&ctx));
        drop(body_tx);

        // A dropped sender with nothing queued: EOF already reached.
        let (body_tx, body_rx) = flume::bounded::<bytes::Bytes>(1);
        drop(body_tx);
        let mut ctx = test_ctx();
        ctx.body_rx = Some(body_rx);
        assert!(!body_possible(&ctx));

        // Disconnected but with a queued chunk: still readable.
        let (body_tx, body_rx) = flume::bounded(1);
        body_tx.send(bytes::Bytes::from_static(b"x")).unwrap();
        drop(body_tx);
        let mut ctx = test_ctx();
        ctx.body_rx = Some(body_rx);
        assert!(body_possible(&ctx));
    }
}
