//! `Kino::Native::Request`: the opaque per-request handle. Created INSIDE
//! the worker ractor by the take calls (queue.rs), so the wrapping Ruby
//! object is owned by the ractor that will use it, and never crosses
//! ractor boundaries.

use std::cell::RefCell;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use magnus::r_hash::ForEach;
use magnus::value::ReprValue;
use magnus::{Error, RArray, RHash, RString, Ruby, Value};

use crate::gvl;
use crate::response::{full_body, BodyFrame, Responder};

pub struct RequestCtx {
    pub method: http::Method,
    pub uri: http::Uri,
    pub version: http::Version,
    pub headers: http::HeaderMap,
    pub remote_addr: SocketAddr,
    pub local_addr: SocketAddr,
    pub https: bool,
    /// Request body, streamed from hyper through a bounded channel: hyper is
    /// only polled as Ruby consumes, so inbound backpressure is free.
    pub body_rx: flume::Receiver<Bytes>,
    /// Set by the body forwarder when the body exceeded max_body_size: turns
    /// the next read into an error instead of a (truncated) clean EOF.
    pub body_overflow: Arc<std::sync::atomic::AtomicBool>,
    /// Set by the body forwarder when the client stalled past the idle
    /// deadline: the next read raises so the worker reclaims its slot.
    pub body_timeout: Arc<std::sync::atomic::AtomicBool>,
    /// When a frame is bigger than read_body's max_len, the rest waits here.
    pub leftover: Option<Bytes>,
    /// The owning worker slot (set at admit time, queue.rs); its interrupt
    /// flag makes blocked body reads/writes interruptible like the queue pop.
    pub slot: Option<Arc<crate::registry::WorkerSlot>>,
    /// The server's zero-copy roots: large response bodies are pinned
    /// here and ride to hyper without a copy (pin.rs).
    pub pin_slab: Arc<crate::pin::PinSlab>,
    pub responder: Arc<Responder>,
}

impl Drop for RequestCtx {
    /// Universal backstop: whatever kills this request (Ruby exception that
    /// escaped the worker rescue, ractor death, GC of an abandoned handle),
    /// the client gets a 500/abort instead of a hung connection. Never
    /// touches the Ruby API: safe under `free_immediately`.
    fn drop(&mut self) {
        self.responder.respond_500_if_unsent();
    }
}

/// Block on a channel operation with the GVL released, waking on the
/// slot's interrupt flag. `attempt` performs one bounded tick and returns
/// Some when done; None on timeout. Ok(None) = interrupted. Every Ruby
/// handle is created by `admit` (queue.rs), which always sets the slot.
fn block_on<T>(
    slot: &Option<Arc<crate::registry::WorkerSlot>>,
    attempt: impl FnMut() -> Option<T>,
) -> Result<Option<T>, Error> {
    let slot = slot.as_ref().expect("slot set by admit");
    gvl::interruptible(&slot.interrupted, attempt)
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

fn invalid_response(ruby: &Ruby, e: impl std::fmt::Display) -> Error {
    Error::new(
        ruby.exception_runtime_error(),
        format!("invalid response: {e}"),
    )
}

#[magnus::wrap(class = "Kino::Native::Request", free_immediately)]
pub struct Request(pub RefCell<RequestCtx>);

/// Build the complete Rack env for a request. Static keys and common
/// values come from the frozen, Ractor-shareable cache (env_strings);
/// only genuinely dynamic strings are allocated. Called by `admit`
/// (queue.rs) with the GVL held, before the ctx is wrapped.
pub fn build_env(ruby: &Ruby, ctx: &RequestCtx) -> Result<RHash, Error> {
    use crate::env_strings;

    let s = env_strings::get();
    let env = ruby.hash_new();
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
        _ => s.http11,
    };
    env.aset(live(s.server_protocol), live(protocol))?;
    env_strings::set_addr_env(ruby, env, ctx.remote_addr.ip())?;
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

    // SERVER_NAME/PORT: prefer the Host header (HTTP/1.1 requires it),
    // fall back to the accepted socket's local address.
    let host_header = ctx.headers.get(http::header::HOST);
    match host_header {
        Some(host) => {
            let local_port = ctx.local_addr.port();
            env_strings::set_host_env(ruby, env, host.as_bytes(), || {
                match std::str::from_utf8(host.as_bytes()) {
                    Ok(h) => split_host_port(h, local_port),
                    Err(_) => (
                        String::from_utf8_lossy(host.as_bytes()).into_owned(),
                        local_port,
                    ),
                }
            })?;
        }
        None => {
            let ip = ctx.local_addr.ip();
            let port = ctx.local_addr.port();
            env_strings::set_host_env(ruby, env, format!("\0{ip}:{port}").as_bytes(), || {
                (ip.to_string(), port)
            })?;
        }
    }

    for name in ctx.headers.keys() {
        // Single-value fast path (the overwhelming majority).
        let mut values = ctx.headers.get_all(name).iter();
        let first = values.next().map(|v| v.as_bytes()).unwrap_or(b"");
        let value = match values.next() {
            None => ruby.str_from_slice(first),
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
                ruby.str_from_slice(&joined)
            }
        };

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
        env.aset(key, value)?;
    }

    Ok(env)
}

/// Bodyless requests get their channel sender dropped at accept time, so
/// "disconnected and empty" means no byte can ever arrive.
fn body_possible(ctx: &RequestCtx) -> bool {
    ctx.leftover.is_some() || !(ctx.body_rx.is_empty() && ctx.body_rx.is_disconnected())
}

/// Complete response in one shot: status, the Rack headers Hash (iterated
/// here, no intermediate array on the Ruby side), full body. Free function
/// shared by the `send_simple` binding and the fused respond_and_take path
/// (queue.rs).
pub fn respond_simple(
    ruby: &Ruby,
    request: &Request,
    status: u16,
    headers: RHash,
    body: RString,
) -> Result<bool, Error> {
    let ctx = request.0.borrow();
    let builder = build_head(status, headers)?;
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
    /// Next chunk of the request body, at most `max_len` bytes; nil at EOF.
    /// Blocks (GVL released) until the client sends more.
    pub fn read_body(
        ruby: &Ruby,
        rb_self: &Request,
        max_len: usize,
    ) -> Result<Option<RString>, Error> {
        let mut ctx = rb_self.0.borrow_mut();
        let max_len = max_len.max(1);

        let mut chunk = match ctx.leftover.take() {
            Some(bytes) => bytes,
            None => {
                let body_rx = ctx.body_rx.clone();
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
                        if ctx.body_overflow.load(std::sync::atomic::Ordering::Relaxed) {
                            return Err(body_too_large_error(ruby));
                        }
                        if ctx.body_timeout.load(std::sync::atomic::Ordering::Relaxed) {
                            return Err(body_timeout_error(ruby));
                        }
                        return Ok(None); // EOF
                    }
                    None => return Err(interrupted_error(ruby)),
                }
            }
        };

        if chunk.len() > max_len {
            ctx.leftover = Some(chunk.split_off(max_len));
        }
        Ok(Some(ruby.str_from_slice(&chunk)))
    }

    /// Start a streaming response: head goes out now, chunks follow via
    /// write_chunk, terminated by finish.
    pub fn send_headers(
        ruby: &Ruby,
        rb_self: &Request,
        status: u16,
        headers: RHash,
    ) -> Result<bool, Error> {
        let ctx = rb_self.0.borrow();
        let builder = build_head(status, headers)?;
        ctx.responder
            .send_stream_head(builder)
            .map_err(|e| invalid_response(ruby, e))
    }

    /// One body chunk. Blocks (GVL released) when the client is slower than
    /// the app: this is the outbound backpressure.
    pub fn write_chunk(ruby: &Ruby, rb_self: &Request, chunk: RString) -> Result<(), Error> {
        let ctx = rb_self.0.borrow();
        let Some(sender) = ctx.responder.body_sender() else {
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
                Err(flume::SendTimeoutError::Timeout(frame)) => {
                    pending = Some(frame); // client slow; keep waiting
                    None
                }
                Err(flume::SendTimeoutError::Disconnected(_)) => Some(false),
            }
        })?;
        match outcome {
            // Receiver dropped = client went away; the app keeps writing
            // into the void harmlessly (Rack has no error contract here).
            Some(_) => Ok(()),
            None => Err(interrupted_error(ruby)),
        }
    }

    /// Clean end of a streaming response.
    pub fn finish(_ruby: &Ruby, rb_self: &Request) -> Result<(), Error> {
        rb_self.0.borrow().responder.finish_stream();
        Ok(())
    }

    /// Give up on this request: canned 500 if the head wasn't sent, abort
    /// the connection if mid-stream. Used by the worker's rescue path.
    pub fn abort(_ruby: &Ruby, rb_self: &Request) -> Result<(), Error> {
        rb_self.0.borrow().responder.respond_500_if_unsent();
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

fn split_host_port(host: &str, default_port: u16) -> (String, u16) {
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
    let (_body_tx, body_rx) = flume::bounded(1);
    let (head_tx, _head_rx) = tokio::sync::oneshot::channel();
    Box::new(RequestCtx {
        method: http::Method::GET,
        uri: "/".parse().expect("static uri"),
        version: http::Version::HTTP_11,
        headers: http::HeaderMap::new(),
        remote_addr: "127.0.0.1:40000".parse().expect("static addr"),
        local_addr: "127.0.0.1:9292".parse().expect("static addr"),
        https: false,
        body_rx,
        body_overflow: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        body_timeout: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        leftover: None,
        slot: None,
        pin_slab: Arc::new(crate::pin::PinSlab::new()),
        responder: Arc::new(Responder::new(head_tx)),
    })
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
        // test_ctx drops its sender immediately: disconnected + empty = no
        // byte can ever arrive.
        let ctx = test_ctx();
        assert!(!body_possible(&ctx));

        // A leftover chunk means there is still body to read.
        let mut ctx = test_ctx();
        ctx.leftover = Some(bytes::Bytes::from_static(b"tail"));
        assert!(body_possible(&ctx));

        // A live sender means bytes may still arrive.
        let (body_tx, body_rx) = flume::bounded(1);
        let mut ctx = test_ctx();
        ctx.body_rx = body_rx;
        assert!(body_possible(&ctx));
        drop(body_tx);

        // Disconnected but with a queued chunk: still readable.
        let (body_tx, body_rx) = flume::bounded(1);
        body_tx.send(bytes::Bytes::from_static(b"x")).unwrap();
        drop(body_tx);
        let mut ctx = test_ctx();
        ctx.body_rx = body_rx;
        assert!(body_possible(&ctx));
    }
}
