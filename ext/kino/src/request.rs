//! `Kino::Native::Request`: the opaque per-request handle. Created INSIDE
//! the worker ractor by the take calls (queue/worker.rs), so the wrapping Ruby
//! object is owned by the ractor that will use it, and never crosses
//! ractor boundaries.

use std::cell::{Cell, RefCell, RefMut};
use std::os::fd::RawFd;
use std::sync::Arc;

use bytes::Bytes;
use magnus::r_hash::ForEach;
use magnus::value::ReprValue;
use magnus::{Error, RArray, RHash, RString, Ruby, Value};

use crate::doorbell::Doorbell;
use crate::env_strings::{self, EnvStrings};
use crate::framing::Bodiless;
use crate::gvl;
use crate::pin::PinSlab;
use crate::registry::ServerInner;
use crate::response::{full_body, BodyFrame, Cut, FrameResult, Responder};
use crate::server::Peer;
use crate::slot::WorkerSlot;

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
    /// The owning worker slot (set at admit time, queue/worker.rs); its halted flag ends
    /// blocked body reads/writes at the shutdown, as it ends a park.
    pub slot: Option<Arc<WorkerSlot>>,
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
    pub waits: Waits,
    /// Served inline: its connection, held as long as the request is, so a write's wait on its
    /// socket outlives its job (InlineIo::waited); its responder holds it weakly.
    pub inline: Option<Arc<crate::inline::InlineIo>>,
}

/// Whether a wait opened the bells, how many waits are out (Request#waited), and whether the
/// bells close once none is. Cells, so each native takes the handle's shared borrow: a blocking
/// write_chunk holds one across its wait while another thread closes the stream.
#[derive(Default)]
pub struct Waits {
    opened: Cell<bool>,
    out: Cell<u32>,
    closing: Cell<bool>,
}

/// One step under a fiber scheduler: done, or the descriptor to wait on before the next.
#[derive(Debug, PartialEq, Eq)]
pub enum Step<T> {
    Done(T),
    Wait(RawFd),
}

#[derive(Debug)]
pub enum WaitError {
    /// The response stream was not started, or already finished.
    Closed,
    /// Nothing written reaches anyone (client gone, stream given up): Kino::ClientGone.
    Gone,
    Interrupted,
    /// No descriptor to wait on (out of file descriptors).
    Io(std::io::Error),
}

impl Waits {
    /// `attempt`, else `bell` armed and `attempt` once more, so a change just before the arm
    /// still rings. The bell stays armed after a find, for other fibers on it: a stray ring costs
    /// a look. Past the shutdown deadline (a halted slot) a wait fails as interrupted.
    fn step<T>(
        &self,
        slot: Option<&WorkerSlot>,
        bell: &Doorbell,
        mut attempt: impl FnMut() -> Option<T>,
    ) -> Result<Step<T>, WaitError> {
        if let Some(done) = attempt() {
            return Ok(Step::Done(done));
        }
        let fd = bell.open().map_err(WaitError::Io)?;
        self.opened.set(true);
        bell.arm();
        if let Some(done) = attempt() {
            return Ok(Step::Done(done));
        }
        // After the arm, as the shutdown rings after raising the flag.
        if slot.is_some_and(|slot| slot.life.is_halted()) {
            return Err(WaitError::Interrupted);
        }
        self.out.set(self.out.get() + 1);
        Ok(Step::Wait(fd))
    }
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
        let mut ctx = RequestCtx::queued(
            server,
            parts.method,
            parts.uri,
            parts.version,
            parts.headers,
            peer,
            responder,
        );
        ctx.body_rx = body_rx;
        ctx.upgrade = upgrade;
        Box::new(ctx)
    }

    /// Queued now, with no body, worker or switch yet: the caller sets what its request has.
    #[inline]
    pub fn queued(
        server: &ServerInner,
        method: http::Method,
        uri: http::Uri,
        version: http::Version,
        headers: http::HeaderMap,
        peer: Peer,
        responder: Arc<Responder>,
    ) -> Self {
        let now = std::time::Instant::now();
        RequestCtx {
            method,
            uri,
            version,
            headers,
            peer,
            https: server.config.tls.is_some(),
            body_rx: None,
            leftover: Cell::new(None),
            slot: None,
            pin_slab: server.pin_slab.clone(),
            responder,
            enqueued_at: now,
            timed: server.access_log.is_some(),
            wait: std::time::Duration::ZERO,
            admitted_at: now,
            gc: Cell::new(None),
            upgrade: None,
            hijacked: false,
            server_id: server.id,
            waits: Waits::default(),
            inline: None,
        }
    }

    /// Done(None) at the body's end (or abandonment, see body_ended), when its waits close too.
    pub fn read_nonblock(&self) -> Result<Step<Option<Bytes>>, WaitError> {
        let Some(body_rx) = self.body_rx.as_ref() else {
            return Ok(Step::Done(None));
        };
        let step =
            self.waits.step(
                self.slot.as_deref(),
                &self.responder.body_bell,
                || match body_rx.try_recv() {
                    Ok(bytes) => Some(Some(bytes)),
                    Err(flume::TryRecvError::Empty) => None,
                    Err(flume::TryRecvError::Disconnected) => Some(None),
                },
            )?;
        if step == Step::Done(None) {
            self.close_waits();
        }
        Ok(step)
    }

    /// One data frame without blocking; `make` makes its bytes once there is room. Gone once the
    /// client is gone or the stream was cut, as write_chunk raises, also for a write that waited.
    pub fn write_nonblock(
        &self,
        make: impl FnOnce(&Arc<PinSlab>) -> Bytes,
    ) -> Result<Step<()>, WaitError> {
        let Some(sender) = self.responder.body_sender() else {
            if self.responder.stream_cut() {
                return Err(WaitError::Gone);
            }
            return Err(WaitError::Closed);
        };
        let pin_slab = &self.pin_slab;
        let mut make = Some(make);
        let mut frame: Option<FrameResult> = None;
        let step = self
            .waits
            .step(self.slot.as_deref(), &self.responder.room_bell, || {
                if sender.is_disconnected() {
                    return Some(false);
                }
                if sender.is_full() {
                    // As a blocking write's wait: a stream given up while
                    // its client stalls never drains.
                    return self.responder.stream_cut().then_some(false);
                }
                let next = frame.take().unwrap_or_else(|| {
                    let make = make.take().expect("bytes made once");
                    Ok(BodyFrame::data(make(pin_slab)))
                });
                match sender.try_send(next) {
                    Ok(()) => Some(true),
                    Err(flume::TrySendError::Disconnected(_)) => Some(false),
                    Err(flume::TrySendError::Full(back)) => {
                        frame = Some(back);
                        None
                    }
                }
            })?;
        match step {
            Step::Done(true) => Ok(Step::Done(())),
            Step::Done(false) => Err(WaitError::Gone),
            Step::Wait(fd) => Ok(Step::Wait(fd)),
        }
    }

    pub fn waited(&self) {
        self.waits.out.set(self.waits.out.get().saturating_sub(1));
        if self.waits.closing.get() {
            self.close_waits();
        }
    }

    /// Now if no wait is out, else once the last is back, so no fiber waits on a closed descriptor.
    pub fn close_waits(&self) {
        if !self.waits.opened.get() {
            return;
        }
        if self.waits.out.get() > 0 {
            self.waits.closing.set(true);
            return;
        }
        self.waits.opened.set(false);
        self.waits.closing.set(false);
        self.responder.close_bells();
    }

    /// Its waits close, and the worker that took it hears (WorkerSlot::answered).
    fn answered(&self) {
        self.close_waits();
        if let Some(slot) = &self.slot {
            slot.answered(&self.responder, self.inline.is_some());
        }
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

/// Block on a channel operation with the GVL released, as the worker parks (gvl::park):
/// `attempt` runs one bounded tick, Some when done; Ok(None) once the slot is halted. A raise
/// comes back as Err, a chunk read or written as it came staying the app's. Every Ruby handle
/// is created by `admit` (queue/worker.rs), which always sets the slot.
fn block_on<T>(
    slot: &Option<Arc<WorkerSlot>>,
    attempt: impl FnMut() -> Option<T>,
) -> Result<Option<T>, Error> {
    let slot = slot.as_ref().expect("slot set by admit");
    gvl::park(
        &mut gvl::Released,
        slot.life.halted(),
        attempt,
        gvl::Back::HandedOver,
    )
}

fn runtime_error(ruby: &Ruby, message: impl Into<std::borrow::Cow<'static, str>>) -> Error {
    Error::new(ruby.exception_runtime_error(), message)
}

const INTERRUPTED: &str = "Kino: request interrupted during shutdown";

fn interrupted_error(ruby: &Ruby) -> Error {
    runtime_error(ruby, INTERRUPTED)
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
    runtime_error(ruby, format!("Kino: {BUSY}"))
}

/// ClientGone once Kino gave the stream up, else it was never started or is already finished.
fn stream_closed_error(ruby: &Ruby, responder: &Responder) -> Error {
    if responder.stream_cut() {
        return client_gone_error(ruby);
    }
    runtime_error(
        ruby,
        "Kino: response stream not started or already finished",
    )
}

fn wait_error(ruby: &Ruby, responder: &Responder, error: WaitError) -> Error {
    match error {
        WaitError::Closed => stream_closed_error(ruby, responder),
        WaitError::Gone => {
            // Counted once, unless Kino cut it first (counted then).
            responder.cut_stream(crate::response::Cut::ClientGone);
            client_gone_error(ruby)
        }
        WaitError::Interrupted => interrupted_error(ruby),
        WaitError::Io(e) => Error::new(
            ruby.exception_io_error(),
            format!("Kino: no descriptor to wait on the client with: {e}"),
        ),
    }
}

fn invalid_response(ruby: &Ruby, e: impl std::fmt::Display) -> Error {
    runtime_error(ruby, format!("invalid response: {e}"))
}

/// Kino::Native::Request, wrapped in typed_data.rs, whose promise its drop keeps: RequestCtx's
/// gives the request up through atomics, Kino's own locks, channels and a socket write.
#[derive(magnus::DataTypeFunctions)]
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
    let s = env_strings::get();
    // Pre-sized: a dozen static keys plus one per header lands near 30
    // entries, which a default Hash reaches only via the ar-to-st
    // migration and a rehash per doubling.
    let env = ruby.hash_new_capa(32);
    set_request_env(ruby, env, s, ctx)?;
    set_headers_env(ruby, env, s, ctx)?;
    // After the headers, so an authority-derived HTTP_HOST wins over a literal host header
    // a proxy forwarded.
    set_server_env(ruby, env, ctx)?;
    Ok(env)
}

/// The request line, the peer, the scheme, and the rack.* streams.
#[inline(always)]
fn set_request_env(ruby: &Ruby, env: RHash, s: &EnvStrings, ctx: &RequestCtx) -> Result<(), Error> {
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
    Ok(())
}

/// One HTTP_* entry per header name, its values joined.
#[inline(always)]
fn set_headers_env(ruby: &Ruby, env: RHash, s: &EnvStrings, ctx: &RequestCtx) -> Result<(), Error> {
    let live = |o: magnus::value::Opaque<RString>| ruby.get_inner(o);
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
    Ok(())
}

/// SERVER_NAME/SERVER_PORT/HTTP_HOST, best source first: the request
/// URI's authority (every h2 request via :authority; also h1
/// absolute-form, where it beats Host per RFC 9112), then the Host
/// header (HTTP/1.1 requires it), then the accepted socket's local
/// address.
#[inline(always)]
fn set_server_env(ruby: &Ruby, env: RHash, ctx: &RequestCtx) -> Result<(), Error> {
    let local_port = ctx.peer.local.port();
    if let Some(authority) = ctx.uri.authority() {
        let port = authority.port_u16();
        let key = host_key(authority.as_str().as_bytes(), port.is_some(), local_port);
        return env_strings::set_authority_env(ruby, env, &key, authority.as_str(), || {
            (authority.host().to_string(), port.unwrap_or(local_port))
        });
    }
    match ctx.headers.get(http::header::HOST) {
        Some(host) => {
            let (bytes, text) = (host.as_bytes(), std::str::from_utf8(host.as_bytes()));
            let names_port = text.is_ok_and(|text| named_port(text).is_some());
            env_strings::set_host_env(ruby, env, &host_key(bytes, names_port, local_port), || {
                match text {
                    Ok(text) => split_host_port(text, local_port),
                    Err(_) => (String::from_utf8_lossy(bytes).into_owned(), local_port),
                }
            })
        }
        None => {
            let ip = ctx.peer.local.ip();
            let key = format!("\0{ip}:{local_port}");
            env_strings::set_host_env(ruby, env, key.as_bytes(), || (ip.to_string(), local_port))
        }
    }
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

/// The body channel disconnected: a clean EOF, unless the forwarder
/// abandoned the body (too large, or the client stalled).
fn body_ended(ruby: &Ruby, ctx: &RequestCtx) -> Result<(), Error> {
    use crate::response::BodyAbandon;
    let why = match ctx.responder.body_abandoned() {
        None => return Ok(()),
        Some(BodyAbandon::Oversize) => "Kino: request body exceeded max_body_size",
        Some(BodyAbandon::TimedOut) => "Kino: request body read timed out",
        Some(BodyAbandon::Interrupted) => INTERRUPTED,
    };
    Err(runtime_error(ruby, why))
}

/// `chunk`, up to `max_len` bytes (the rest waits in leftover), topped up with what arrived.
fn body_chunk(ruby: &Ruby, ctx: &RequestCtx, mut chunk: Bytes, max_len: usize) -> RString {
    let max_len = max_len.max(1);
    if chunk.len() > max_len {
        ctx.leftover.set(Some(chunk.split_off(max_len)));
        return ruby.str_from_slice(&chunk);
    }

    // Greedy drain: what the forwarder already queued rides out in this crossing, up to max_len,
    // so a body of small DATA frames (HTTP/2's 16 KB default) costs one GVL round-trip and one
    // Ruby string, not one per frame. What has not come yet, EOF included, is the next read's.
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
    assembled.unwrap_or_else(|| ruby.str_from_slice(&chunk))
}

/// Complete response in one shot: status, the Rack headers Hash (iterated
/// here, no intermediate array on the Ruby side), full body. Free function
/// shared by the `send_simple` binding and the fused respond_and_take path
/// (queue/worker.rs).
/// An inline request's answer is written on its connection here and now.
pub fn respond_simple(
    ruby: &Ruby,
    request: &Request,
    status: u16,
    headers: RHash,
    body: RString,
) -> Result<bool, Error> {
    request.decline_upgrade();
    let ctx = request.0.borrow();
    if let Some(io) = &ctx.inline {
        let head = head_parts(ruby, status, headers)?;
        // No copy: the GVL is held until the write returns, which copies what the socket leaves.
        // SAFETY: `body` is an RString on this frame's stack (the GC neither frees nor moves it),
        // and no Ruby runs nor is the GVL released before the slice's last use, in respond.
        let body = unsafe { body.as_slice() };
        let sent = io.respond(&ctx.responder, head.status, &head.headers, body);
        ctx.answered();
        return Ok(sent);
    }
    let mut builder = timed(&ctx, build_head(status, headers)?);
    let bytes = if frame_by_length(&mut builder, status, &ctx.method, body.len()) {
        body_bytes(&ctx.pin_slab, body)
    } else {
        Bytes::new()
    };
    let response = builder
        .body(full_body(bytes))
        .map_err(|e| invalid_response(ruby, e))?;
    let sent = ctx.responder.send_response(response);
    ctx.answered();
    Ok(sent)
}

/// A buffered answer is framed by its body, as the inline writer frames it: hyper trusts a
/// Content-Length it is given, so the app's is dropped (a wrong one would misframe what follows),
/// as is its Transfer-Encoding (beside a length, hyper ends the connection unanswered). A HEAD
/// sends no body (over h2 it would go as DATA) and keeps the app's first Content-Length, else its
/// body's (`len`) unless 304; a 304 to another method, a 1xx and a 204 send none either. True
/// when the body goes out.
pub(crate) fn frame_by_length(
    builder: &mut hyper::http::response::Builder,
    status: u16,
    method: &http::Method,
    len: usize,
) -> bool {
    let bodiless = Bodiless::of(status, *method == http::Method::HEAD);
    if let Some(headers) = builder.headers_mut() {
        headers.remove(http::header::TRANSFER_ENCODING);
        match bodiless {
            None => drop(headers.remove(http::header::CONTENT_LENGTH)),
            Some(bodiless) => bodiless.settle(headers, len),
        }
    }
    bodiless.is_none()
}

/// Body bytes for hyper: large bodies are pinned in place (zero-copy),
/// everything else is copied out of the Ruby string, which may be
/// mutated or collected the moment we return.
fn body_bytes(pin_slab: &Arc<PinSlab>, body: RString) -> Bytes {
    if let Some(bytes) = crate::pin::pinned_bytes(pin_slab, body) {
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
        let head = head_parts(ruby, 101, headers)?;
        let extra: Vec<_> = head
            .headers
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
            Some(mut ctx) => {
                let taken = crate::hijack::take(&mut ctx, head);
                ctx.close_waits();
                taken
            }
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
        // Shared across the wait: other threads may abort or answer the request meanwhile.
        let ctx = rb_self.0.borrow();
        let chunk = match ctx.leftover.take() {
            Some(bytes) => bytes,
            None => {
                let Some(body_rx) = ctx.body_rx.clone() else {
                    return Ok(None); // bodyless request: EOF from the start
                };
                match block_on(&ctx.slot, || crate::queue::recv_tick(&body_rx))? {
                    Some(Some(bytes)) => bytes,
                    Some(None) => return body_ended(ruby, &ctx).map(|()| None),
                    None => return Err(interrupted_error(ruby)),
                }
            }
        };
        Ok(Some(body_chunk(ruby, &ctx, chunk, max_len)))
    }

    /// read_body for a fiber scheduler: while no chunk has come, the descriptor to wait on.
    pub fn read_body_nonblock(
        ruby: &Ruby,
        rb_self: &Request,
        max_len: usize,
    ) -> Result<Value, Error> {
        let ctx = rb_self.0.borrow();
        let chunk = match ctx.leftover.take() {
            Some(bytes) => bytes,
            None => match ctx
                .read_nonblock()
                .map_err(|e| wait_error(ruby, &ctx.responder, e))?
            {
                Step::Done(Some(bytes)) => bytes,
                Step::Done(None) => {
                    body_ended(ruby, &ctx)?;
                    return Ok(ruby.qnil().as_value());
                }
                Step::Wait(fd) => return Ok(ruby.into_value(fd)),
            },
        };
        Ok(body_chunk(ruby, &ctx, chunk, max_len).as_value())
    }

    /// Start a streaming response: head goes out now, chunks follow via
    /// write_chunk, terminated by finish. Once Kino answered, the first write ends the body.
    /// An inline request's head is written on its connection (InlineIo::start_stream).
    pub fn send_headers(
        ruby: &Ruby,
        rb_self: &Request,
        status: u16,
        headers: RHash,
    ) -> Result<bool, Error> {
        rb_self.decline_upgrade();
        let ctx = rb_self.0.borrow();
        if let Some(io) = &ctx.inline {
            let head = head_parts(ruby, status, headers)?;
            return Ok(io.start_stream(&ctx.responder, head.status, &head.headers));
        }
        let builder = timed(&ctx, build_head(status, headers)?);
        let sent = match Bodiless::of(status, ctx.method == http::Method::HEAD) {
            Some(bodiless) => ctx.responder.send_bodiless_head(builder, bodiless),
            None => ctx.responder.send_stream_head(builder),
        };
        sent.map_err(|e| invalid_response(ruby, e))
    }

    /// One body chunk. Blocks (GVL released) when the client is slower than
    /// client is gone or Kino gave the stream up.
    /// False once the chunk is on its way. An inline write waits a TICK at most for its socket,
    /// then returns true to be made again, so a pending Thread#raise reaches the thread in Ruby.
    pub fn write_chunk(ruby: &Ruby, rb_self: &Request, chunk: RString) -> Result<bool, Error> {
        let ctx = rb_self.0.borrow();
        if let Some(io) = &ctx.inline {
            let Step::Wait(fd) = write_inline(ruby, &ctx, io, chunk)? else {
                return Ok(false);
            };
            let unread = std::sync::atomic::AtomicBool::new(false);
            let wait = || crate::inline::await_writable(fd, crate::queue::TICK);
            let waited = gvl::Parker::release(&mut gvl::Released, &unread, wait);
            io.waited();
            waited?;
            return Ok(true);
        }
        let Some(sender) = ctx.responder.body_sender() else {
            return Err(stream_closed_error(ruby, &ctx.responder));
        };
        let bytes = body_bytes(&ctx.pin_slab, chunk);
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
            Some(true) => Ok(false),
            // Gone, or given up (and counted then).
            Some(false) => {
                ctx.responder.cut_stream(crate::response::Cut::ClientGone);
                Err(client_gone_error(ruby))
            }
            None => Err(interrupted_error(ruby)),
        }
    }

    /// write_chunk for a fiber scheduler: true once on its way, else the descriptor to wait on.
    /// An inline request's is [:writable, fd].
    pub fn write_chunk_nonblock(
        ruby: &Ruby,
        rb_self: &Request,
        chunk: RString,
    ) -> Result<Value, Error> {
        let ctx = rb_self.0.borrow();
        if let Some(io) = &ctx.inline {
            return Ok(match write_inline(ruby, &ctx, io, chunk)? {
                Step::Done(()) => ruby.qtrue().as_value(),
                Step::Wait(fd) => {
                    let writable = crate::symbols::get().writable.value();
                    let wait = ruby.ary_new_from_values(&[writable, ruby.into_value(fd)]);
                    wait.as_value()
                }
            });
        }
        let step = ctx.write_nonblock(|pin_slab| body_bytes(pin_slab, chunk));
        Ok(
            match step.map_err(|e| wait_error(ruby, &ctx.responder, e))? {
                Step::Done(()) => ruby.qtrue().as_value(),
                Step::Wait(fd) => ruby.into_value(fd),
            },
        )
    }

    /// A wait on a descriptor read_body_nonblock or write_chunk_nonblock handed out is over.
    pub fn waited(_ruby: &Ruby, rb_self: &Request) {
        rb_self.0.borrow().waited();
    }

    /// The [:writable, fd] wait write_chunk_nonblock handed out is over: the descriptor may
    /// close.
    pub fn waited_writable(_ruby: &Ruby, rb_self: &Request) {
        if let Some(io) = &rb_self.0.borrow().inline {
            io.waited();
        }
    }

    /// The request's job is over: its waits' descriptors close (RequestCtx::close_waits).
    pub fn close_waits(_ruby: &Ruby, rb_self: &Request) {
        rb_self.0.borrow().close_waits();
    }

    /// Clean end of a streaming response.
    pub fn finish(_ruby: &Ruby, rb_self: &Request) -> Result<(), Error> {
        let ctx = rb_self.0.borrow();
        ctx.responder.finish_stream();
        ctx.answered();
        Ok(())
    }

    /// Give up on this request: canned 500 if the head wasn't sent, abort
    /// the connection if mid-stream. Used by the worker's rescue path. It
    /// borrows no more than threads waiting on the request do, so a waiting write fails.
    pub fn abort(_ruby: &Ruby, rb_self: &Request) -> Result<(), Error> {
        rb_self.decline_upgrade();
        if let Ok(ctx) = rb_self.0.try_borrow() {
            ctx.responder.give_up(Cut::Crash);
            ctx.answered();
        }
        Ok(())
    }
}

/// A head's status and headers alone: what an inline answer is encoded from (inline/encode.rs),
/// and what a handshake's 101 adds to Kino's own.
#[inline]
fn head_parts(ruby: &Ruby, status: u16, headers: RHash) -> Result<http::response::Parts, Error> {
    let response = build_head(status, headers)?.body(());
    Ok(response
        .map_err(|e| invalid_response(ruby, e))?
        .into_parts()
        .0)
}

/// One chunk of an inline request's stream, written now with the GVL held, or the socket to wait
/// on first (InlineIo::write_chunk); ClientGone once nothing reaches the client.
fn write_inline(
    ruby: &Ruby,
    ctx: &RequestCtx,
    io: &crate::inline::InlineIo,
    chunk: RString,
) -> Result<Step<()>, Error> {
    use crate::inline::Wrote;

    // SAFETY: `chunk` is an RString on this frame's stack (the GC neither frees nor moves it),
    // and no Ruby runs nor is the GVL released before write_chunk, which copies what the socket
    // does not take.
    let data = unsafe { chunk.as_slice() };
    match io.write_chunk(&ctx.responder, data) {
        Wrote::Done => Ok(Step::Done(())),
        Wrote::Wait(fd) => Ok(Step::Wait(fd)),
        Wrote::Gone => Err(wait_error(ruby, &ctx.responder, WaitError::Gone)),
        Wrote::Closed => Err(stream_closed_error(ruby, &ctx.responder)),
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
    match named_port(host) {
        Some((name, port)) => (name.to_string(), port),
        None => (host.to_string(), default_port),
    }
}

/// `host`'s name and port, when it ends in a colon and a port that parses.
pub(crate) fn named_port(host: &str) -> Option<(&str, u16)> {
    match host.rsplit_once(':') {
        Some((name, port)) if !name.is_empty() => port.parse().ok().map(|port| (name, port)),
        _ => None,
    }
}

/// The host cache's key for `host` (a Host header or an authority) on a connection to
/// `local_port`: the bytes alone when they name a port, else the bytes, a NUL and that port, as
/// it becomes SERVER_PORT and a process may serve a host on several ports. Up to 256 bytes stay
/// on the stack.
pub(crate) fn host_key(
    host: &[u8],
    names_port: bool,
    local_port: u16,
) -> smallvec::SmallVec<[u8; 256]> {
    let mut key = smallvec::SmallVec::from_slice(host);
    if !names_port {
        key.push(0);
        key.extend_from_slice(&local_port.to_be_bytes());
    }
    key
}

/// A minimal ctx for pure-Rust tests (no Ruby involved). Dropping it
/// fires the Drop-500 backstop into a dropped receiver, which is a no-op.
#[cfg(test)]
pub fn test_ctx() -> crate::registry::BoxedCtx {
    test_ctx_with_client().0
}

/// A test_ctx whose client still waits for its answer, as long as the
/// caller keeps the returned receiver of its response head.
#[cfg(test)]
pub fn test_ctx_with_client() -> (
    crate::registry::BoxedCtx,
    tokio::sync::oneshot::Receiver<crate::response::HyperResponse>,
) {
    let (head_tx, head_rx) = tokio::sync::oneshot::channel();
    let (parts, ()) = http::Request::new(()).into_parts();
    let server = crate::testing::TestServer::shared_queue().build();
    let responder = Arc::new(Responder::new(head_tx));
    let ctx = RequestCtx::new(parts, crate::testing::PEER, &server, responder, None, None);
    (ctx, head_rx)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_host_splits_into_its_name_and_port_else_stays_whole_with_the_default_port() {
        let rows = [
            ("example.com:8080", "example.com", 8080),
            ("example.com", "example.com", 80),
            // Bracketed IPv6 (the RFC-correct Host form) keeps the brackets.
            ("[::1]:8080", "[::1]", 8080),
            // An out-of-range or missing port, or an empty name: whole.
            ("example.com:99999", "example.com:99999", 80),
            ("example.com:", "example.com:", 80),
            (":8080", ":8080", 80),
        ];
        for (host, name, port) in rows {
            let split = split_host_port(host, 80);
            assert_eq!(split, (name.to_string(), port), "{host}");
        }
    }

    #[test]
    fn a_chunk_that_arrives_as_the_read_arms_is_read_not_waited_for() {
        let (body_tx, body_rx) = flume::bounded(1);
        let mut ctx = test_ctx();
        ctx.body_rx = Some(body_rx);
        let responder = ctx.responder.clone();

        // As the forwarder sends: the chunk, then a ring, which finds the
        // bell not armed yet.
        ctx.responder.body_bell.before_arm(move || {
            body_tx.send(Bytes::from_static(b"late")).expect("open");
            responder.body_bell.ring();
        });
        assert_eq!(
            ctx.read_nonblock().expect("a read"),
            Step::Done(Some(Bytes::from_static(b"late")))
        );
    }

    #[test]
    fn a_wait_fails_as_interrupted_once_its_slot_is_halted_though_what_came_is_still_read() {
        let (body_tx, body_rx) = flume::bounded::<Bytes>(1);
        let slot = Arc::new(WorkerSlot::new(true, 1, None, false));
        let mut ctx = test_ctx();
        ctx.body_rx = Some(body_rx);
        ctx.slot = Some(slot.clone());

        slot.halt();
        assert!(matches!(ctx.read_nonblock(), Err(WaitError::Interrupted)));
        body_tx.send(Bytes::from_static(b"sent")).expect("open");
        assert!(
            matches!(ctx.read_nonblock(), Ok(Step::Done(Some(_)))),
            "what already came is read"
        );
    }

    #[test]
    fn closing_waits_holds_their_descriptor_until_the_last_wait_is_back() {
        let (body_tx, body_rx) = flume::bounded::<Bytes>(1);
        let mut ctx = test_ctx();
        ctx.body_rx = Some(body_rx);
        let first = ctx.read_nonblock().expect("a read");
        let second = ctx.read_nonblock().expect("a read");
        let Step::Wait(fd) = first else {
            panic!("expected to wait, got {first:?}");
        };
        assert_eq!(
            second,
            Step::Wait(fd),
            "waits share the request's descriptor"
        );

        ctx.close_waits();
        ctx.waited();
        assert_eq!(
            ctx.responder.body_bell.sockets().map(|s| s.fd()),
            Some(fd),
            "a wait is still out"
        );
        ctx.waited();
        assert!(ctx.responder.body_bell.sockets().is_none());

        assert!(
            matches!(ctx.read_nonblock(), Ok(Step::Wait(_))),
            "a later wait opens it again"
        );
        ctx.waited();
        ctx.close_waits();
        assert!(ctx.responder.body_bell.sockets().is_none());
        drop(body_tx);
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
