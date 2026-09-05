// All Rust-side allocations (request/response buffers, hyper, tokio,
// channels) go through mimalloc: ~+10% plaintext throughput on Linux over
// glibc malloc, no downside measured elsewhere.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod access_log;
mod control;
mod cpus;
mod env_strings;
mod gvl;
mod io_shards;
mod listen;
mod log;
mod logsink;
mod mono;
mod pin;
mod queue;
mod registry;
mod request;
mod response;
mod server;
mod style;
mod test_support;
mod timer;
mod tls;

use magnus::{function, method, prelude::*, Error, Ruby};

use crate::request::Request;

#[magnus::init]
fn init(ruby: &Ruby) -> Result<(), Error> {
    // Must come before any method definition: methods defined while this
    // flag is unset raise Ractor::UnsafeError when called from a non-main
    // ractor, and worker ractors are this gem's entire reason to exist.
    unsafe { rb_sys::rb_ext_ractor_safe(true) };

    let module = ruby.define_module("Kino")?;

    let native = module.define_module("Native")?;
    native.define_singleton_method("server_start", function!(server::server_start, 1))?;
    native.define_singleton_method("register_worker", function!(server::register_worker, 1))?;
    native.define_singleton_method("worker", function!(queue::worker, 2))?;
    native.define_singleton_method("stop_accepting", function!(server::stop_accepting, 1))?;
    native.define_singleton_method("close_queue", function!(server::close_queue, 1))?;
    native.define_singleton_method("queue_stats", function!(server::queue_stats, 1))?;
    native.define_singleton_method("server_stats", function!(server::server_stats, 1))?;
    native.define_singleton_method("worker_stats", function!(server::worker_stats, 1))?;
    native.define_singleton_method("queue_time", function!(server::queue_time, 1))?;
    native.define_singleton_method("quarantine_slot", function!(server::quarantine_slot, 2))?;
    native.define_singleton_method(
        "record_quarantine_replacement",
        function!(server::record_quarantine_replacement, 1),
    )?;
    native.define_singleton_method("control_ready", function!(server::control_ready, 1))?;
    native.define_singleton_method("record_respawn", function!(server::record_respawn, 1))?;
    native.define_singleton_method("control_stop", function!(control::control_stop, 1))?;
    native.define_singleton_method("abort_inflight", function!(server::abort_inflight, 2))?;
    native.define_singleton_method(
        "abort_all_inflight",
        function!(server::abort_all_inflight, 1),
    )?;
    native.define_singleton_method(
        "interrupt_all_workers",
        function!(server::interrupt_all_workers, 1),
    )?;
    native.define_singleton_method("shutdown_runtime", function!(server::shutdown_runtime, 2))?;
    native.define_singleton_method("log_line", function!(log::log_line, 3))?;
    native.define_singleton_method("sleep_chunk", function!(timer::sleep_chunk, 1))?;
    native.define_singleton_method(
        "available_parallelism",
        function!(cpus::available_parallelism, 0),
    )?;
    native.define_singleton_method("log_device_open", function!(logsink::device_open, 1))?;
    native.define_singleton_method("log_device_write", function!(logsink::device_write, 2))?;
    native.define_singleton_method("log_device_close", function!(logsink::device_close, 1))?;
    native.define_singleton_method(
        "register_defaults",
        function!(env_strings::register_defaults, 2),
    )?;

    native.define_class("PinKeeper", ruby.class_object())?;
    native.define_singleton_method("pin_keeper", function!(server::pin_keeper, 1))?;

    let worker = native.define_class("Worker", ruby.class_object())?;
    worker.define_method("take_one", method!(queue::Worker::take_one, 0))?;
    worker.define_method("take_batch", method!(queue::Worker::take_batch, 1))?;

    let request = native.define_class("Request", ruby.class_object())?;
    request.define_method("respond_and_take", method!(queue::respond_and_take, 5))?;
    request.define_method(
        "respond_and_take_one",
        method!(queue::respond_and_take_one, 4),
    )?;
    request.define_method("read_body", method!(Request::read_body, 1))?;
    request.define_method("send_simple", method!(crate::request::respond_simple, 3))?;
    request.define_method("send_headers", method!(Request::send_headers, 2))?;
    request.define_method("write_chunk", method!(Request::write_chunk, 1))?;
    request.define_method("finish", method!(Request::finish, 0))?;
    request.define_method("abort", method!(Request::abort, 0))?;
    request.define_method("timing", method!(Request::set_timing, 2))?;

    // Force-resolve the TypedData class caches on the main ractor: magnus
    // resolves them lazily on first wrap, and a racy first resolution from
    // two worker ractors is the failure mode we must rule out.
    let _ = <Request as magnus::TypedData>::class(ruby);
    let _ = <queue::Worker as magnus::TypedData>::class(ruby);
    let _ = <pin::PinKeeper as magnus::TypedData>::class(ruby);

    // Frozen env key/value cache: built once here (main ractor, GVL held),
    // shared by every worker ractor afterwards.
    env_strings::init(ruby);

    native.define_singleton_method("_test_channel_create", function!(test_support::create, 1))?;
    native.define_singleton_method("_test_push", function!(test_support::push, 2))?;
    native.define_singleton_method("_test_take", function!(test_support::take, 1))?;
    native.define_singleton_method("_test_close", function!(test_support::close, 1))?;
    native.define_singleton_method("_test_panic", function!(test_support::panic_in_release, 0))?;
    native.define_singleton_method("_test_env_probe", function!(test_support::env_probe, 4))?;

    Ok(())
}
