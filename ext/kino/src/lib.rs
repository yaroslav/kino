// All Rust-side allocations (request/response buffers, hyper, tokio,
// channels) go through mimalloc: ~+10% plaintext throughput on Linux over
// glibc malloc, no downside measured elsewhere.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod access_log;
mod control;
mod counters;
mod cpus;
mod dispatch;
mod doorbell;
mod env_strings;
mod framing;
mod gvl;
mod hijack;
mod inline;
mod io_shards;
mod lane_room;
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
mod shared_gvl;
mod slot;
mod stats;
mod style;
mod symbols;
mod test_support;
#[cfg(test)]
mod testing;
mod ticker;
mod timer;
mod tls;
mod typed_data;
mod upgrade;
mod ws;

use magnus::{function, method, prelude::*, Error, Ruby};

use crate::request::Request;

/// Each `"name" => function, arity;` row as a singleton function of `$on`.
macro_rules! functions {
    ($on:ident: $($name:literal => $function:path, $arity:tt;)*) => {
        $($on.define_singleton_method($name, function!($function, $arity))?;)*
    };
}

/// Each `"name" => method, arity;` row as an instance method of `$on`.
macro_rules! methods {
    ($on:ident: $($name:literal => $method:path, $arity:tt;)*) => {
        $($on.define_method($name, method!($method, $arity))?;)*
    };
}

#[magnus::init]
fn init(ruby: &Ruby) -> Result<(), Error> {
    // Must come before any method definition: methods defined while this
    // flag is unset raise Ractor::UnsafeError when called from a non-main
    // ractor, and worker ractors are this gem's entire reason to exist.
    unsafe { rb_sys::rb_ext_ractor_safe(true) };

    let module = ruby.define_module("Kino")?;

    let native = module.define_module("Native")?;
    native.define_class("PinKeeper", ruby.class_object())?;
    // A worker's tick; the ticker rings a worker_loop's doorbell wait as often.
    native.const_set("TICK_SECONDS", queue::TICK.as_secs_f64())?;
    functions!(native:
        "server_start" => server::server_start, 1;
        "register_worker" => server::register_worker, 1;
        "worker" => queue::worker, 2;
        "wake_worker" => queue::wake_worker, 3;
        "lingering_loops" => queue::lingering_loops, 2;
        "stop_accepting" => server::stop_accepting, 1;
        "close_queue" => server::close_queue, 1;
        "close_bridges" => server::close_bridges, 1;
        "queue_stats" => server::queue_stats, 1;
        "stats" => server::stats, 1;
        "worker_stats" => server::worker_stats, 1;
        "quarantine_slot" => server::quarantine_slot, 3;
        "retire_slot" => server::retire_slot, 3;
        "reset_slot" => server::reset_slot, 2;
        "ws_leave_all" => server::ws_leave_all, 2;
        "set_active_workers" => server::set_active_workers, 2;
        "record_scale_up" => server::record_scale_up, 1;
        "record_scale_down" => server::record_scale_down, 1;
        "record_quarantine_replacement" => server::record_quarantine_replacement, 1;
        "control_ready" => server::control_ready, 1;
        "record_respawn" => server::record_respawn, 1;
        "control_stop" => control::control_stop, 1;
        "abort_inflight" => server::abort_inflight, 2;
        "abort_all_inflight" => server::abort_all_inflight, 1;
        "interrupt_all_workers" => server::interrupt_all_workers, 1;
        "shutdown_runtime" => server::shutdown_runtime, 2;
        "log_line" => log::log_line, 3;
        "sleep_chunk" => timer::sleep_chunk, 1;
        "available_parallelism" => cpus::available_parallelism, 0;
        "log_device_open" => logsink::device_open, 1;
        "log_device_write" => logsink::device_write, 2;
        "log_device_close" => logsink::device_close, 1;
        "register_defaults" => env_strings::register_defaults, 2;
        "pin_keeper" => server::pin_keeper, 1;
        "ws_write" => ws::ws_write, 2;
        "ws_write_binary" => ws::ws_write_binary, 2;
        "ws_write_raw" => ws::ws_write_raw, 2;
        "ws_close" => ws::ws_close, 2;
        "ws_abort" => ws::abort, 1;
        "ws_fail" => ws::fail, 1;
        "ws_handled" => ws::handled, 1;
        "ws_shutdown_ran" => ws::shutdown_ran, 1;
        "ws_pending" => ws::pending, 1;
        "ws_registered" => ws::registered, 1;
        "ws_subscribe" => ws::topics::ws_subscribe, 5;
        "ws_unsubscribe" => ws::topics::ws_unsubscribe, 3;
        "ws_publish" => ws::topics::ws_publish, 3;
    );

    let worker = native.define_class("Worker", ruby.class_object())?;
    methods!(worker:
        "take_one" => queue::Worker::take_one, 0;
        "take_batch" => queue::Worker::take_batch, 1;
        "identity" => queue::Worker::identity, 0;
        "release" => queue::Worker::release, 0;
        "crashed" => queue::Worker::crashed, 0;
        "drained" => queue::Worker::drained, 1;
        "ring" => queue::Worker::ring, 0;
        "ready_fd" => queue::Worker::ready_fd, 0;
        "woke" => queue::Worker::woke, 0;
        "capacity=" => queue::Worker::set_capacity, 1;
        "lanes?" => queue::Worker::lanes, 0;
        "claim_warning" => queue::Worker::claim_warning, 1;
    );
    // Variadic (an optional `only`), which the table's one-token arity cannot say.
    worker.define_method("take_nonblock", method!(queue::Worker::take_nonblock, -1))?;
    worker.define_method("take_woken", method!(queue::Worker::take_woken, -1))?;

    let conn = native.define_class("Conn", ruby.class_object())?;
    methods!(conn:
        "fd" => inline::ConnHandle::fd, 0;
        "next_request" => inline::ConnHandle::next_request, 1;
        "close" => inline::ConnHandle::close, 0;
    );

    let request = native.define_class("Request", ruby.class_object())?;
    methods!(request:
        "respond_and_take" => queue::respond_and_take, 5;
        "respond_and_take_one" => queue::respond_and_take_one, 4;
        "read_body" => Request::read_body, 1;
        "read_body_nonblock" => Request::read_body_nonblock, 1;
        "send_simple" => crate::request::respond_simple, 3;
        "send_headers" => Request::send_headers, 2;
        // Request#write_chunk (worker.rb) waits on what this hands back.
        "write_chunk_or_wait" => Request::write_chunk, 1;
        "write_chunk_nonblock" => Request::write_chunk_nonblock, 1;
        "waited" => Request::waited, 0;
        "waited_writable" => Request::waited_writable, 0;
        "close_waits" => Request::close_waits, 0;
        "finish" => Request::finish, 0;
        "abort" => Request::abort, 0;
        "timing" => Request::set_timing, 2;
        "upgrade_websocket" => Request::upgrade_websocket, 2;
        "switch_protocols" => Request::switch_protocols, 0;
        "hijack" => Request::hijack, 0;
        "hijack_switched" => Request::hijack_switched, 1;
        "hijacked?" => Request::hijacked, 0;
    );

    typed_data::resolve_classes(ruby);

    // Frozen env key/value cache: built once here (main ractor, GVL held),
    // shared by every worker ractor afterwards.
    env_strings::init(ruby);
    // So are the Symbols natives tell apart or hand Ruby.
    symbols::init(ruby);

    functions!(native:
        "_test_channel_create" => test_support::create, 1;
        "_test_push" => test_support::push, 2;
        "_test_take" => test_support::take, 1;
        "_test_close" => test_support::close, 1;
        "_test_panic" => test_support::panic_in_release, 0;
        "_test_env_probe" => test_support::env_probe, 4;
    );

    Ok(())
}
