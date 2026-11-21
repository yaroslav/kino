//! The tokio + hyper front-end: listener, runtime and intake. Ruby never runs on these
//! threads; it meets them at the queue (in) and at each request's Responder (out).

mod accept;
mod config;
mod handle;
mod intake;
mod natives;
mod serve;
#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::time::Duration;

use magnus::{Error, Ruby};

use crate::listen::Listener;
use crate::registry::{self, RuntimeHandle, ServerInner};

use accept::accept_loop;
pub(crate) use accept::{next_accept, Accepted, AsyncListener, Conn, Peer};
pub use config::{ServerConfig, Topology};
pub(crate) use intake::recv_now;
pub use natives::*;
pub(crate) use serve::serve_conn;
#[cfg(test)]
pub(crate) use serve::testing;

fn io_error(ruby: &Ruby, what: &str, e: std::io::Error) -> Error {
    Error::new(ruby.exception_runtime_error(), format!("{what}: {e}"))
}

/// Bind and spawn the accept loop, synchronously so bind and TLS errors raise at `start`.
/// Returns the id, the bound port (for `port: 0`) and the control port, if any.
pub fn server_start(ruby: &Ruby, config: magnus::RHash) -> Result<(u64, u16, Option<u16>), Error> {
    let mut config = ServerConfig::try_from(config)?;
    let acceptor = config
        .tls
        .as_ref()
        .map(|(cert, key)| {
            crate::tls::build_acceptor(cert, key, config.http2)
                .map_err(|e| Error::new(ruby.exception_runtime_error(), format!("TLS: {e}")))
        })
        .transpose()?;

    let listener =
        Listener::bind(&config.bind, config.port).map_err(|e| io_error(ruby, "bind failed", e))?;
    // Ruby refuses this up front; this guards embedders calling the native layer directly.
    if acceptor.is_some() && matches!(listener, Listener::Unix(..)) {
        return Err(Error::new(
            ruby.exception_arg_error(),
            "TLS is not supported on a unix socket bind",
        ));
    }
    let local_port = listener
        .port()
        .map_err(|e| io_error(ruby, "listener setup failed", e))?;
    if let Listener::Unix(_, path) = &listener {
        config.unix_path = Some(path.clone());
    }

    let control_bind = config
        .control_bind
        .as_deref()
        .map(|addr| {
            crate::control::bind_control(addr).map_err(|e| io_error(ruby, "control bind failed", e))
        })
        .transpose()?;

    let server = Arc::new(ServerInner::new(config));
    // Before the runtime, as a server that fails here has nothing running yet.
    if server.config.worker_loop {
        let ticker = crate::ticker::Ticker::start(&server)
            .map_err(|e| io_error(ruby, "worker_loop ticker failed", e))?;
        *server.ticker.lock() = Some(ticker);
    }
    let runtime =
        start_runtime(&server, listener, acceptor).map_err(|(what, e)| io_error(ruby, what, e))?;
    *server.runtime.lock() = runtime;

    let id = server.id;
    let control_port = match control_bind {
        Some(bind) => {
            start_control(&server, bind).map_err(|e| io_error(ruby, "control start failed", e))?
        }
        None => None,
    };
    registry::insert(server);
    Ok((id, local_port, control_port))
}

/// The I/O threads, accepting from `listener`: the shards, or tokio's multi-thread runtime.
/// An error comes with what failed.
fn start_runtime(
    server: &Arc<ServerInner>,
    listener: Listener,
    acceptor: Option<tokio_rustls::TlsAcceptor>,
) -> Result<RuntimeHandle, (&'static str, std::io::Error)> {
    let config = &server.config;
    let shutdown_rx = server.shutdown_tx.subscribe();
    if config.io_shards {
        // The shards serve on while the acceptor drains; this signal stops them at final teardown.
        let (runtime_shutdown_tx, runtime_shutdown_rx) = tokio::sync::watch::channel(false);
        let threads = crate::io_shards::spawn(
            listener,
            acceptor,
            server.clone(),
            config.max_connections,
            shutdown_rx,
            runtime_shutdown_rx,
            crate::io_shards::thread_count(config.io_threads),
        )
        .map_err(|e| ("sharded runtime failed", e))?;
        return Ok(RuntimeHandle::Shards {
            shutdown_tx: runtime_shutdown_tx,
            threads,
        });
    }
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all().thread_name("kino-tokio");
    if config.tokio_threads > 0 {
        builder.worker_threads(config.tokio_threads);
    }
    let runtime = builder.build().map_err(|e| ("tokio runtime failed", e))?;
    let tokio_listener = {
        let _guard = runtime.enter();
        AsyncListener::from_std(listener).map_err(|e| ("listener setup failed", e))?
    };
    let _ = server.io_runtime.set(runtime.handle().clone());
    runtime.spawn(accept_loop(
        tokio_listener,
        acceptor,
        server.clone(),
        config.max_connections,
        shutdown_rx,
    ));
    Ok(RuntimeHandle::MultiThread(runtime))
}

/// The control plane's port, if it listens on one. On failure the runtime stops here: not
/// registered yet, nothing else could shut it down, and the accept task's Arc keeps it alive
/// (a plain Ruby thread, safe to block).
fn start_control(
    server: &Arc<ServerInner>,
    bind: crate::control::ControlBind,
) -> std::io::Result<Option<u16>> {
    let token = server.config.control_token.clone();
    crate::control::start(bind, server.clone(), token).inspect_err(|_| {
        // Nothing serves yet: the bound matters only for a wedged shard thread.
        let _ = server.shutdown_tx.send(true);
        std::mem::take(&mut *server.runtime.lock()).shutdown(Duration::from_millis(1_000));
    })
}
