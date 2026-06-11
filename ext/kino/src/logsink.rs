//! Asynchronous log sink: callers push complete lines onto a lock-free
//! channel and return immediately; one dedicated flusher thread batches
//! them into the output. This removes the two per-line costs of a classic
//! Ruby Logger device (the cross-thread mutex and the write syscall)
//! from request threads. Shared by the native access log and
//! Kino::Logger::Device.
//!
//! Durability: the flusher flushes after each drained batch, and dropping
//! the last Sender makes it drain everything and exit; a graceful
//! shutdown loses nothing. A hard crash can lose the tail of the buffer,
//! the standard async-logging trade-off.

use std::io::Write;

pub struct Sink {
    tx: flume::Sender<String>,
}

impl Sink {
    /// `out` is taken by the flusher thread (stdout lock, File, ...).
    pub fn new<W: Write + Send + 'static>(mut out: W) -> Sink {
        let (tx, rx) = flume::bounded::<String>(8192);
        std::thread::Builder::new()
            .name("kino-log".to_string())
            .spawn(move || {
                fn put<W: Write>(out: &mut W, line: &str) {
                    let _ = out.write_all(line.as_bytes());
                    let _ = out.write_all(b"\n");
                }
                // Block for the first line, then drain whatever else is
                // queued before flushing once: batching under load,
                // prompt output when quiet.
                while let Ok(line) = rx.recv() {
                    put(&mut out, &line);
                    while let Ok(line) = rx.try_recv() {
                        put(&mut out, &line);
                    }
                    let _ = out.flush();
                }
                let _ = out.flush();
            })
            .expect("failed to spawn log flusher thread");
        Sink { tx }
    }

    /// Queue one line (without trailing newline). Never blocks the caller:
    /// when the channel is full the line is dropped; backpressure on the
    /// request path is exactly what an async log must not create.
    pub fn write_line(&self, line: String) {
        let _ = self.tx.try_send(line);
    }
}

// --- Ruby-facing log devices (Kino::Logger::Device) -------------------
//
// A device is just a Sink in a registry, addressed by id from Ruby. The
// flusher thread exits when the device is closed (sender dropped).

use std::sync::OnceLock;

type DeviceMap = parking_lot::Mutex<std::collections::HashMap<u64, Sink, ahash::RandomState>>;

static DEVICES: OnceLock<DeviceMap> = OnceLock::new();
static NEXT_DEVICE_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

fn devices() -> &'static DeviceMap {
    DEVICES.get_or_init(|| parking_lot::Mutex::new(std::collections::HashMap::default()))
}

/// Open a device: nil/empty path = stdout, otherwise append-create the
/// file. Returns the device id.
pub fn device_open(ruby: &magnus::Ruby, path: Option<String>) -> Result<u64, magnus::Error> {
    let sink = match path.as_deref() {
        None | Some("") => Sink::new(std::io::stdout()),
        Some(p) => {
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(p)
                .map_err(|e| {
                    magnus::Error::new(
                        ruby.exception_runtime_error(),
                        format!("Kino::Logger::Device: cannot open {p}: {e}"),
                    )
                })?;
            Sink::new(file)
        }
    };
    let id = NEXT_DEVICE_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    devices().lock().insert(id, sink);
    Ok(id)
}

/// Queue a message on a device. The trailing newline (Logger adds one) is
/// trimmed because the sink writes line-wise.
pub fn device_write(
    _ruby: &magnus::Ruby,
    id: u64,
    mut message: String,
) -> Result<(), magnus::Error> {
    if let Some(sink) = devices().lock().get(&id) {
        // Trim in place: no second allocation per line.
        message.truncate(message.trim_end_matches('\n').len());
        sink.write_line(message);
    }
    Ok(())
}

/// Close a device: drops the sink, which makes the flusher drain its
/// queue and exit. Writes after close are silently ignored.
pub fn device_close(_ruby: &magnus::Ruby, id: u64) -> Result<(), magnus::Error> {
    devices().lock().remove(&id);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::Sink;
    use std::io::Write;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    #[derive(Clone, Default)]
    struct SharedBuf(Arc<parking_lot::Mutex<Vec<u8>>>);

    impl Write for SharedBuf {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn sink_writes_lines_and_drains_on_drop() {
        let buf = SharedBuf::default();
        let sink = Sink::new(buf.clone());

        sink.write_line("first".to_string());
        sink.write_line("second".to_string());
        drop(sink); // sender gone: the flusher drains everything and exits

        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if *buf.0.lock() == b"first\nsecond\n" {
                break;
            }
            assert!(Instant::now() < deadline, "flusher never drained");
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}
