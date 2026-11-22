//! The readiness signal a worker_loop worker's fiber scheduler waits on.

use std::io::{Read, Write};
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

/// Armed by a worker that finds nothing to take; the next lane send or close disarms it and
/// writes one byte, so a send to a busy worker makes no syscall and takes no lock.
#[derive(Default)]
pub struct Doorbell {
    armed: AtomicBool,
    /// A ring writes under this lock, so never to a closed descriptor or a reused number.
    sockets: Mutex<Option<Arc<DoorbellSockets>>>,
    /// Runs once, between a look that found nothing and the arm, where tests send.
    #[cfg(test)]
    before_arm: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

pub struct DoorbellSockets {
    rx: UnixStream,
    tx: UnixStream,
}

impl DoorbellSockets {
    fn pair() -> std::io::Result<Arc<Self>> {
        let (rx, tx) = UnixStream::pair()?;
        rx.set_nonblocking(true)?;
        tx.set_nonblocking(true)?;
        Ok(Arc::new(DoorbellSockets { rx, tx }))
    }

    pub fn fd(&self) -> RawFd {
        self.rx.as_raw_fd()
    }

    pub fn drain(&self) {
        let mut rings = [0u8; 64];
        while matches!((&self.rx).read(&mut rings), Ok(n) if n > 0) {}
    }

    /// One read: a ring writes one byte as it disarms; any left over cost one empty wake-up.
    pub fn take_rings(&self) {
        let _ = (&self.rx).read(&mut [0u8; 64]);
    }
}

impl Doorbell {
    pub fn new() -> std::io::Result<Self> {
        let bell = Doorbell::default();
        bell.open()?;
        Ok(bell)
    }

    pub fn sockets(&self) -> Option<Arc<DoorbellSockets>> {
        self.sockets.lock().clone()
    }

    /// The descriptor to wait on, emptied of past rings; made on first use.
    pub fn open(&self) -> std::io::Result<RawFd> {
        let mut sockets = self.sockets.lock();
        Ok(match sockets.as_ref() {
            Some(open) => {
                open.drain();
                open.fd()
            }
            None => sockets.insert(DoorbellSockets::pair()?).fd(),
        })
    }

    pub fn release(&self) {
        self.sockets.lock().take();
    }

    /// The worker must look once more after arming: a send just before it found no arm.
    pub fn arm(&self) {
        #[cfg(test)]
        {
            let hook = self.before_arm.lock().take();
            hook.into_iter().for_each(|hook| hook());
        }
        self.armed.store(true, Ordering::SeqCst);
    }

    #[cfg(test)]
    pub fn before_arm(&self, hook: impl FnOnce() + Send + 'static) {
        *self.before_arm.lock() = Some(Box::new(hook));
    }

    pub fn disarm(&self) {
        self.armed.store(false, Ordering::SeqCst);
    }

    /// The load keeps a busy worker's sends off the flag; a full socket means a ring already
    /// waits.
    /// A thread-safe free may run it (a request given up), so it keeps typed_data.rs's promise:
    /// one write that never waits, under a lock nothing holds across a Ruby call.
    pub fn ring(&self) {
        if self.armed.load(Ordering::SeqCst) && self.armed.swap(false, Ordering::SeqCst) {
            if let Some(sockets) = self.sockets.lock().as_ref() {
                let _ = (&sockets.tx).write(&[1]);
            }
        }
    }
}
