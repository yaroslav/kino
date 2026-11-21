//! The Symbols natives tell apart or hand Ruby on hot paths, made once at init on the main
//! Ractor. An interned Symbol is one VALUE, the same in every Ractor for the process's life, so
//! telling one apart is a compare and handing one to Ruby allocates nothing, where reading a
//! Symbol's name takes the VM-wide lock once more than one Ractor exists and Ruby::to_symbol
//! allocates a String.

use std::sync::OnceLock;

use magnus::rb_sys::{AsRawValue, FromRawValue};
use magnus::{Ruby, Value};

#[derive(Clone, Copy)]
pub(crate) struct Sym(rb_sys::VALUE);

impl Sym {
    pub(crate) fn is(self, value: Value) -> bool {
        value.as_raw() == self.0
    }

    pub(crate) fn value(self) -> Value {
        // SAFETY: an interned Symbol's VALUE stays valid in every Ractor for the process's life.
        unsafe { Value::from_raw(self.0) }
    }
}

pub(crate) struct Symbols {
    /// The rooms Kino::Native::Worker#take_nonblock narrows to.
    pub(crate) events: Sym,
    pub(crate) requests: Sym,
    pub(crate) wake: Sym,
    pub(crate) message: Sym,
    pub(crate) shutdown: Sym,
    pub(crate) drained: Sym,
    pub(crate) close: Sym,
    pub(crate) websocket: Sym,
}

static SYMBOLS: OnceLock<Symbols> = OnceLock::new();

/// At init, on the main Ractor, before any native that reads them runs.
pub(crate) fn init(ruby: &Ruby) {
    let sym = |name: &str| Sym(ruby.sym_new(name).as_raw());
    let _ = SYMBOLS.set(Symbols {
        events: sym("events"),
        requests: sym("requests"),
        wake: sym("wake"),
        message: sym("message"),
        shutdown: sym("shutdown"),
        drained: sym("drained"),
        close: sym("close"),
        websocket: sym("websocket"),
    });
}

pub(crate) fn get() -> &'static Symbols {
    SYMBOLS.get().expect("Kino's Symbols are made at init")
}
