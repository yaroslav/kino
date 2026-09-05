//! One-time cache of frozen Ruby strings for the per-request env hash.
//!
//! Frozen strings are Ractor-shareable, so a single cache built on the main
//! ractor at init serves every worker ractor forever. Frozen Hash keys are
//! also stored without the dup `Hash#[]=` does for unfrozen string keys.
//! This removes the bulk of per-request allocations: every static key, the
//! method/protocol/scheme values, and the `HTTP_*` names of all common
//! request headers.
//!
//! `Opaque<RString>` is magnus's sanctioned way to keep Ruby values in
//! statics (it is Send + Sync); GC registration makes the static ones
//! immortal. Built once, with the GVL held, on the main ractor; read-only
//! afterwards.
//!
//! The LRU caches (hosts, peer addresses, interned header values) are the
//! exception: they insert and evict from every worker ractor in parallel.
//! That rules out per-value GC registration, because in Ruby 4.0
//! `rb_gc_register_address` and its unregister walk a VM-wide linked list
//! with no lock. Their strings are rooted through a [`PinSlab`] instead:
//! one atomic VALUE slot per cached string, marked from a keeper object
//! registered once at init. The same parallelism forbids any Ruby call
//! while a cache lock is held: a Ruby allocation can start a GC, a GC
//! with several ractors stops at a barrier every other ractor must
//! reach, and a ractor parked on the cache mutex never would. So the
//! caches allocate their strings first, then lock only to look up, root,
//! insert, or evict.

use std::borrow::Borrow;
use std::hash::Hash;
use std::net::IpAddr;
use std::sync::{Arc, OnceLock};

use magnus::rb_sys::AsRawValue;
use magnus::value::Opaque;
use magnus::{gc, prelude::*, RString, Ruby, Value};
use parking_lot::{Mutex, RwLock};

use crate::pin::{PinKeeper, PinSlab};

/// These maps are probed several times per request; ahash beats the
/// DoS-resistant default since every key here is our own static data.
type HashMap<K, V> = std::collections::HashMap<K, V, ahash::RandomState>;
type HashSet<K> = std::collections::HashSet<K, ahash::RandomState>;
type LruCache<K, V> = lru::LruCache<K, V, ahash::RandomState>;

pub struct EnvStrings {
    // keys
    pub request_method: Opaque<RString>,
    pub script_name: Opaque<RString>,
    pub path_info: Opaque<RString>,
    pub query_string: Opaque<RString>,
    pub server_protocol: Opaque<RString>,
    pub server_name: Opaque<RString>,
    pub server_port: Opaque<RString>,
    pub remote_addr: Opaque<RString>,
    pub content_type: Opaque<RString>,
    pub content_length: Opaque<RString>,
    pub rack_url_scheme: Opaque<RString>,
    pub rack_input: Opaque<RString>,
    pub rack_errors: Opaque<RString>,
    pub kino_request: Opaque<RString>,
    // values
    pub empty: Opaque<RString>,
    pub http: Opaque<RString>,
    pub https: Opaque<RString>,
    pub http10: Opaque<RString>,
    pub http11: Opaque<RString>,
    pub http2: Opaque<RString>,
    pub methods: HashMap<&'static str, Opaque<RString>>,
    /// lowercase header name -> frozen "HTTP_<UPPER>" key
    pub header_names: HashMap<&'static str, Opaque<RString>>,
    /// GC roots for every string the LRU caches below hold (see the
    /// module docs); a cache entry owns its slots and clears them when
    /// it is evicted.
    pub slab: Arc<PinSlab>,
    /// Host-header or :authority bytes -> frozen host values, and peer
    /// IP -> frozen REMOTE_ADDR value. Real traffic has low cardinality
    /// on both, so these kill 3 string allocations per request.
    /// LRU-bounded, so a rotating-host attack recycles cache slots
    /// instead of accumulating immortal strings.
    pub hosts: Mutex<LruCache<Vec<u8>, HostEntry>>,
    pub addrs: Mutex<LruCache<IpAddr, CachedStr>>,
    /// Interned values of low-cardinality headers (see
    /// [`INTERNABLE_VALUES`]): value bytes -> frozen RString, shared
    /// across headers that happen to carry the same bytes. Same rooting
    /// and locking contract as `hosts`/`addrs`.
    pub values: Mutex<LruCache<Vec<u8>, CachedStr>>,
    /// The names whose values go through the `values` cache.
    pub internable: HashSet<&'static str>,
    /// Ractor-shareable defaults provided by the Ruby layer at boot:
    /// the frozen rack.errors writer and the frozen null rack.input.
    pub errors_stream: RwLock<Option<Opaque<Value>>>,
    pub null_input: RwLock<Option<Opaque<Value>>>,
}

const HOST_CACHE_CAP: usize = 256;
const ADDR_CACHE_CAP: usize = 1024;
const VALUE_CACHE_CAP: usize = 512;

/// Slab slots for every string the three caches can hold at once: up to
/// three per host entry, one per address and per interned value, plus
/// one entry per cache for the insert that precedes an eviction. A full
/// slab (which this bound rules out) degrades to an uncached string,
/// never to an unrooted one.
const CACHE_SLAB_CAPACITY: usize =
    (HOST_CACHE_CAP + 1) * 3 + (ADDR_CACHE_CAP + 1) + (VALUE_CACHE_CAP + 1);

/// Values longer than this are never interned: past it the memcpy into
/// a fresh Ruby string is cheap relative to the bytes themselves, and
/// unbounded keys would let one client fill the cache with garbage.
const VALUE_INTERN_MAX_LEN: usize = 512;

/// Headers whose values are effectively enums or per-install constants
/// (a browser resends the same UA, accept-*, and sec-ch-* on every
/// request), so caching kills an allocation + copy per header per
/// request: the env-side analogue of what HPACK does on the wire.
/// Deliberately absent: `cookie` and `authorization` (per-user
/// cardinality would churn the cache, and secrets should not outlive
/// their request in an evict-to-free cache), `referer`/`x-request-id`
/// and friends (unbounded cardinality).
const INTERNABLE_VALUES: &[&str] = &[
    "user-agent",
    "accept",
    "accept-encoding",
    "accept-language",
    "cache-control",
    "dnt",
    "origin",
    "pragma",
    "priority",
    "sec-ch-ua",
    "sec-ch-ua-mobile",
    "sec-ch-ua-platform",
    "sec-fetch-dest",
    "sec-fetch-mode",
    "sec-fetch-site",
    "sec-fetch-user",
    "upgrade-insecure-requests",
    "x-requested-with",
];

/// One hosts-cache entry: the frozen SERVER_NAME/SERVER_PORT pair, plus
/// the frozen full authority ("host[:port]" as sent) used as the
/// HTTP_HOST value for requests that carry the name in the URI (the h2
/// :authority pseudo-header) rather than a Host header. Lazily filled:
/// Host-header entries and the NUL-prefixed socket-fallback entries
/// never allocate it.
pub struct HostEntry {
    name: CachedStr,
    port: CachedStr,
    host: Option<CachedStr>,
}

impl HostEntry {
    /// Root a fresh entry; None (with nothing left rooted) when the slab
    /// is full.
    fn root(
        slab: &Arc<PinSlab>,
        name: RString,
        port: RString,
        host: Option<RString>,
    ) -> Option<Self> {
        let name = CachedStr::root(slab, name)?;
        let port = CachedStr::root(slab, port)?;
        let host = match host {
            Some(host) => Some(CachedStr::root(slab, host)?),
            None => None,
        };
        Some(HostEntry { name, port, host })
    }

    fn name_port(&self, ruby: &Ruby) -> (RString, RString) {
        (self.name.get(ruby), self.port.get(ruby))
    }

    /// The entry's full-authority string, adopting `host` (rooted, slab
    /// permitting) when it has none yet.
    fn fill_host(&mut self, ruby: &Ruby, slab: &Arc<PinSlab>, host: RString) -> RString {
        if self.host.is_none() {
            self.host = CachedStr::root(slab, host);
        }
        self.host.as_ref().map_or(host, |cached| cached.get(ruby))
    }
}

/// A frozen RString kept alive by one slab slot for as long as its cache
/// entry exists. Drop (LRU eviction, or an insert that lost a race)
/// clears the slot; the string then dies at the next sweep unless an
/// env still references it.
///
/// Readers copy the VALUE out under the cache lock and set it on their
/// env only afterwards (the aset is a Ruby call, so it may not run under
/// the lock; see the module docs). That is sound even when another
/// ractor evicts the entry in between: a VALUE held in a native frame is
/// a conservative GC root, since Ruby scans every thread's machine stack
/// and saved registers on every ractor, so the string outlives the
/// window until the env roots it for good.
pub struct CachedStr {
    value: Opaque<RString>,
    slab: Arc<PinSlab>,
    slot: usize,
}

impl CachedStr {
    /// Root `string` in the slab; None when the slab is full.
    fn root(slab: &Arc<PinSlab>, string: RString) -> Option<Self> {
        let slot = slab.insert(string.as_raw())?;
        Some(CachedStr {
            value: Opaque::from(string),
            slab: slab.clone(),
            slot,
        })
    }

    fn get(&self, ruby: &Ruby) -> RString {
        ruby.get_inner(self.value)
    }
}

impl Drop for CachedStr {
    fn drop(&mut self) {
        self.slab.release(self.slot);
    }
}

static ENV_STRINGS: OnceLock<EnvStrings> = OnceLock::new();

const COMMON_METHODS: &[&str] = &[
    "GET", "HEAD", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "TRACE", "CONNECT",
];

/// Headers worth pre-caching: hyper lowercases names, so these match the
/// wire form of essentially all browser/proxy/SDK traffic.
const COMMON_HEADERS: &[&str] = &[
    "host",
    "connection",
    "user-agent",
    "accept",
    "accept-encoding",
    "accept-language",
    "accept-charset",
    "cookie",
    "referer",
    "origin",
    "authorization",
    "cache-control",
    "pragma",
    "expect",
    "forwarded",
    "via",
    "range",
    "te",
    "dnt",
    "upgrade-insecure-requests",
    "if-none-match",
    "if-modified-since",
    "if-match",
    "if-unmodified-since",
    "if-range",
    "x-forwarded-for",
    "x-forwarded-proto",
    "x-forwarded-host",
    "x-forwarded-port",
    "x-real-ip",
    "x-request-id",
    "x-requested-with",
    "x-csrf-token",
    "x-api-key",
    "content-encoding",
    "content-language",
    "sec-fetch-dest",
    "sec-fetch-mode",
    "sec-fetch-site",
    "sec-fetch-user",
    "sec-ch-ua",
    "sec-ch-ua-mobile",
    "sec-ch-ua-platform",
    "keep-alive",
    "priority",
    "alt-used",
];

pub fn cgi_name(lower: &str) -> String {
    let mut key = String::with_capacity(5 + lower.len());
    key.push_str("HTTP_");
    for ch in lower.chars() {
        key.push(match ch {
            '-' => '_',
            c => c.to_ascii_uppercase(),
        });
    }
    key
}

/// A frozen string rooted for the life of the process. Init only: the
/// registration takes the VM lock, and nothing else must exist yet.
fn frozen(ruby: &Ruby, s: &str) -> Opaque<RString> {
    let string = frozen_str(ruby, s);
    gc::register_mark_object(string);
    Opaque::from(string)
}

/// A fresh frozen string, rooted only by the caller's stack until a
/// cache slot or an env takes it.
fn frozen_str(ruby: &Ruby, s: &str) -> RString {
    let string = ruby.str_new(s);
    string.freeze();
    string
}

/// Header values are bytes on the wire (not guaranteed UTF-8), so they
/// cache as the same binary strings `str_from_slice` builds on the
/// uncached path; interning must not change the encoding an app
/// observes.
fn frozen_slice(ruby: &Ruby, bytes: &[u8]) -> RString {
    let string = ruby.str_from_slice(bytes);
    string.freeze();
    string
}

fn lru<K: Hash + Eq, V>(capacity: usize) -> Mutex<LruCache<K, V>> {
    Mutex::new(LruCache::with_hasher(
        std::num::NonZeroUsize::new(capacity).expect("cache capacity is non-zero"),
        ahash::RandomState::new(),
    ))
}

/// Build the cache. Main ractor, GVL held, before any worker exists.
pub fn init(ruby: &Ruby) {
    let methods = COMMON_METHODS
        .iter()
        .map(|m| (*m, frozen(ruby, m)))
        .collect::<HashMap<_, _>>();
    let header_names = COMMON_HEADERS
        .iter()
        .map(|h| (*h, frozen(ruby, &cgi_name(h))))
        .collect::<HashMap<_, _>>();

    // The slab's GC-visible face lives for the process: registered here
    // on the main ractor, before any worker ractor can exist.
    let slab = Arc::new(PinSlab::with_capacity(CACHE_SLAB_CAPACITY));
    gc::register_mark_object(ruby.obj_wrap(PinKeeper(slab.clone())));

    let strings = EnvStrings {
        request_method: frozen(ruby, "REQUEST_METHOD"),
        script_name: frozen(ruby, "SCRIPT_NAME"),
        path_info: frozen(ruby, "PATH_INFO"),
        query_string: frozen(ruby, "QUERY_STRING"),
        server_protocol: frozen(ruby, "SERVER_PROTOCOL"),
        server_name: frozen(ruby, "SERVER_NAME"),
        server_port: frozen(ruby, "SERVER_PORT"),
        remote_addr: frozen(ruby, "REMOTE_ADDR"),
        content_type: frozen(ruby, "CONTENT_TYPE"),
        content_length: frozen(ruby, "CONTENT_LENGTH"),
        rack_url_scheme: frozen(ruby, "rack.url_scheme"),
        rack_input: frozen(ruby, "rack.input"),
        rack_errors: frozen(ruby, "rack.errors"),
        kino_request: frozen(ruby, "kino.request"),
        empty: frozen(ruby, ""),
        http: frozen(ruby, "http"),
        https: frozen(ruby, "https"),
        http10: frozen(ruby, "HTTP/1.0"),
        http11: frozen(ruby, "HTTP/1.1"),
        http2: frozen(ruby, "HTTP/2"),
        methods,
        header_names,
        slab,
        hosts: lru(HOST_CACHE_CAP),
        addrs: lru(ADDR_CACHE_CAP),
        values: lru(VALUE_CACHE_CAP),
        internable: INTERNABLE_VALUES.iter().copied().collect(),
        errors_stream: RwLock::new(None),
        null_input: RwLock::new(None),
    };
    let _ = ENV_STRINGS.set(strings);
}

pub fn get() -> &'static EnvStrings {
    ENV_STRINGS.get().expect("env_strings::init not called")
}

/// Called once from lib/kino.rb (main ractor) with the Ractor-shareable
/// singletons the Ruby layer owns. Shareability is the real requirement
/// (every worker ractor reads them out of its envs), and it is stricter
/// than frozen: a frozen object whose state is not itself shareable
/// fails it. `Ractor.shareable?` is `rb_ractor_shareable_p`, which rb-sys
/// does not bind (it is a static inline).
pub fn register_defaults(
    ruby: &Ruby,
    errors: Value,
    null_input: Value,
) -> Result<(), magnus::Error> {
    let ractor: Value = ruby.class_object().const_get("Ractor")?;
    for value in [errors, null_input] {
        let shareable: bool = ractor.funcall("shareable?", (value,))?;
        if !shareable {
            return Err(magnus::Error::new(
                ruby.exception_arg_error(),
                "register_defaults expects Ractor-shareable objects",
            ));
        }
    }
    gc::register_mark_object(errors);
    gc::register_mark_object(null_input);
    let s = get();
    *s.errors_stream.write() = Some(Opaque::from(errors));
    *s.null_input.write() = Some(Opaque::from(null_input));
    Ok(())
}

/// The cached value for `key`, or a freshly built one, inserted for the
/// next caller. `build` runs with the lock released (it allocates Ruby
/// strings; see the module docs); `read` copies a cached entry's value
/// out; `root` turns a fresh value into an entry, or None when the slab
/// is full, in which case the value is used uncached. An entry a racing
/// ractor inserted meanwhile wins over ours, whose string is then plain
/// garbage.
fn get_or_insert<K, Q, V, T>(
    cache: &Mutex<LruCache<K, V>>,
    key: &Q,
    read: impl Fn(&V) -> T,
    build: impl FnOnce() -> T,
    root: impl FnOnce(&T) -> Option<V>,
    own_key: impl FnOnce() -> K,
) -> T
where
    K: Hash + Eq + Borrow<Q>,
    Q: Hash + Eq + ?Sized,
{
    if let Some(found) = cache.lock().get(key) {
        return read(found);
    }
    let fresh = build();
    let mut cache = cache.lock();
    if let Some(found) = cache.get(key) {
        return read(found);
    }
    if let Some(entry) = root(&fresh) {
        cache.put(own_key(), entry); // may evict: the entry's Drop clears its slots
    }
    fresh
}

/// Set SERVER_NAME/SERVER_PORT on `env` from the LRU host cache, building
/// (and caching) frozen values on miss.
pub fn set_host_env(
    ruby: &Ruby,
    env: magnus::RHash,
    host: &[u8],
    make: impl FnOnce() -> (String, u16),
) -> Result<(), magnus::Error> {
    let s = get();
    let (name, port) = get_or_insert(
        &s.hosts,
        host,
        |entry| entry.name_port(ruby),
        || {
            let (name, port) = make();
            (frozen_str(ruby, &name), frozen_str(ruby, &port.to_string()))
        },
        |&(name, port)| HostEntry::root(&s.slab, name, port, None),
        || host.to_vec(),
    );
    env.aset(ruby.get_inner(s.server_name), name)?;
    env.aset(ruby.get_inner(s.server_port), port)?;
    Ok(())
}

/// Set SERVER_NAME/SERVER_PORT *and* HTTP_HOST on `env` from the URI
/// authority (every h2 request via :authority; also h1 absolute-form).
/// HTTP_HOST is set here because such requests carry no Host header for
/// the header loop to surface. Same cache and locking contract as
/// [`set_host_env`]; an entry first created by a Host header upgrades in
/// place, gaining the full-authority string on first use.
pub fn set_authority_env(
    ruby: &Ruby,
    env: magnus::RHash,
    authority: &str,
    make: impl FnOnce() -> (String, u16),
) -> Result<(), magnus::Error> {
    let s = get();
    let key = authority.as_bytes();
    let read = |entry: &HostEntry| {
        let (name, port) = entry.name_port(ruby);
        (name, port, entry.host.as_ref().map(|host| host.get(ruby)))
    };
    // Bound to a local so the lock guard (a temporary of this statement)
    // is gone before the arms below take the lock again.
    let hit = s.hosts.lock().get(key).map(read);
    let (name, port, host) = match hit {
        Some((name, port, Some(host))) => (name, port, host),
        Some((name, port, None)) => {
            let host = frozen_str(ruby, authority);
            let mut hosts = s.hosts.lock();
            match hosts.get_mut(key) {
                Some(entry) => (name, port, entry.fill_host(ruby, &s.slab, host)),
                // Evicted meanwhile; the strings in hand are still valid.
                None => (name, port, host),
            }
        }
        None => {
            let (name, port) = make();
            let name = frozen_str(ruby, &name);
            let port = frozen_str(ruby, &port.to_string());
            let host = frozen_str(ruby, authority);
            let mut hosts = s.hosts.lock();
            match hosts.get_mut(key) {
                // A racing ractor got there first; share its entry.
                Some(entry) => {
                    let (name, port) = entry.name_port(ruby);
                    (name, port, entry.fill_host(ruby, &s.slab, host))
                }
                None => {
                    if let Some(entry) = HostEntry::root(&s.slab, name, port, Some(host)) {
                        hosts.put(key.to_vec(), entry);
                    }
                    (name, port, host)
                }
            }
        }
    };
    env.aset(ruby.get_inner(s.server_name), name)?;
    env.aset(ruby.get_inner(s.server_port), port)?;
    let host_key = *s.header_names.get("host").expect("host is a common header");
    env.aset(ruby.get_inner(host_key), host)?;
    Ok(())
}

/// Set one header's value on `env` under `key`: through the interned
/// value cache when the header qualifies (low-cardinality name, bounded
/// length), else a fresh per-request string.
pub fn set_value_env(
    ruby: &Ruby,
    env: magnus::RHash,
    key: RString,
    name: &str,
    value: &[u8],
) -> Result<(), magnus::Error> {
    let s = get();
    if value.len() > VALUE_INTERN_MAX_LEN || !s.internable.contains(name) {
        return env.aset(key, ruby.str_from_slice(value));
    }
    let cached = get_or_insert(
        &s.values,
        value,
        |cached| cached.get(ruby),
        || frozen_slice(ruby, value),
        |&fresh| CachedStr::root(&s.slab, fresh),
        || value.to_vec(),
    );
    env.aset(key, cached)
}

/// Set REMOTE_ADDR on `env` from the LRU peer-IP cache.
pub fn set_addr_env(ruby: &Ruby, env: magnus::RHash, ip: IpAddr) -> Result<(), magnus::Error> {
    let s = get();
    let value = get_or_insert(
        &s.addrs,
        &ip,
        |cached| cached.get(ruby),
        || frozen_str(ruby, &ip.to_string()),
        |&fresh| CachedStr::root(&s.slab, fresh),
        || ip,
    );
    env.aset(ruby.get_inner(s.remote_addr), value)
}

#[cfg(test)]
mod tests {
    use super::{cgi_name, COMMON_HEADERS, COMMON_METHODS};

    #[test]
    fn cgi_names() {
        assert_eq!(cgi_name("x-request-id"), "HTTP_X_REQUEST_ID");
        assert_eq!(cgi_name("host"), "HTTP_HOST");
        assert_eq!(cgi_name(""), "HTTP_");
        assert_eq!(cgi_name("sec-ch-ua"), "HTTP_SEC_CH_UA");
    }

    // The cache is keyed by hyper's lowercase wire form; one uppercase
    // entry would silently never hit.
    #[test]
    fn common_headers_are_lowercase_and_unique() {
        let mut seen = std::collections::HashSet::new();
        for header in COMMON_HEADERS {
            assert_eq!(
                *header,
                header.to_ascii_lowercase(),
                "{header} must be lowercase"
            );
            assert!(seen.insert(*header), "{header} listed twice");
        }
    }

    #[test]
    fn common_methods_are_uppercase_and_unique() {
        let mut seen = std::collections::HashSet::new();
        for method in COMMON_METHODS {
            assert_eq!(
                *method,
                method.to_ascii_uppercase(),
                "{method} must be uppercase"
            );
            assert!(seen.insert(*method), "{method} listed twice");
        }
    }
}
