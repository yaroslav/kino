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
//! statics (it is Send + Sync); GC registration makes them immortal. Built
//! once, with the GVL held, on the main ractor; read-only afterwards.

use std::net::IpAddr;
use std::sync::OnceLock;

use magnus::value::Opaque;
use magnus::{gc, prelude::*, RString, Ruby, Value};
use parking_lot::{Mutex, RwLock};

/// These maps are probed several times per request; ahash beats the
/// DoS-resistant default since every key here is our own static data.
type HashMap<K, V> = std::collections::HashMap<K, V, ahash::RandomState>;

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
    /// Host-header or :authority bytes -> frozen host values, and peer
    /// IP -> frozen REMOTE_ADDR value. Real traffic has low cardinality
    /// on both, so these kill 3 string allocations per request.
    /// LRU-bounded: entries are BoxValue-rooted (registered with the GC on
    /// insert, UNregistered on eviction-drop), so a rotating-host attack
    /// recycles cache slots instead of leaking immortal strings.
    pub hosts: Mutex<lru::LruCache<Vec<u8>, HostEntry, ahash::RandomState>>,
    pub addrs: Mutex<lru::LruCache<IpAddr, CachedStr, ahash::RandomState>>,
    /// Ractor-shareable defaults provided by the Ruby layer at boot:
    /// the frozen rack.errors writer and the frozen null rack.input.
    pub errors_stream: RwLock<Option<Opaque<Value>>>,
    pub null_input: RwLock<Option<Opaque<Value>>>,
}

const HOST_CACHE_CAP: usize = 256;
const ADDR_CACHE_CAP: usize = 1024;

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

/// A frozen RString rooted via BoxValue (GC-registered address; unregisters
/// on Drop, so LRU eviction actually frees the string).
///
/// SAFETY of Send + Sync: the contents are frozen Ruby strings (therefore
/// Ractor-shareable), and creation, reads, and drops all happen while the
/// calling thread holds its GVL (native method context) AND the owning
/// cache Mutex; readers root the value into a live env Hash before
/// releasing the lock, so an eviction on another ractor can never free a
/// string that a reader still holds unrooted.
pub struct CachedStr(magnus::value::BoxValue<RString>);
unsafe impl Send for CachedStr {}
unsafe impl Sync for CachedStr {}

impl CachedStr {
    fn new(ruby: &Ruby, s: &str) -> Self {
        let string = ruby.str_new(s);
        string.freeze();
        CachedStr(magnus::value::BoxValue::new(string))
    }

    fn get(&self) -> RString {
        *self.0
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

fn frozen(ruby: &Ruby, s: &str) -> Opaque<RString> {
    let string = ruby.str_new(s);
    string.freeze();
    gc::register_mark_object(string);
    Opaque::from(string)
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
        hosts: Mutex::new(lru::LruCache::with_hasher(
            std::num::NonZeroUsize::new(HOST_CACHE_CAP).unwrap(),
            ahash::RandomState::new(),
        )),
        addrs: Mutex::new(lru::LruCache::with_hasher(
            std::num::NonZeroUsize::new(ADDR_CACHE_CAP).unwrap(),
            ahash::RandomState::new(),
        )),
        errors_stream: RwLock::new(None),
        null_input: RwLock::new(None),
    };
    let _ = ENV_STRINGS.set(strings);
}

pub fn get() -> &'static EnvStrings {
    ENV_STRINGS.get().expect("env_strings::init not called")
}

/// Called once from lib/kino.rb (main ractor) with the frozen,
/// Ractor-shareable singletons the Ruby layer owns.
pub fn register_defaults(
    ruby: &Ruby,
    errors: Value,
    null_input: Value,
) -> Result<(), magnus::Error> {
    for value in [errors, null_input] {
        if !value.is_frozen() {
            return Err(magnus::Error::new(
                ruby.exception_arg_error(),
                "register_defaults expects frozen objects",
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

/// Set SERVER_NAME/SERVER_PORT on `env` from the LRU host cache, building
/// (and caching) frozen values on miss. The aset happens UNDER the cache
/// lock; see CachedStr's safety contract.
pub fn set_host_env(
    ruby: &Ruby,
    env: magnus::RHash,
    host: &[u8],
    make: impl FnOnce() -> (String, u16),
) -> Result<(), magnus::Error> {
    let s = get();
    let mut hosts = s.hosts.lock();
    let (name, port) = match hosts.get(host) {
        Some(entry) => (entry.name.get(), entry.port.get()),
        None => {
            let (name_s, port_n) = make();
            let entry = HostEntry {
                name: CachedStr::new(ruby, &name_s),
                port: CachedStr::new(ruby, &port_n.to_string()),
                host: None,
            };
            let values = (entry.name.get(), entry.port.get());
            hosts.put(host.to_vec(), entry); // may evict + free an old entry
            values
        }
    };
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
    let mut hosts = s.hosts.lock();
    let (name, port, host) = match hosts.get_mut(authority.as_bytes()) {
        Some(entry) => {
            if entry.host.is_none() {
                entry.host = Some(CachedStr::new(ruby, authority));
            }
            (
                entry.name.get(),
                entry.port.get(),
                entry.host.as_ref().expect("just filled").get(),
            )
        }
        None => {
            let (name_s, port_n) = make();
            let entry = HostEntry {
                name: CachedStr::new(ruby, &name_s),
                port: CachedStr::new(ruby, &port_n.to_string()),
                host: Some(CachedStr::new(ruby, authority)),
            };
            let values = (
                entry.name.get(),
                entry.port.get(),
                entry.host.as_ref().expect("just built").get(),
            );
            hosts.put(authority.as_bytes().to_vec(), entry);
            values
        }
    };
    env.aset(ruby.get_inner(s.server_name), name)?;
    env.aset(ruby.get_inner(s.server_port), port)?;
    let host_key = *s.header_names.get("host").expect("host is a common header");
    env.aset(ruby.get_inner(host_key), host)?;
    Ok(())
}

/// Set REMOTE_ADDR on `env` from the LRU peer-IP cache; same locking
/// contract as set_host_env.
pub fn set_addr_env(ruby: &Ruby, env: magnus::RHash, ip: IpAddr) -> Result<(), magnus::Error> {
    let s = get();
    let mut addrs = s.addrs.lock();
    let value = match addrs.get(&ip) {
        Some(cached) => cached.get(),
        None => {
            let entry = CachedStr::new(ruby, &ip.to_string());
            let value = entry.get();
            addrs.put(ip, entry);
            value
        }
    };
    env.aset(ruby.get_inner(s.remote_addr), value)?;
    Ok(())
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
