//! The request heads served inline: a strict subset of HTTP/1.1, read with httparse. Anything
//! else goes back to hyper, whose own rules then decide.

use bytes::Bytes;
use smallvec::SmallVec;

use crate::framing::{digits, tokens, MAX_HEADERS};

/// The longest head served inline; a longer one goes back to hyper.
pub const HEAD_MAX: usize = 16 * 1024;
/// The longest request body hyper frames by its length.
const LENGTH_MAX: u64 = u64::MAX - 2;

pub type Span = (usize, usize);

/// A complete head the subset covers, as offsets into its buffer.
pub struct Head {
    /// Its length in bytes, the empty line included.
    pub len: usize,
    pub method: http::Method,
    pub path: Span,
    pub headers: SmallVec<[(Span, Span); 24]>,
    pub close: bool,
    /// Its body's length: Content-Length, or 0 without one.
    pub content_length: u64,
}

// On the stack, one per look: boxing the head would allocate per request.
#[allow(clippy::large_enum_variant)]
pub enum Parsed {
    Partial,
    /// Outside the subset: hyper's to read.
    Refused,
    Head(Head),
}

/// The head at the start of `buf`, if complete and in the subset: HTTP/1.1, origin-form, no
/// Transfer-Encoding, Expect or protocol switch (CONNECT included), at most one all-digits
/// Content-Length up to LENGTH_MAX, and at most MAX_HEADERS headers.
pub fn parse_head(buf: &[u8]) -> Parsed {
    let mut slots = [httparse::EMPTY_HEADER; MAX_HEADERS];
    let mut req = httparse::Request::new(&mut slots);
    let len = match req.parse(buf) {
        Ok(httparse::Status::Complete(len)) => len,
        Ok(httparse::Status::Partial) => return Parsed::Partial,
        Err(_) => return Parsed::Refused,
    };
    let (Some(method), Some(path), Some(version)) = (req.method, req.path, req.version) else {
        return Parsed::Refused;
    };
    let Ok(method) = http::Method::from_bytes(method.as_bytes()) else {
        return Parsed::Refused;
    };
    let base = buf.as_ptr() as usize;
    let at = |part: &[u8]| {
        let start = part.as_ptr() as usize - base;
        (start, start + part.len())
    };

    let (mut chunked, mut expect, mut close) = (false, false, false);
    // hyper holds a CONNECT's connection for a tunnel, as an Upgrade's.
    let mut upgrade = method == http::Method::CONNECT;
    let mut lengths = 0;
    let mut content_length = Some(0);
    let mut headers = SmallVec::new();
    for header in req.headers.iter() {
        let (name, value) = (header.name, header.value);
        if name.eq_ignore_ascii_case("transfer-encoding") {
            chunked = true;
        } else if name.eq_ignore_ascii_case("content-length") {
            lengths += 1;
            content_length = digits(value).filter(|&length| length <= LENGTH_MAX);
        } else if name.eq_ignore_ascii_case("expect") {
            expect = true;
        } else if name.eq_ignore_ascii_case("upgrade")
            || name.eq_ignore_ascii_case("http2-settings")
        {
            upgrade = true;
        } else if name.eq_ignore_ascii_case("connection") {
            for token in tokens(value) {
                // hyper reads no close in a value that is not visible ASCII.
                close |= token.eq_ignore_ascii_case(b"close") && value.is_ascii();
                upgrade |= token.eq_ignore_ascii_case(b"upgrade");
            }
        }
        headers.push((at(name.as_bytes()), at(value)));
    }

    // Every header was looked at; the first refusal in this order wins.
    if version != 1 {
        return Parsed::Refused;
    }
    if !path.starts_with('/') {
        return Parsed::Refused;
    }
    if chunked {
        return Parsed::Refused;
    }
    let (0..=1, Some(content_length)) = (lengths, content_length) else {
        return Parsed::Refused;
    };
    if expect {
        return Parsed::Refused;
    }
    if upgrade {
        return Parsed::Refused;
    }
    Parsed::Head(Head {
        len,
        method,
        path: at(path.as_bytes()),
        headers,
        close,
        content_length,
    })
}

impl Head {
    /// Its target and headers, from a copy of the bytes `head` was parsed from, which the values
    /// share. The http crate's own checks refuse what httparse let through.
    pub fn parts(&self, head: &Bytes) -> Option<(http::Uri, http::HeaderMap)> {
        let uri = http::Uri::from_maybe_shared(head.slice(self.path.0..self.path.1)).ok()?;
        let mut headers = http::HeaderMap::with_capacity(self.headers.len());
        for &((n0, n1), (v0, v1)) in &self.headers {
            let name = http::HeaderName::from_bytes(&head[n0..n1]).ok()?;
            let value = http::HeaderValue::from_maybe_shared(head.slice(v0..v1)).ok()?;
            headers.append(name, value);
        }
        Some((uri, headers))
    }
}
