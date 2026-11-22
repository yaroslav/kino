//! Response heads for inline connections, framed as hyper's HTTP/1.1 server frames them:
//! lowercase names, `server: Kino` unless the app set one, no date.

use std::io::Write;

use http::{header, HeaderMap, StatusCode};
use smallvec::SmallVec;

use crate::framing::{bodiless, digits, has_token, lengthless, usable_length};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Body {
    Length(usize),
    Chunked,
}

/// The head of a response (`head_only`: to a HEAD), its body's framing, and whether the
/// connection closes after it (`close`, or the app's Connection asks). Framed as hyper frames it:
/// Length(0) where no body may follow, else `body`, an unknown length (Chunked) taking the app's
/// Content-Length. None where hyper refuses the head and ends the connection: an unknown-length
/// body whose Content-Length is invalid, disagrees with another or comes with a
/// Transfer-Encoding, or an empty HEAD answer with more than one (never on a 1xx or a 204). A
/// known length is written as it is, except that a HEAD's keeps the app's first; an empty body
/// gets none unless it answers a HEAD; Transfer-Encoding is dropped.
pub fn head(
    status: StatusCode,
    headers: &HeaderMap,
    body: Body,
    head_only: bool,
    close: bool,
) -> Option<(Vec<u8>, Body, bool)> {
    let mut out = Vec::with_capacity(96 + headers.len() * 40);
    out.extend_from_slice(b"HTTP/1.1 ");
    out.extend_from_slice(status.as_str().as_bytes());
    out.push(b' ');
    // As hyper: a reason must be there, as many parsers expect one.
    out.extend_from_slice(status.canonical_reason().unwrap_or("<none>").as_bytes());
    out.extend_from_slice(b"\r\n");

    let (mut server_set, mut encoded) = (false, false);
    let (mut length, mut more_lengths): (Option<&[u8]>, bool) = (None, false);
    let mut connection: SmallVec<[&[u8]; 2]> = SmallVec::new();
    for (name, value) in headers {
        if name == header::CONNECTION {
            connection.push(value.as_bytes());
            continue;
        }
        if name == header::CONTENT_LENGTH {
            more_lengths |= length.is_some();
            length.get_or_insert(value.as_bytes());
            continue;
        }
        if name == header::TRANSFER_ENCODING {
            encoded = true;
            continue;
        }
        server_set |= name == header::SERVER;
        line(&mut out, name.as_str().as_bytes(), value.as_bytes());
    }
    let asked = connection.iter().any(|value| has_token(value, b"close"));
    if close && !asked {
        connection.push(b"close");
    }
    if !connection.is_empty() {
        line(&mut out, b"connection", &connection.join(&b", "[..]));
    }

    let no_body = bodiless(status.as_u16(), head_only);
    let lengthless = lengthless(status.as_u16());
    let framing = match (body, length) {
        (Body::Length(len), Some(value)) if head_only && !lengthless => {
            if len == 0 && more_lengths {
                return None;
            }
            line(&mut out, b"content-length", value);
            Body::Length(len)
        }
        (Body::Length(len), _) => {
            if !bodiless(status.as_u16(), false) && !(head_only && len == 0) {
                let _ = write!(out, "content-length: {len}\r\n");
            }
            Body::Length(len)
        }
        (Body::Chunked, Some(_)) if lengthless => Body::Length(0),
        (Body::Chunked, Some(value)) => {
            let len = digits(value).and_then(|n| usize::try_from(n).ok());
            let (Some(len), true) = (len, usable_length(headers)) else {
                return None;
            };
            if encoded && !no_body {
                return None;
            }
            // Where no body follows, only a HEAD's says what its GET's would be.
            if head_only || !no_body {
                line(&mut out, b"content-length", value);
            }
            Body::Length(len)
        }
        (Body::Chunked, None) => {
            if !no_body {
                line(&mut out, b"transfer-encoding", b"chunked");
            }
            Body::Chunked
        }
    };
    if !server_set {
        line(&mut out, b"server", b"Kino");
    }
    out.extend_from_slice(b"\r\n");
    let framing = if no_body { Body::Length(0) } else { framing };
    Some((out, framing, close || asked))
}

fn line(out: &mut Vec<u8>, name: &[u8], value: &[u8]) {
    out.extend_from_slice(name);
    out.extend_from_slice(b": ");
    out.extend_from_slice(value);
    out.extend_from_slice(b"\r\n");
}
