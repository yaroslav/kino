//! HTTP/1 framing as hyper reads it: bodiless and lengthless answers, lengths, header lists.

use http::header::CONTENT_LENGTH;

/// No body whatever the head declares (RFC 9110 6.4.1): a 1xx, 204 or 304, or a HEAD answer.
pub fn bodiless(status: u16, head_request: bool) -> bool {
    head_request || matches!(status, 100..=199 | 204 | 304)
}

/// An answer that sends no body, and the one rule for what its head keeps of a Content-Length,
/// whichever way the answer goes out (whole from the worker, streamed, or canned by Kino).
#[derive(Clone, Copy)]
pub struct Bodiless {
    /// By its status alone, whatever the request's method.
    by_status: bool,
    /// A 1xx or a 204: no Content-Length, which RFC 9110 forbids and h2 clients reject.
    lengthless: bool,
}

impl Bodiless {
    /// None for an answer whose body goes out.
    #[inline]
    pub fn of(status: u16, head_request: bool) -> Option<Bodiless> {
        let by_status = bodiless(status, false);
        if !head_request && !by_status {
            return None;
        }
        Some(Bodiless {
            by_status,
            lengthless: matches!(status, 100..=199 | 204),
        })
    }

    /// For a whole body of `len` bytes that stays behind: keep_length unless it is empty, then
    /// drop_forbidden_length. keep_length leaves one Content-Length where the head repeated
    /// it, which loses nothing: a worker's answer comes with one, and a hijack bridge's
    /// answer whose lengths differ was refused with a 502, so only equal ones meet here.
    pub fn settle(&self, headers: &mut http::HeaderMap, len: usize) {
        if len > 0 {
            self.keep_length(headers, len);
        }
        self.drop_forbidden_length(headers);
    }

    /// The app's first Content-Length stays, alone; without one a HEAD's answer gets its body's
    /// `len`, unless that is 0 or its status alone forbids the body (a 304).
    pub fn keep_length(&self, headers: &mut http::HeaderMap, len: usize) {
        match headers.get(CONTENT_LENGTH).cloned() {
            Some(first) => {
                headers.insert(CONTENT_LENGTH, first);
            }
            None if !self.by_status && len > 0 => {
                headers.insert(CONTENT_LENGTH, len.into());
            }
            None => {}
        }
    }

    pub fn drop_forbidden_length(&self, headers: &mut http::HeaderMap) {
        if self.lengthless {
            headers.remove(CONTENT_LENGTH);
        }
    }
}

/// A Content-Length value: digits only, as hyper reads it.
pub fn digits(value: &[u8]) -> Option<u64> {
    if value.is_empty() {
        return None;
    }
    value.iter().try_fold(0u64, |n, &b| {
        if !b.is_ascii_digit() {
            return None;
        }
        n.checked_mul(10)?.checked_add(u64::from(b - b'0'))
    })
}

/// The first Content-Length's value; None when it is absent or not digits.
pub fn content_length(headers: &http::HeaderMap) -> Option<u64> {
    digits(headers.get(CONTENT_LENGTH)?.as_bytes())
}

/// No Content-Length, or values all digits and equal: what hyper takes beside an unknown length.
pub fn usable_length(headers: &http::HeaderMap) -> bool {
    let values = headers.get_all(CONTENT_LENGTH);
    let mut lengths = values.iter().map(|value| digits(value.as_bytes()));
    match lengths.next() {
        None => true,
        Some(first) => first.is_some() && lengths.all(|next| next == first),
    }
}

/// The items of a comma-separated header value, trimmed.
pub fn tokens(value: &str) -> impl DoubleEndedIterator<Item = &str> {
    value.split(',').map(str::trim)
}
