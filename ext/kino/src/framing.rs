//! HTTP/1 framing helpers: bodiless answers, Content-Length, and comma-separated header lists.

/// No body whatever the head declares (RFC 9110 6.4.1): a 1xx, 204 or 304, or a HEAD answer.
pub fn bodiless(status: u16, head_request: bool) -> bool {
    head_request || matches!(status, 100..=199 | 204 | 304)
}

/// The Content-Length header's value; None when it is absent or not a number.
pub fn content_length(headers: &http::HeaderMap) -> Option<u64> {
    let value = headers.get(http::header::CONTENT_LENGTH)?.to_str().ok()?;
    value.trim().parse().ok()
}

/// The items of a comma-separated header value, trimmed.
pub fn tokens(value: &str) -> impl DoubleEndedIterator<Item = &str> {
    value.split(',').map(str::trim)
}
