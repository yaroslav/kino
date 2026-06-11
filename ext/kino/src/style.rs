//! ANSI styling for the few places the native layer writes to the
//! terminal: stderr error lines and the stdout access log. (The Ruby-side
//! startup banner has its own twin in Kino::CLI.) Color-capability is
//! decided once per stream; every styled string resets at its end so
//! nothing bleeds.

use std::sync::OnceLock;

#[derive(Clone, Copy)]
enum Stream {
    Stdout,
    Stderr,
}

fn enabled(stream: Stream) -> bool {
    use std::io::IsTerminal;
    static STDOUT: OnceLock<bool> = OnceLock::new();
    static STDERR: OnceLock<bool> = OnceLock::new();

    let env_ok = || {
        std::env::var_os("NO_COLOR").is_none()
            && std::env::var_os("TERM").is_none_or(|t| t != "dumb")
    };
    match stream {
        Stream::Stdout => *STDOUT.get_or_init(|| std::io::stdout().is_terminal() && env_ok()),
        Stream::Stderr => *STDERR.get_or_init(|| std::io::stderr().is_terminal() && env_ok()),
    }
}

/// Wrap `text` in an SGR code (e.g. "31" red, "1" bold, "38;5;208"
/// 256-color), plain when the stream isn't a color terminal.
fn paint(stream: Stream, code: &str, text: &str) -> String {
    if enabled(stream) {
        format!("\x1b[{code}m{text}\x1b[0m")
    } else {
        text.to_string()
    }
}

/// Errors on stderr are bright red (91): the base red slot (31) is
/// remapped to odd hues by some terminal themes; 91 stays red.
pub fn red(text: &str) -> String {
    paint(Stream::Stderr, "91", text)
}

/// The SGR code for a status class (basic 16-color palette only):
/// 2xx green, 3xx yellow, 4xx maroon (ANSI color 1, plain dark red),
/// 5xx bright red, anything else uncolored.
fn status_sgr(status: u16) -> Option<&'static str> {
    match status {
        200..=299 => Some("32"),
        300..=399 => Some("33"),
        400..=499 => Some("31"),
        500..=599 => Some("91"),
        _ => None,
    }
}

/// Access-log lines on stdout, tinted by status class.
pub fn status_colored(status: u16, line: &str) -> String {
    match status_sgr(status) {
        Some(code) => paint(Stream::Stdout, code, line),
        None => line.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::status_sgr;

    #[test]
    fn status_classes_map_to_their_colors() {
        assert_eq!(status_sgr(200), Some("32")); // green
        assert_eq!(status_sgr(299), Some("32"));
        assert_eq!(status_sgr(301), Some("33")); // yellow
        assert_eq!(status_sgr(404), Some("31")); // maroon
        assert_eq!(status_sgr(500), Some("91")); // bright red
        assert_eq!(status_sgr(599), Some("91"));
    }

    #[test]
    fn out_of_class_statuses_stay_plain() {
        assert_eq!(status_sgr(100), None);
        assert_eq!(status_sgr(199), None);
        assert_eq!(status_sgr(600), None);
    }
}
