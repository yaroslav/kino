//! ANSI styling for what the native layer writes to the terminal: the
//! access log on stdout and server log lines on either stream. (The
//! Ruby-side startup banner has its own twin in Kino::CLI.) Color
//! capability is decided once per stream; every styled string resets at
//! its end so nothing bleeds.

use std::sync::OnceLock;

#[derive(Clone, Copy)]
pub enum Stream {
    Stdout,
    Stderr,
}

/// Whether `stream` is a color terminal: a tty, no NO_COLOR, TERM not dumb.
pub fn enabled(stream: Stream) -> bool {
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

/// Wrap `text` in an SGR code ("1" bold, "91" bright red, "1;32" bold
/// green) when `color`, resetting at the end; plain otherwise.
pub fn sgr(code: &str, text: &str, color: bool) -> String {
    if color {
        format!("\x1b[{code}m{text}\x1b[0m")
    } else {
        text.to_string()
    }
}

/// Dark gray: timestamps and the timing breakdown recede behind the record.
pub const DIM: &str = "90";
/// Bold bright white: the arrival line, which has no status to color by.
pub const BOLD_WHITE: &str = "1;97";
/// Warnings are yellow.
pub const WARN: &str = "33";
/// Errors are bright red (91): the base red slot (31) is remapped to odd
/// hues by some terminal themes; 91 stays red.
pub const ERROR: &str = "91";

/// The SGR code for a status class, bold so the record leads its line
/// (basic 16-color palette only): 2xx green, 3xx yellow, 4xx maroon
/// (ANSI color 1, plain dark red), 5xx bright red, anything else uncolored.
pub fn status_sgr(status: u16) -> Option<&'static str> {
    match status {
        200..=299 => Some("1;32"),
        300..=399 => Some("1;33"),
        400..=499 => Some("1;31"),
        500..=599 => Some("1;91"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{sgr, status_sgr};

    #[test]
    fn status_classes_map_to_their_colors() {
        assert_eq!(status_sgr(200), Some("1;32")); // green
        assert_eq!(status_sgr(299), Some("1;32"));
        assert_eq!(status_sgr(301), Some("1;33")); // yellow
        assert_eq!(status_sgr(404), Some("1;31")); // maroon
        assert_eq!(status_sgr(500), Some("1;91")); // bright red
        assert_eq!(status_sgr(599), Some("1;91"));
    }

    #[test]
    fn out_of_class_statuses_stay_plain() {
        assert_eq!(status_sgr(100), None);
        assert_eq!(status_sgr(199), None);
        assert_eq!(status_sgr(600), None);
    }

    #[test]
    fn sgr_wraps_and_resets_only_when_coloring() {
        assert_eq!(sgr("1;32", "ok", true), "\x1b[1;32mok\x1b[0m");
        assert_eq!(sgr("1;32", "ok", false), "ok");
    }
}
