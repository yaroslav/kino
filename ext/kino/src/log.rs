//! Server log lines: lifecycle notices, crashes and respawns, hook
//! failures, the failed-request report, and whatever apps write to
//! rack.errors, all in one shape:
//!
//! ```text
//! kino[4213] worker-3: after_worker_boot hook raised RuntimeError: boom
//! ```
//!
//! The label is syslog's `ident[pid]` tag plus the source that spoke (the
//! ractor and/or thread name, `main` for neither), styled by level; the
//! message stays plain. Ruby builds the source, since only Ruby knows its
//! ractor and thread names, and hands the rest over; this side decides
//! color per stream and writes, so worker ractors never touch $stdout or
//! $stderr themselves. A multi-line message is labelled on its first line.

use std::io::Write;

use magnus::{Error, Ruby};

use crate::style::{self, Stream};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Level {
    Info,
    Warn,
    Error,
}

impl Level {
    /// The level named by Ruby ("info", "warn", "error").
    pub fn parse(name: &str) -> Option<Level> {
        match name {
            "info" => Some(Level::Info),
            "warn" => Some(Level::Warn),
            "error" => Some(Level::Error),
            _ => None,
        }
    }

    /// Notes go to stdout; warnings and errors to stderr.
    fn stream(self) -> Stream {
        match self {
            Level::Info => Stream::Stdout,
            Level::Warn | Level::Error => Stream::Stderr,
        }
    }

    fn sgr(self) -> &'static str {
        match self {
            Level::Info => style::DIM,
            Level::Warn => style::WARN,
            Level::Error => style::ERROR,
        }
    }
}

/// Write one line from `source` at `level`.
pub fn emit(level: Level, source: &str, message: &str) {
    let stream = level.stream();
    let label = label(std::process::id(), source);
    let line = format_line(level, &label, message, style::enabled(stream));
    match stream {
        Stream::Stdout => {
            let _ = writeln!(std::io::stdout().lock(), "{line}");
        }
        Stream::Stderr => {
            let _ = writeln!(std::io::stderr().lock(), "{line}");
        }
    }
}

/// The `kino[<pid>] <source>:` tag.
pub fn label(pid: u32, source: &str) -> String {
    format!("kino[{pid}] {source}:")
}

/// The label styled by level, then the message as given.
pub fn format_line(level: Level, label: &str, message: &str, color: bool) -> String {
    format!("{} {message}", style::sgr(level.sgr(), label, color))
}

/// The Ruby entry point (Kino::Log): Ruby knows its ractor and thread,
/// the native side knows the terminal.
pub fn log_line(ruby: &Ruby, level: String, source: String, message: String) -> Result<(), Error> {
    let level = Level::parse(&level).ok_or_else(|| {
        Error::new(
            ruby.exception_arg_error(),
            format!("unknown log level {level:?}"),
        )
    })?;
    emit(level, &source, &message);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{format_line, Level};

    // The label, a plain line and a report's, each on its level's stream: spec/log_spec.rb, which
    // sees no color.
    #[test]
    fn color_styles_only_the_label_by_level() {
        let rows = [
            (Level::Info, "\x1b[90mkino[1] main:\x1b[0m hello"),
            (Level::Warn, "\x1b[33mkino[1] main:\x1b[0m hello"),
            (Level::Error, "\x1b[91mkino[1] main:\x1b[0m hello"),
        ];
        for (level, line) in rows {
            assert_eq!(format_line(level, "kino[1] main:", "hello", true), line);
        }
    }

    #[test]
    fn a_level_kino_log_never_names_is_refused() {
        assert_eq!(Level::parse("debug"), None);
    }
}
