//! The access log: two records per request on the async sink, so neither
//! costs the request path a write syscall. An arrival line is queued as
//! soon as the request head is parsed, before the app sees it, so a hang
//! shows as an arrow with no answer; a status-tinted completion line
//! follows the response head with the total and a timing breakdown:
//!
//! ```text
//! 2026-08-22 14:03:11 +0300 → GET /users?q=1  from 127.0.0.1
//! 2026-08-22 14:03:11 +0300 ← 200 GET /users?q=1  12.4ms (ruby 9.1ms [gc 0.8ms; 1.5k obj]; kino 3.2ms; wait 0.1ms)
//! ```
//!
//! `ruby` is the time the request spent in a Ruby worker (admit to
//! response head), with the GC pause and objects allocated during the app
//! call when the worker measured them; `kino` is the server's own
//! overhead (total minus ruby minus wait); `wait` is the queue time before
//! a worker took the request. A blank line sets one request apart from
//! the next.

use std::fmt::Write as _;
use std::net::IpAddr;
use std::time::Duration;

use crate::logsink::Sink;
use crate::style::{self, Stream};

/// Per-request timing, measured on the way through and riding the
/// response as an extension so the intake side can log it.
#[derive(Clone, Copy, Debug)]
pub struct Timing {
    /// Queue wait before a worker took the request.
    pub wait: Duration,
    /// Admit to response head: the request's time in Ruby.
    pub ruby: Duration,
    /// GC pause and objects allocated during the app call, when measured.
    /// Left out where the VM's process-wide counters cannot be attributed
    /// to one request (parallel ractors).
    pub gc: Option<(Duration, u64)>,
}

/// A request's two records in the access log, whichever way it is served: made as its arrival
/// line is queued, spent on its completion line.
pub struct Logged {
    method: String,
    target: String,
    start: std::time::Instant,
}

impl Logged {
    /// Queues the arrival line on `log` now, before the app sees the request, so a hang shows
    /// as an arrow with no answer.
    pub fn arrival(log: &Sink, method: &http::Method, uri: &http::Uri, from: IpAddr) -> Logged {
        let target = match uri.query() {
            Some(query) => format!("{}?{query}", uri.path()),
            None => uri.path().to_string(),
        };
        let method = method.to_string();
        log.write_line(arrival(&method, &target, from));
        Logged {
            method,
            target,
            start: std::time::Instant::now(),
        }
    }

    /// Since its arrival: its total, once its answer begins.
    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }

    /// Queues the completion line, with the breakdown where a worker measured `timing`.
    /// A thread-safe free may run it (an inline give-up's 500), so it keeps typed_data.rs's
    /// promise: it formats and queues, no more.
    pub fn completion(self, log: &Sink, status: u16, total: Duration, timing: Option<Timing>) {
        let line = completion(status, &self.method, &self.target, total, timing);
        log.write_line(line);
    }
}

/// The arrival record, stamped and styled for stdout.
pub fn arrival(method: &str, target: &str, ip: IpAddr) -> String {
    let color = style::enabled(Stream::Stdout);
    let record = style::sgr(style::BOLD_WHITE, &arrival_line(method, target, ip), color);
    stamped(&record, color)
}

/// The completion record, stamped, tinted by status class, followed by the
/// dimmed breakdown when timing was measured (a 503 or 504 never reached a
/// worker, so it has none). Ends with an extra newline: with the sink's
/// own, that leaves the blank line after the record.
pub fn completion(
    status: u16,
    method: &str,
    target: &str,
    total: Duration,
    timing: Option<Timing>,
) -> String {
    let color = style::enabled(Stream::Stdout);
    let record = completion_line(status, method, target, total);
    let record = match style::status_sgr(status) {
        Some(code) => style::sgr(code, &record, color),
        None => record,
    };
    let mut line = stamped(&record, color);
    if let Some(timing) = timing {
        line.push(' ');
        if color {
            let _ = write!(line, "\x1b[{}m", style::DIM);
        }
        write_breakdown(&mut line, total, &timing);
        if color {
            line.push_str("\x1b[0m");
        }
    }
    line.push('\n');
    line
}

/// Prefix a record with the local timestamp, dimmed when coloring so it
/// recedes behind the arrow and status.
fn stamped(record: &str, color: bool) -> String {
    format!("{} {record}", style::sgr(style::DIM, &now_stamp(), color))
}

/// The local wall clock as `2026-08-22 14:03:11 +0300`: date, time, and
/// the numeric UTC offset, so a log file is unambiguous across machines.
fn now_stamp() -> String {
    jiff::Zoned::now()
        .strftime("%Y-%m-%d %H:%M:%S %z")
        .to_string()
}

/// `→ METHOD target  from IP`.
fn arrival_line(method: &str, target: &str, ip: IpAddr) -> String {
    format!("\u{2192} {method} {target}  from {ip}")
}

/// `← STATUS METHOD target  N.Nms`.
fn completion_line(status: u16, method: &str, target: &str, total: Duration) -> String {
    format!(
        "\u{2190} {status} {method} {target}  {:.1}ms",
        millis(total)
    )
}

/// Append the breakdown straight into the line buffer (no string of its
/// own): `(ruby N.Nms [gc N.Nms; N obj]; kino N.Nms; wait N.Nms)`, the gc
/// bracket only when measured. `kino` floors at zero: at sub-millisecond
/// totals clock granularity can make the parts exceed the whole.
fn write_breakdown(out: &mut String, total: Duration, timing: &Timing) {
    let kino = total
        .saturating_sub(timing.ruby)
        .saturating_sub(timing.wait);
    let _ = write!(out, "(ruby {:.1}ms", millis(timing.ruby));
    if let Some((gc, allocs)) = timing.gc {
        let _ = write!(out, " [gc {:.1}ms; ", millis(gc));
        write_count(out, allocs);
        out.push_str(" obj]");
    }
    let _ = write!(
        out,
        "; kino {:.1}ms; wait {:.1}ms)",
        millis(kino),
        millis(timing.wait)
    );
}

fn millis(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

/// A humanized count: `523`, `1.5k`, `52k`.
fn write_count(out: &mut String, n: u64) {
    if n < 1000 {
        let _ = write!(out, "{n}");
    } else if n < 10_000 {
        let _ = write!(out, "{:.1}k", n as f64 / 1000.0);
    } else {
        let _ = write!(out, "{}k", n / 1000);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{write_breakdown, write_count, Timing};

    // The records' shape, their stamp and the blank line after a completion:
    // spec/logging_spec.rb. What the breakdown's numbers are is checked here.
    #[test]
    fn a_breakdown_splits_the_total_into_ruby_kino_and_wait_with_the_gc_bracket_when_measured() {
        let us = Duration::from_micros;
        // The total, wait, ruby and GC pause in microseconds with the objects allocated, and the
        // breakdown: kino is what is left, floored at zero. One row per case; rustfmt would split
        // each row field by field.
        #[rustfmt::skip]
        let rows = [
            (45_200, 100, 41_000, Some((9_700, 52_000)), "(ruby 41.0ms [gc 9.7ms; 52k obj]; kino 4.1ms; wait 0.1ms)"),
            (4_000, 1_000, 2_000, None, "(ruby 2.0ms; kino 1.0ms; wait 1.0ms)"),
            (40_000, 1_000, 40_000, Some((0, 10)), "(ruby 40.0ms [gc 0.0ms; 10 obj]; kino 0.0ms; wait 1.0ms)"),
        ];
        for (total, wait, ruby, gc, expected) in rows {
            let gc = gc.map(|(pause, objects)| (us(pause), objects));
            let timing = Timing {
                wait: us(wait),
                ruby: us(ruby),
                gc,
            };
            let mut breakdown = String::new();
            write_breakdown(&mut breakdown, us(total), &timing);
            assert_eq!(breakdown, expected);
        }
    }

    #[test]
    fn count_humanizes_allocation_counts() {
        for (n, humanized) in [(0, "0"), (523, "523"), (1500, "1.5k"), (52_000, "52k")] {
            let mut count = String::new();
            write_count(&mut count, n);
            assert_eq!(count, humanized);
        }
    }
}
