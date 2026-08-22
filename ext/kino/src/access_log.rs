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
    use super::{arrival_line, completion_line, now_stamp, write_breakdown, write_count, Timing};
    use std::net::{IpAddr, Ipv4Addr};
    use std::time::Duration;

    fn breakdown(total: Duration, timing: &Timing) -> String {
        let mut out = String::new();
        write_breakdown(&mut out, total, timing);
        out
    }

    fn count(n: u64) -> String {
        let mut out = String::new();
        write_count(&mut out, n);
        out
    }

    #[test]
    fn arrival_line_carries_method_target_and_ip() {
        let ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 5));
        assert_eq!(
            arrival_line("GET", "/users?q=1", ip),
            "\u{2192} GET /users?q=1  from 203.0.113.5"
        );
    }

    #[test]
    fn completion_line_carries_status_target_and_total() {
        assert_eq!(
            completion_line(200, "GET", "/users", Duration::from_millis(12)),
            "\u{2190} 200 GET /users  12.0ms"
        );
    }

    #[test]
    fn breakdown_splits_the_total_into_ruby_kino_and_wait() {
        let timing = Timing {
            wait: Duration::from_micros(100),
            ruby: Duration::from_millis(41),
            gc: Some((Duration::from_micros(9_700), 52_000)),
        };
        // kino = total - ruby - wait = 45.2 - 41.0 - 0.1 = 4.1ms.
        assert_eq!(
            breakdown(Duration::from_micros(45_200), &timing),
            "(ruby 41.0ms [gc 9.7ms; 52k obj]; kino 4.1ms; wait 0.1ms)"
        );
    }

    #[test]
    fn breakdown_leaves_out_the_gc_bracket_when_not_measured() {
        let timing = Timing {
            wait: Duration::from_millis(1),
            ruby: Duration::from_millis(2),
            gc: None,
        };
        assert_eq!(
            breakdown(Duration::from_millis(4), &timing),
            "(ruby 2.0ms; kino 1.0ms; wait 1.0ms)"
        );
    }

    #[test]
    fn breakdown_floors_kino_at_zero() {
        let timing = Timing {
            wait: Duration::from_millis(1),
            ruby: Duration::from_millis(40),
            gc: Some((Duration::ZERO, 10)),
        };
        assert_eq!(
            breakdown(Duration::from_millis(40), &timing),
            "(ruby 40.0ms [gc 0.0ms; 10 obj]; kino 0.0ms; wait 1.0ms)"
        );
    }

    #[test]
    fn count_humanizes_allocation_counts() {
        assert_eq!(count(0), "0");
        assert_eq!(count(523), "523");
        assert_eq!(count(1500), "1.5k");
        assert_eq!(count(52_000), "52k");
    }

    #[test]
    fn timestamp_is_date_time_and_numeric_offset() {
        // `YYYY-MM-DD HH:MM:SS +HHMM`, e.g. `2026-08-22 14:03:11 +0300`.
        let ts = now_stamp();
        let (datetime, offset) = ts.rsplit_once(' ').expect("offset field");
        let (date, time) = datetime.split_once(' ').expect("date and time");
        assert_eq!(date.len(), 10, "date {date}");
        assert_eq!(&date[4..5], "-");
        assert_eq!(time.len(), 8, "time {time}");
        assert_eq!(&time[2..3], ":");
        assert_eq!(offset.len(), 5, "offset {offset}");
        assert!(matches!(&offset[0..1], "+" | "-"));
        assert!(offset[1..].bytes().all(|b| b.is_ascii_digit()));
    }

    #[test]
    fn completion_ends_with_the_blank_line_newline() {
        let line = super::completion(200, "GET", "/", Duration::from_millis(1), None);
        assert!(line.ends_with("\u{2190} 200 GET /  1.0ms\n") || line.ends_with("1.0ms\x1b[0m\n"));
    }
}
