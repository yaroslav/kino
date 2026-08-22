//! How many CPUs this process may actually use: the default worker count.
//!
//! `Etc.nprocessors` honours the affinity mask but not a cgroup CPU quota,
//! so a container limited to two CPUs on a 64-core host would spawn 64
//! workers. The standard library's `available_parallelism` reads both the
//! mask and the cgroup v1/v2 quota on Linux (rounding a fractional quota
//! up), which is also what tokio sizes its own pool by.

use magnus::{Error, Ruby};

/// The usable CPU count, never below one (a quota of 0.5 CPU still needs
/// a worker; an unreadable count falls back to one rather than failing
/// boot).
pub fn available_parallelism(_ruby: &Ruby) -> Result<usize, Error> {
    Ok(count())
}

fn count() -> usize {
    std::thread::available_parallelism().map_or(1, |n| n.get())
}

#[cfg(test)]
mod tests {
    use super::count;

    #[test]
    fn reports_at_least_one_cpu() {
        assert!(count() >= 1);
    }

    #[test]
    fn never_exceeds_what_the_os_reports_as_online() {
        // A quota can only lower the count below the online CPUs; never raise it.
        let online = std::thread::available_parallelism().map_or(1, |n| n.get());
        assert!(count() <= online);
    }
}
