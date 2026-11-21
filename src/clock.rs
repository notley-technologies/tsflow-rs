//! The wall clock, behind a trait so pacing can be tested without waiting.
//!
//! Everything in tsflow that reads the time or sleeps goes through a
//! [`Clock`]. Programs use [`SystemClock`]; tests use [`ManualClock`],
//! whose time only moves when it is told to (or when something sleeps on
//! it), so a 24-hour playback can be checked in milliseconds and every
//! timing assertion is exact.

use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

/// A monotonic clock that can be slept on.
pub trait Clock {
    /// The current time.
    fn now(&self) -> Instant;

    /// Return no earlier than `deadline` (at once if it has passed).
    fn sleep_until(&self, deadline: Instant);
}

/// The system's monotonic clock and `thread::sleep`.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn sleep_until(&self, deadline: Instant) {
        let now = Instant::now();
        if deadline > now {
            thread::sleep(deadline - now);
        }
    }
}

/// A simulated clock. It starts at an arbitrary instant and advances only
/// through [`advance`](Self::advance) and [`Clock::sleep_until`], which
/// jumps straight to the deadline. Shareable between threads.
///
/// ```
/// use std::time::Duration;
/// use tsflow::clock::{Clock, ManualClock};
///
/// let clock = ManualClock::new();
/// let t0 = clock.now();
/// clock.sleep_until(t0 + Duration::from_secs(3600));
/// assert_eq!(clock.now() - t0, Duration::from_secs(3600));
/// ```
#[derive(Debug)]
pub struct ManualClock {
    origin: Instant,
    elapsed_ns: AtomicU64,
}

impl Default for ManualClock {
    fn default() -> Self {
        ManualClock::new()
    }
}

impl ManualClock {
    /// A clock reading an arbitrary starting instant.
    pub fn new() -> ManualClock {
        ManualClock { origin: Instant::now(), elapsed_ns: AtomicU64::new(0) }
    }

    /// Move the time forward.
    pub fn advance(&self, by: Duration) {
        self.elapsed_ns.fetch_add(by.as_nanos().min(u128::from(u64::MAX)) as u64, Ordering::SeqCst);
    }

    /// Time since the clock was created.
    pub fn elapsed(&self) -> Duration {
        Duration::from_nanos(self.elapsed_ns.load(Ordering::SeqCst))
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Instant {
        self.origin + self.elapsed()
    }

    fn sleep_until(&self, deadline: Instant) {
        let target = deadline.saturating_duration_since(self.origin).as_nanos().min(u128::from(u64::MAX)) as u64;
        self.elapsed_ns.fetch_max(target, Ordering::SeqCst);
    }
}

impl<C: Clock + ?Sized> Clock for &C {
    fn now(&self) -> Instant {
        (**self).now()
    }

    fn sleep_until(&self, deadline: Instant) {
        (**self).sleep_until(deadline)
    }
}
