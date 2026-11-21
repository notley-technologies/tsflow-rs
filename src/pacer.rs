//! Real-time pacing: sending each packet at its time on the program clock.
//!
//! A [`PcrTimeline`](mpegts::PcrTimeline) gives every packet a position on
//! a continuous 27 MHz time line (interpolated between PCRs, continuous
//! across discontinuities and file loops). A [`Schedule`] maps positions
//! onto the wall clock with a fixed delay, the latency:
//!
//! ```text
//! due(position) = anchor_time + (position - anchor_position) / 27 MHz
//! ```
//!
//! where the anchor is the first packet, due `latency` after it arrived. So
//! a PCR packet leaves exactly at its PCR time on the output's clock, and
//! the packets between PCRs at the times the transport rate gives them
//! (ISO/IEC 13818-1 2.4.2.2). The arithmetic is integer nanoseconds from
//! the anchor, never accumulated, so hours of playback do not drift.
//!
//! The latency absorbs input jitter. If a packet arrives so late that it
//! would be due more than `max_late` ago (the input stalled), or so early
//! that it would wait more than `latency + max_early` (the input runs
//! ahead of real time), the schedule re-anchors on that packet.
//!
//! A [`Pacer`] adds the queue and the datagram grouping: up to `burst`
//! packets per datagram, each datagram sent when its last packet is due,
//! the way a network encapsulator sends a datagram once it is full, so no
//! packet ever leaves before its time. A packet is held at most `max_hold`
//! waiting for others to fill its datagram, which bounds the lateness this
//! adds at low bitrates and flushes a partial datagram when input pauses.

use crate::endpoint::MAX_PACKETS_PER_DATAGRAM;
use crate::{Error, Result, PACKET_SIZE};
use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// Ticks of the 27 MHz program clock per second (ISO/IEC 13818-1 2.4.2.1).
pub const TICKS_PER_SECOND: u64 = 27_000_000;

/// 27 MHz ticks as a duration, exactly (to the nanosecond, rounded down).
///
/// ```
/// use std::time::Duration;
/// assert_eq!(tsflow::pacer::ticks_to_duration(27), Duration::from_nanos(1000));
/// assert_eq!(tsflow::pacer::ticks_to_duration(27_000_000 * 86_400), Duration::from_secs(86_400));
/// ```
pub fn ticks_to_duration(ticks: u64) -> Duration {
    let rem = ticks % TICKS_PER_SECOND;
    // rem * 1000 < 2.7e10 and the quotient < 1e9: no overflow.
    Duration::new(ticks / TICKS_PER_SECOND, (rem * 1000 / 27) as u32)
}

/// Longest latency accepted: 10 minutes.
pub const MAX_LATENCY: Duration = Duration::from_secs(600);

/// Settings of a [`Pacer`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct PacerConfig {
    /// Delay between a packet's arrival and its departure, the buffer that
    /// absorbs input jitter. It must exceed the input's PCR interval (the
    /// timeline holds one interval) plus the network jitter.
    pub latency: Duration,
    /// Re-anchor when a packet would already be due by more than this when
    /// it arrives.
    pub max_late: Duration,
    /// Re-anchor when a packet would wait more than `latency` plus this.
    /// `None` for an input read on demand (a file), which is never early.
    pub max_early: Option<Duration>,
    /// Most TS packets per datagram.
    pub burst: usize,
    /// Longest a packet waits for others to fill its datagram.
    pub max_hold: Duration,
    /// Most packets queued; beyond it the oldest are dropped (counted).
    /// Bounds memory if the schedule cannot keep up.
    pub max_queue: usize,
}

impl Default for PacerConfig {
    fn default() -> Self {
        PacerConfig::new(Duration::from_millis(200))
    }
}

impl PacerConfig {
    /// Defaults around a latency: `max_late` half of it (at least 20 ms),
    /// `max_early` 1 s, 7 packets per datagram, `max_hold` 10 ms, at most
    /// 200,000 packets (37.6 MB) queued.
    pub fn new(latency: Duration) -> PacerConfig {
        PacerConfig {
            latency,
            max_late: (latency / 2).max(Duration::from_millis(20)),
            max_early: Some(Duration::from_secs(1)),
            burst: crate::endpoint::DEFAULT_PACKETS_PER_DATAGRAM,
            max_hold: Duration::from_millis(10),
            max_queue: 200_000,
        }
    }

    fn validate(&self) -> Result<()> {
        if self.latency > MAX_LATENCY {
            return Err(Error::config(format!("latency must be at most {} s", MAX_LATENCY.as_secs())));
        }
        let day = Duration::from_secs(86_400);
        if self.max_late > day || self.max_early.is_some_and(|e| e > day) || self.max_hold > Duration::from_secs(1) {
            return Err(Error::config("max_late and max_early must be at most a day, max_hold at most 1 s"));
        }
        if !(1..=MAX_PACKETS_PER_DATAGRAM).contains(&self.burst) {
            return Err(Error::config(format!(
                "packets per datagram must be 1 to {MAX_PACKETS_PER_DATAGRAM}, not {}",
                self.burst
            )));
        }
        if self.max_queue < self.burst {
            return Err(Error::config("the pacer queue must hold at least one datagram"));
        }
        Ok(())
    }
}

/// Maps stream positions onto the wall clock (see the [module
/// documentation](self)).
#[derive(Debug, Clone)]
pub struct Schedule {
    latency: Duration,
    max_late: Duration,
    max_early: Option<Duration>,
    anchor: Option<(u64, Instant)>,
    last_due: Option<Instant>,
    reanchors: u64,
}

impl Schedule {
    /// A schedule with no anchor yet.
    pub fn new(latency: Duration, max_late: Duration, max_early: Option<Duration>) -> Schedule {
        Schedule { latency, max_late, max_early, anchor: None, last_due: None, reanchors: 0 }
    }

    /// The due time of the packet at `position` (27 MHz ticks) that
    /// arrives at `now`, and whether the schedule re-anchored on it. Due
    /// times never go backwards.
    pub fn due(&mut self, position: u64, now: Instant) -> (Instant, bool) {
        let anchor = *self.anchor.get_or_insert((position, now + self.latency));
        let mut due = map(anchor, position);
        let late = due.checked_add(self.max_late).is_some_and(|d| d < now);
        let early = self.max_early.is_some_and(|e| due > now + self.latency + e);
        if late || early {
            self.anchor = Some((position, now + self.latency));
            self.reanchors += 1;
            self.last_due = None;
            due = now + self.latency;
        }
        if let Some(last) = self.last_due {
            due = due.max(last);
        }
        self.last_due = Some(due);
        (due, late || early)
    }

    /// The time `position` maps to under the current anchor, without any
    /// check (`None` before the first packet).
    pub fn at(&self, position: u64) -> Option<Instant> {
        self.anchor.map(|a| map(a, position))
    }

    /// The current anchor: a position and its due time.
    pub fn anchor(&self) -> Option<(u64, Instant)> {
        self.anchor
    }

    /// Times the schedule re-anchored.
    pub fn reanchors(&self) -> u64 {
        self.reanchors
    }
}

fn map((p0, t0): (u64, Instant), position: u64) -> Instant {
    // A position beyond what the system's Instant can hold (a hostile
    // stream claiming millennia) maps to the anchor instead of panicking;
    // due times are kept in order by the caller.
    if position >= p0 {
        t0.checked_add(ticks_to_duration(position - p0)).unwrap_or(t0)
    } else {
        t0.checked_sub(ticks_to_duration(p0 - position)).unwrap_or(t0)
    }
}

#[derive(Debug, Clone)]
struct Queued {
    packet: [u8; PACKET_SIZE],
    position: u64,
    due: Instant,
}

/// The next datagram a [`Pacer`] wants sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Departure {
    /// Packets in it.
    pub count: usize,
    /// When to send it.
    pub at: Instant,
}

/// A queue of packets with due times, grouped into datagrams (see the
/// [module documentation](self)).
///
/// ```
/// use std::time::Duration;
/// use tsflow::pacer::{Pacer, PacerConfig};
///
/// let mut pacer = Pacer::new(PacerConfig::new(Duration::from_millis(100)))?;
/// let t0 = std::time::Instant::now();
/// let packet = [0x47u8; 188];
/// for i in 0..7 {
///     pacer.push(&packet, i * 27_000, t0); // one packet per millisecond
/// }
/// let d = pacer.next(true).unwrap();
/// assert_eq!(d.count, 7);
/// assert_eq!(d.at, t0 + Duration::from_millis(106)); // when the 7th is due
/// # Ok::<(), tsflow::Error>(())
/// ```
#[derive(Debug, Clone)]
pub struct Pacer {
    cfg: PacerConfig,
    schedule: Schedule,
    queue: VecDeque<Queued>,
    dropped: u64,
}

impl Pacer {
    /// An empty pacer.
    pub fn new(cfg: PacerConfig) -> Result<Pacer> {
        cfg.validate()?;
        let schedule = Schedule::new(cfg.latency, cfg.max_late, cfg.max_early);
        Ok(Pacer { cfg, schedule, queue: VecDeque::new(), dropped: 0 })
    }

    /// Queue a packet at stream `position` that arrived at `now`.
    pub fn push(&mut self, packet: &[u8; PACKET_SIZE], position: u64, now: Instant) {
        let (due, reanchored) = self.schedule.due(position, now);
        if reanchored {
            // Keep the queue on the new schedule, so a backlog drains at once
            // after the input ran ahead instead of piling up.
            for q in &mut self.queue {
                q.due = self.schedule.at(q.position).unwrap_or(due).min(due);
            }
        }
        if self.queue.len() >= self.cfg.max_queue {
            self.queue.pop_front();
            self.dropped += 1;
        }
        self.queue.push_back(Queued { packet: *packet, position, due });
    }

    /// The next datagram to send, if any. `input_open` false means no more
    /// packets will come, so a partial datagram need not wait.
    pub fn next(&self, input_open: bool) -> Option<Departure> {
        let first = self.queue.front()?;
        let limit = first.due.checked_add(self.cfg.max_hold).unwrap_or(first.due);
        let avail = self.queue.len().min(self.cfg.burst);
        let within = self.queue.iter().take(avail).take_while(|q| q.due <= limit).count().max(1);
        let last_due = self.queue[within - 1].due;
        if within == self.cfg.burst || within < avail || !input_open {
            // Full; or the next packet is beyond the hold limit; or the end.
            Some(Departure { count: within, at: last_due })
        } else {
            // Partial: wait for more until the hold limit.
            Some(Departure { count: within, at: limit })
        }
    }

    /// Remove `count` packets into `out` (cleared first). Returns the
    /// position of the first.
    pub fn pop(&mut self, count: usize, out: &mut Vec<u8>) -> Option<u64> {
        out.clear();
        let first = self.queue.front()?.position;
        for q in self.queue.drain(..count.min(self.queue.len())) {
            out.extend_from_slice(&q.packet);
        }
        Some(first)
    }

    /// Packets queued.
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// Nothing queued.
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// How far ahead of `now` the newest queued packet is due: the current
    /// buffer depth.
    pub fn depth(&self, now: Instant) -> Duration {
        self.queue.back().map_or(Duration::ZERO, |q| q.due.saturating_duration_since(now))
    }

    /// The schedule.
    pub fn schedule(&self) -> &Schedule {
        &self.schedule
    }

    /// Packets dropped because the queue was full.
    pub fn dropped(&self) -> u64 {
        self.dropped
    }

    /// The settings.
    pub fn config(&self) -> &PacerConfig {
        &self.cfg
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: u64 = 27_000;

    fn schedule(latency_ms: u64, early: Option<u64>) -> Schedule {
        let latency = Duration::from_millis(latency_ms);
        Schedule::new(latency, latency / 2, early.map(Duration::from_millis))
    }

    #[test]
    fn positions_map_to_due_times_exactly() {
        let mut s = schedule(200, Some(1000));
        let t0 = Instant::now();
        let (d, re) = s.due(5 * MS, t0);
        assert_eq!((d, re), (t0 + Duration::from_millis(200), false));
        // Arrival jitter does not move the due times.
        for (pos, arrive_ms) in [(6 * MS, 3), (40 * MS, 20), (1_000 * MS, 990), (1_001 * MS, 1_100)] {
            let (d, re) = s.due(pos, t0 + Duration::from_millis(arrive_ms));
            assert!(!re);
            assert_eq!(d - t0, Duration::from_millis(200) + ticks_to_duration(pos - 5 * MS));
        }
        // One tick is 37.037 ns: rounding is per packet, never accumulated.
        // Absurd positions do not overflow the Instant.
        assert!(s.at(u64::MAX).is_some());
        let (d, _) = s.due(1_001 * MS + 1, t0 + Duration::from_secs(1));
        assert_eq!(d - t0, Duration::from_nanos(200_000_000 + 996_000_000 + 37));
        assert_eq!(s.reanchors(), 0);
    }

    #[test]
    fn reanchors_when_input_falls_behind_or_runs_ahead() {
        let mut s = schedule(200, Some(1000));
        let t0 = Instant::now();
        s.due(0, t0);
        // A stall: position 100 ms arrives 600 ms later, so it would be due
        // 300 ms ago (more than latency / 2): re-anchor 200 ms ahead.
        let now = t0 + Duration::from_millis(600);
        assert_eq!(s.due(100 * MS, now), (now + Duration::from_millis(200), true));
        assert_eq!(s.anchor(), Some((100 * MS, now + Duration::from_millis(200))));
        // Within latency / 2 of being late is fine.
        let (_, re) = s.due(200 * MS, now + Duration::from_millis(390));
        assert!(!re);
        // Running ahead: 1.5 s of stream arrives at once.
        let (d, re) = s.due(1_600 * MS, now + Duration::from_millis(400));
        assert!(re);
        assert_eq!(d, now + Duration::from_millis(600));
        assert_eq!(s.reanchors(), 2);
        // A file input is never early.
        let mut s = schedule(200, None);
        s.due(0, t0);
        assert!(!s.due(3_600_000 * MS, t0).1);
    }

    #[test]
    fn no_drift_over_24_hours() {
        let mut s = schedule(100, Some(1000));
        let t0 = Instant::now();
        // A packet every 40 ms of stream time, plus an odd step that is not
        // a whole number of nanoseconds (1,234,567 ticks = 45.724... ms),
        // each arriving 100 ms before it is due.
        for step in [40 * MS, 1_234_567] {
            let mut s2 = s.clone();
            let mut pos = 0;
            let mut n = 0u64;
            let (first, _) = s2.due(0, t0);
            while pos < TICKS_PER_SECOND * 86_400 {
                pos += step;
                n += 1;
                let expected = first + ticks_to_duration(pos);
                let (d, re) = s2.due(pos, expected - Duration::from_millis(100));
                assert!(!re);
                assert_eq!(d, expected, "step {step}, packet {n}");
            }
            let total = ticks_to_duration(pos);
            assert!(total >= Duration::from_secs(86_400));
            assert_eq!(s2.at(pos).unwrap() - first, total);
        }
        // Exactly 24 h of positions is exactly 24 h of wall time.
        let (first, _) = s.due(0, t0);
        assert_eq!(s.at(TICKS_PER_SECOND * 86_400).unwrap() - first, Duration::from_secs(86_400));
    }

    fn pacer(burst: usize, hold_ms: u64) -> Pacer {
        let cfg = PacerConfig { burst, max_hold: Duration::from_millis(hold_ms), ..PacerConfig::new(Duration::ZERO) };
        Pacer::new(cfg).unwrap()
    }

    #[test]
    fn full_datagrams_leave_when_their_last_packet_is_due() {
        let mut p = pacer(7, 10);
        let t0 = Instant::now();
        let pkt = [0x47; PACKET_SIZE];
        for i in 0..20 {
            p.push(&pkt, i * MS / 2, t0); // 0.5 ms apart
        }
        let mut out = Vec::new();
        let d = p.next(true).unwrap();
        assert_eq!((d.count, d.at), (7, t0 + Duration::from_micros(3_000)));
        assert_eq!(p.pop(d.count, &mut out), Some(0));
        assert_eq!(out.len(), 7 * PACKET_SIZE);
        let d = p.next(true).unwrap();
        assert_eq!((d.count, d.at), (7, t0 + Duration::from_micros(6_500)));
        p.pop(7, &mut out);
        // Six left: a partial datagram waits for more until the hold limit.
        let d = p.next(true).unwrap();
        assert_eq!((d.count, d.at), (6, t0 + Duration::from_micros(7_000 + 10_000)));
        // At the end of the input it goes when its last packet is due.
        let d = p.next(false).unwrap();
        assert_eq!((d.count, d.at), (6, t0 + Duration::from_micros(9_500)));
    }

    #[test]
    fn sparse_packets_are_not_held_past_the_limit() {
        let mut p = pacer(7, 10);
        let t0 = Instant::now();
        let pkt = [0x47; PACKET_SIZE];
        for i in 0..7 {
            p.push(&pkt, i * 4 * MS, t0); // 4 ms apart: 7 span 24 ms
        }
        let d = p.next(true).unwrap();
        // Packets due within 10 ms of the first (0, 4, 8 ms) go together.
        assert_eq!((d.count, d.at), (3, t0 + Duration::from_millis(8)));
    }

    #[test]
    fn a_backlog_drains_after_reanchoring_and_memory_is_capped() {
        let cfg = PacerConfig { max_queue: 10, ..PacerConfig::new(Duration::from_millis(100)) };
        let mut p = Pacer::new(cfg).unwrap();
        let t0 = Instant::now();
        let pkt = [0x47; PACKET_SIZE];
        for i in 0..8 {
            p.push(&pkt, i * 100 * MS, t0); // 700 ms of stream at once: fine
        }
        // 2 s of stream arriving at the same instant runs ahead.
        p.push(&pkt, 2_000 * MS, t0);
        assert_eq!(p.schedule().reanchors(), 1);
        // The queued packets moved earlier with the schedule.
        assert!(p.queue.iter().all(|q| q.due <= t0 + Duration::from_millis(100)));
        for i in 0..5 {
            p.push(&pkt, (2_001 + i) * MS, t0);
        }
        assert_eq!((p.len(), p.dropped()), (10, 4));
        assert!(Pacer::new(PacerConfig { burst: 0, ..PacerConfig::default() }).is_err());
        assert!(Pacer::new(PacerConfig { burst: 8, max_queue: 4, ..PacerConfig::default() }).is_err());
        assert!(Pacer::new(PacerConfig::new(Duration::from_secs(601))).is_err());
    }
}
