//! Live KLV injection: a live stream in, MISB ST 0601 metadata from a
//! control channel put into it as its video frames pass, out at once.
//!
//! [`run`] reads an input as it arrives (a socket, a pipe), hands each
//! chunk to a [`LiveMux`] with the wall-clock MISP time of its arrival, and
//! sends what comes out straight away: nothing is paced or buffered here,
//! and the muxer adds no delay of its own (with video timestamps off), so
//! the output leaves as soon as the input arrives. Control lines
//! ([`crate::control`]) are applied between reads, each one whole, so a
//! change shows on the next video frame and a packet never mixes two
//! updates. A file input is read as fast as possible; to inject into a
//! file in real time, play it through `tsflow play ... -`.

use crate::clock::Clock;
use crate::control::{Control, ControlLine};
use crate::input::{Input, ReadEvent};
use crate::output::Sink;
use crate::pacer::TICKS_PER_SECOND;
use crate::recv::StopReason;
use crate::{Error, Result, PACKET_SIZE};
use misb::mux::{LiveMux, LiveMuxStats};
use misb::{LeapSeconds, MispTime};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime};

const POLL: Duration = Duration::from_millis(100);

/// Settings of [`run`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct LiveConfig {
    /// TS packets per datagram written (more are split, fewer go as they
    /// are: nothing waits to fill a datagram).
    pub burst: usize,
    /// Stop once no input has arrived for this long, counted from the
    /// first datagram.
    pub idle_exit: Option<Duration>,
    /// Stop after this long.
    pub duration: Option<Duration>,
    /// Stop once at least this many input packets have arrived.
    pub max_packets: Option<u64>,
    /// How often the status callback runs.
    pub status_interval: Duration,
}

impl Default for LiveConfig {
    fn default() -> Self {
        LiveConfig {
            burst: crate::endpoint::DEFAULT_PACKETS_PER_DATAGRAM,
            idle_exit: None,
            duration: None,
            max_packets: None,
            status_interval: Duration::from_secs(1),
        }
    }
}

/// MISP time read from a monotonic [`Clock`]: the MISP time at one instant
/// plus the time elapsed since, so a run is never affected by the system
/// clock being set, and tests can drive it with a simulated clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MispClock {
    origin: Instant,
    misp: MispTime,
}

impl MispClock {
    /// MISP time `misp` at instant `origin`.
    #[must_use]
    pub fn new(origin: Instant, misp: MispTime) -> MispClock {
        MispClock { origin, misp }
    }

    /// The system's UTC time now, converted to MISP time with the leap
    /// second table, at `clock`'s current instant.
    pub fn system(clock: &dyn Clock) -> Result<MispClock> {
        let misp = MispTime::from_system_time(SystemTime::now(), LeapSeconds::builtin())
            .ok_or_else(|| Error::Config("the system clock is before 1970".into()))?;
        Ok(MispClock::new(clock.now(), misp))
    }

    /// MISP time at instant `t` (microseconds, rounded down).
    #[must_use]
    pub fn at(&self, t: Instant) -> MispTime {
        let us = t.saturating_duration_since(self.origin).as_micros();
        MispTime::from_micros(self.misp.as_micros().saturating_add(u64::try_from(us).unwrap_or(u64::MAX)))
    }
}

/// Counters of a live run.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct LiveStats {
    /// TS packets read.
    pub packets_in: u64,
    /// TS packets written.
    pub packets_out: u64,
    /// Datagrams (or writes) sent.
    pub datagrams: u64,
    /// Control lines applied.
    pub control_lines: u64,
    /// Control lines refused (nothing of them was applied).
    pub control_errors: u64,
    /// Time since the run started.
    pub elapsed: Duration,
    /// The muxer's counters.
    pub mux: LiveMuxStats,
}

/// What became of one control line.
pub type ControlOutcome = std::result::Result<misb::json::Applied, String>;

/// Inject live: read `input` until a stop condition, passing each chunk
/// through `mux` at its arrival time (`misp` turns `clock` instants into
/// MISP time) and sending what comes out to `output` at once. Lines from
/// `controls` are applied between reads; `on_control` hears what became of
/// each. `on_status` runs every `cfg.status_interval`. The muxer is
/// finished before returning.
#[allow(clippy::too_many_arguments)]
pub fn run(
    input: &mut Input,
    output: &mut dyn Sink,
    mux: &mut LiveMux,
    controls: &[Control],
    cfg: &LiveConfig,
    clock: &dyn Clock,
    misp: MispClock,
    stop: &AtomicBool,
    on_status: &mut dyn FnMut(&LiveStats),
    on_control: &mut dyn FnMut(&ControlLine, &ControlOutcome),
) -> Result<(StopReason, LiveStats)> {
    let start = clock.now();
    let interval = cfg.status_interval.max(Duration::from_millis(1));
    let burst = cfg.burst.max(1) * PACKET_SIZE;
    let mut next_status = start + interval;
    let mut last_data: Option<Instant> = None;
    let mut stats = LiveStats::default();
    let mut buf = Vec::with_capacity(64 * PACKET_SIZE);
    let reason = loop {
        if stop.load(Ordering::Relaxed) {
            break StopReason::Interrupted;
        }
        for control in controls {
            for line in control.try_lines() {
                let outcome = apply(mux, &line, misp.at(clock.now()));
                match outcome {
                    Ok(_) => stats.control_lines += 1,
                    Err(_) => stats.control_errors += 1,
                }
                on_control(&line, &outcome);
            }
        }
        let now = clock.now();
        let mut deadline = next_status.min(now + POLL);
        if let Some(d) = cfg.duration {
            if now.saturating_duration_since(start) >= d {
                break StopReason::Duration;
            }
            deadline = deadline.min(start + d);
        }
        if let (Some(idle), Some(last)) = (cfg.idle_exit, last_data) {
            if now.saturating_duration_since(last) >= idle {
                break StopReason::Idle;
            }
            deadline = deadline.min(last + idle);
        }
        if now >= next_status {
            stats.elapsed = now.saturating_duration_since(start);
            stats.mux = mux.stats();
            on_status(&stats);
            while next_status <= now {
                next_status += interval;
            }
            continue;
        }
        match input.read(deadline.saturating_duration_since(now))? {
            ReadEvent::Data(chunk) => {
                let arrival = clock.now();
                last_data = Some(arrival);
                stats.packets_in += (chunk.packets.len() / PACKET_SIZE) as u64;
                let r = mux.push(chunk.packets, misp.at(arrival), &mut buf);
                send(output, &mut buf, burst, position(arrival, start), &mut stats)?;
                r.map_err(|e| Error::Inject(e.to_string()))?;
                if cfg.max_packets.is_some_and(|max| stats.packets_in >= max) {
                    break StopReason::Count;
                }
            }
            ReadEvent::Idle => {}
            ReadEvent::End => break StopReason::EndOfInput,
        }
    };
    let r = mux.finish(&mut buf);
    send(output, &mut buf, burst, position(clock.now(), start), &mut stats)?;
    r.map_err(|e| Error::Inject(e.to_string()))?;
    output.flush().map_err(|e| Error::io("write the output", e))?;
    stats.elapsed = clock.now().saturating_duration_since(start);
    stats.mux = mux.stats();
    Ok((reason, stats))
}

/// One control line onto the muxer's state.
fn apply(mux: &mut LiveMux, line: &ControlLine, at: MispTime) -> ControlOutcome {
    let text = line.text.as_ref().map_err(Clone::clone)?;
    mux.apply_line(text, at).map_err(|e| e.to_string())
}

/// 27 MHz since the start, the position an RTP output stamps.
fn position(t: Instant, start: Instant) -> u64 {
    let since = t.saturating_duration_since(start);
    (since.as_nanos() * u128::from(TICKS_PER_SECOND) / 1_000_000_000) as u64
}

/// Send what the muxer wrote, `burst` bytes per datagram at most.
fn send(output: &mut dyn Sink, buf: &mut Vec<u8>, burst: usize, position: u64, stats: &mut LiveStats) -> Result<()> {
    for group in buf.chunks(burst) {
        output.send(group, position).map_err(|e| Error::io("send the output", e))?;
        stats.datagrams += 1;
        stats.packets_out += (group.len() / PACKET_SIZE) as u64;
    }
    buf.clear();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::ManualClock;

    #[test]
    fn misp_clock_counts_from_its_origin() {
        let clock = ManualClock::new();
        let m = MispClock::new(clock.now(), MispTime::from_micros(1_000));
        clock.advance(Duration::from_micros(2_500));
        assert_eq!(m.at(clock.now()), MispTime::from_micros(3_500));
        // Before the origin: the origin's time.
        assert_eq!(m.at(clock.now() - Duration::from_secs(1)), MispTime::from_micros(1_000));
    }
}
