//! Receiving and monitoring a stream, with optional forwarding and
//! recording.
//!
//! [`run`] reads an input until it ends, goes quiet, runs for a set time or
//! is stopped, feeding a [`Monitor`]. Forwarding passes every datagram on
//! as received, before analysis, so a player can watch the stream being
//! monitored (a UDP port has one reader; VLC cannot bind the port the
//! monitor holds). Recording writes the TS packets to a file.

use crate::clock::Clock;
use crate::input::{Input, ReadEvent};
use crate::monitor::Monitor;
use crate::output::{Output, Sink};
use crate::pacer::TICKS_PER_SECOND;
use crate::{Error, Result};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

const POLL: Duration = Duration::from_millis(100);

/// Settings of [`run`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct RecvConfig {
    /// Stop once no datagram has arrived for this long, counted from the
    /// first one (so a receiver can start before its sender).
    pub idle_exit: Option<Duration>,
    /// Stop after this long, whatever arrives.
    pub duration: Option<Duration>,
    /// How often the status callback runs.
    pub status_interval: Duration,
}

impl Default for RecvConfig {
    fn default() -> Self {
        RecvConfig { idle_exit: None, duration: None, status_interval: Duration::from_secs(1) }
    }
}

/// Why [`run`] returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum StopReason {
    /// A file or pipe ended.
    EndOfInput,
    /// Nothing arrived for the idle time.
    Idle,
    /// The configured duration passed.
    Duration,
    /// The stop flag was set (Ctrl-C).
    Interrupted,
}

impl StopReason {
    /// A short description.
    pub fn describe(self) -> &'static str {
        match self {
            StopReason::EndOfInput => "end of input",
            StopReason::Idle => "input went quiet",
            StopReason::Duration => "duration reached",
            StopReason::Interrupted => "interrupted",
        }
    }
}

/// Receive from `input` into `monitor` until a stop condition, passing
/// every datagram to `forward` and every packet to `record`. `on_status`
/// runs every `cfg.status_interval` with the monitor (its input counters
/// already updated) and the current time. The monitor is finished before
/// returning.
#[allow(clippy::too_many_arguments)]
pub fn run(
    input: &mut Input,
    monitor: &mut Monitor,
    mut forward: Option<&mut Output>,
    mut record: Option<&mut Output>,
    cfg: &RecvConfig,
    clock: &dyn Clock,
    stop: &AtomicBool,
    on_status: &mut dyn FnMut(&mut Monitor, Instant),
) -> Result<StopReason> {
    let start = clock.now();
    let interval = cfg.status_interval.max(Duration::from_millis(1));
    let mut next_status = start + interval;
    let mut last_data: Option<Instant> = None;
    let reason = loop {
        if stop.load(Ordering::Relaxed) {
            break StopReason::Interrupted;
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
            monitor.set_input_stats(input.stats());
            on_status(monitor, now);
            while next_status <= now {
                next_status += interval;
            }
            continue;
        }
        match input.read(deadline.saturating_duration_since(now))? {
            ReadEvent::Data(chunk) => {
                let arrival = clock.now();
                last_data = Some(arrival);
                let live = chunk.datagram.is_some();
                if let Some(out) = forward.as_deref_mut() {
                    let since = arrival.saturating_duration_since(start);
                    let position = (since.as_nanos() * u128::from(TICKS_PER_SECOND) / 1_000_000_000) as u64;
                    out.relay(chunk.datagram, chunk.rtp.is_some(), chunk.packets, position)
                        .map_err(|e| Error::io(format!("forward to {}", out.description()), e))?;
                }
                if let Some(out) = record.as_deref_mut() {
                    out.send(chunk.packets, 0).map_err(|e| Error::io(format!("write {}", out.description()), e))?;
                }
                monitor.push(chunk.packets, live.then_some(arrival));
            }
            ReadEvent::Idle => {}
            ReadEvent::End => break StopReason::EndOfInput,
        }
    };
    monitor.finish();
    monitor.set_input_stats(input.stats());
    for out in [forward, record].into_iter().flatten() {
        out.flush().map_err(|e| Error::io(format!("write {}", out.description()), e))?;
    }
    Ok(reason)
}
