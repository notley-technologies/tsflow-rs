//! Real-time playback: a file (or any input) to a socket or file, paced by
//! its PCR.
//!
//! [`play`] reads packets, gives each its position on the program clock
//! ([`PcrTimeline`]), and sends them through a [`Pacer`] so they leave at
//! the stream's own rate. Looping rewrites the repeated passes with a
//! [`LoopFixer`] so a receiver sees one continuous, valid stream (see
//! [`crate::looping`] for exactly what changes at the loop point).
//!
//! A file is read on demand: a packet is read when the pacer needs it, so
//! the file is never far ahead of the output and memory stays at about one
//! read buffer. A socket input is read as datagrams arrive, each packet is
//! scheduled `latency` after its arrival, and the pacer re-anchors when the
//! input stalls or runs ahead (a de-jittering relay). Everything runs on
//! the calling thread.
//!
//! [`play_with`] adds a [`Stage`] between the timing and the output: each
//! timed input packet goes through it (KLV injection, for example) and
//! whatever it emits leaves at that packet's time.

use crate::clock::Clock;
use crate::framer::PacketFormat;
use crate::input::{Input, ReadEvent};
use crate::looping::LoopFixer;
use crate::output::Sink;
use crate::pacer::{Pacer, PacerConfig};
use crate::{Error, Result, PACKET_SIZE};
use mpegts::{Demuxer, PcrTimeline, Report, StreamKind, NULL_PID};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// Longest single wait, so a stop request or a status line is never late
/// by more than this.
const POLL: Duration = Duration::from_millis(100);

/// Settings of [`play`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct PlayConfig {
    /// Output delay: the first packet leaves this long after it is read.
    /// For a socket input it is the de-jitter buffer.
    pub latency: Duration,
    /// TS packets per datagram.
    pub burst: usize,
    /// Pace by the PCR. Without pacing, packets are copied as fast as
    /// possible (for conversion, or to measure throughput).
    pub pace: bool,
    /// How many times to play the input: `Some(1)` once, `None` forever.
    pub passes: Option<u64>,
    /// Transport rate (bits per second) used before two PCRs have been
    /// seen, and for an input without PCR.
    pub fallback_bitrate: u64,
    /// The PID whose PCR times the stream; `None` follows the first PID
    /// that carries one.
    pub pcr_pid: Option<u16>,
    /// Longest a packet waits for others to fill its datagram.
    pub max_hold: Duration,
    /// How often the status callback runs.
    pub status_interval: Duration,
}

impl Default for PlayConfig {
    fn default() -> Self {
        PlayConfig {
            latency: Duration::from_millis(200),
            burst: crate::endpoint::DEFAULT_PACKETS_PER_DATAGRAM,
            pace: true,
            passes: Some(1),
            fallback_bitrate: 4_000_000,
            pcr_pid: None,
            max_hold: Duration::from_millis(10),
            status_interval: Duration::from_secs(1),
        }
    }
}

/// Counters of a playback, passed to the status callback and returned at
/// the end.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct PlayStats {
    /// Packets read.
    pub packets_in: u64,
    /// Packets sent.
    pub packets_out: u64,
    /// Datagrams (or writes) sent.
    pub datagrams: u64,
    /// The current pass, from 1.
    pub pass: u64,
    /// Stream position (27 MHz) of the last datagram sent.
    pub position: u64,
    /// Times the schedule re-anchored.
    pub reanchors: u64,
    /// Packets dropped because the pacer queue was full.
    pub dropped: u64,
    /// PCR discontinuity indicators set at loop points.
    pub pcr_discontinuities: u64,
    /// Latest any datagram left after its due time.
    pub max_late: Duration,
    /// Latest since the previous status callback.
    pub window_max_late: Duration,
    /// How far ahead the pacer's queue reaches.
    pub depth: Duration,
    /// Time since playback started.
    pub elapsed: Duration,
}

/// What the start of a file says about it.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Probe {
    /// Record format.
    pub format: Option<PacketFormat>,
    /// The PCR PID of the first program with video (else the first with a
    /// PCR).
    pub pcr_pid: Option<u16>,
    /// Average bitrate of what was read.
    pub bitrate: Option<u64>,
    /// The demuxer's report of what was read.
    pub report: Report,
}

/// A processing step between timing and output, for [`play_with`].
///
/// Each input packet arrives with its program clock time; every packet the
/// stage passes to `out` during that call goes out at the input packet's
/// position, so pacing keeps working through it.
pub trait Stage {
    /// One timed input packet.
    fn packet(
        &mut self,
        packet: &[u8; PACKET_SIZE],
        clock: Option<u64>,
        out: &mut dyn FnMut(&[u8; PACKET_SIZE]),
    ) -> Result<()>;

    /// The input has ended: emit what is left.
    fn finish(&mut self, out: &mut dyn FnMut(&[u8; PACKET_SIZE])) -> Result<()>;
}

/// Read up to `max_bytes` of a seekable input to find its format, PCR PID
/// and bitrate, then rewind it.
pub fn probe(input: &mut Input, max_bytes: u64) -> Result<Probe> {
    let mut demux = Demuxer::new();
    let mut read = 0u64;
    while read < max_bytes {
        match input.read(Duration::ZERO)? {
            ReadEvent::Data(chunk) => {
                read += chunk.packets.len() as u64;
                demux.push(chunk.packets);
            }
            ReadEvent::Idle => {}
            ReadEvent::End => break,
        }
    }
    demux.finish();
    let report = demux.report();
    let usable = |pid: &Option<u16>| pid.filter(|&p| p != NULL_PID);
    let pcr_pid = report
        .programs
        .iter()
        .find(|p| p.streams_of(StreamKind::Video).next().is_some() && usable(&p.pcr_pid).is_some())
        .or_else(|| report.programs.iter().find(|p| usable(&p.pcr_pid).is_some()))
        .and_then(|p| usable(&p.pcr_pid));
    let probe = Probe { format: input.stats().framer.format, pcr_pid, bitrate: report.bitrate(), report };
    input.rewind()?;
    Ok(probe)
}

/// Play `input` to `sink` until it ends (after `cfg.passes` passes) or
/// `stop` is set. `on_status` runs every `cfg.status_interval`.
pub fn play(
    input: &mut Input,
    sink: &mut dyn Sink,
    cfg: &PlayConfig,
    clock: &dyn Clock,
    stop: &AtomicBool,
    on_status: &mut dyn FnMut(&PlayStats),
) -> Result<PlayStats> {
    play_with(input, sink, cfg, clock, stop, None, on_status)
}

/// [`play`] with a [`Stage`] between the timing and the output. A stage
/// sees one pass only: it cannot be combined with looping.
pub fn play_with(
    input: &mut Input,
    sink: &mut dyn Sink,
    cfg: &PlayConfig,
    clock: &dyn Clock,
    stop: &AtomicBool,
    mut stage: Option<&mut dyn Stage>,
    on_status: &mut dyn FnMut(&PlayStats),
) -> Result<PlayStats> {
    if stage.is_some() && cfg.passes != Some(1) {
        return Err(Error::config("a processing stage plays the input once"));
    }
    if cfg.passes == Some(0) {
        return Err(Error::config("passes must be at least 1"));
    }
    if cfg.passes != Some(1) && !input.is_seekable() {
        return Err(Error::config(format!(
            "{} cannot loop: only a file can be played more than once",
            input.description()
        )));
    }
    let live = input.is_live();
    let mut pacer_cfg = PacerConfig::new(cfg.latency);
    pacer_cfg.burst = cfg.burst;
    pacer_cfg.max_hold = cfg.max_hold;
    if !live {
        // Read on demand: a file is never early, only late after a stall.
        pacer_cfg.max_early = None;
    }
    let mut pacer = Pacer::new(pacer_cfg)?;
    let mut timeline = PcrTimeline::new(cfg.pcr_pid, cfg.fallback_bitrate.max(1));
    let mut fixer = LoopFixer::new();
    let mut stats = PlayStats { pass: 1, ..PlayStats::default() };
    let mut datagram: Vec<u8> = Vec::with_capacity(cfg.burst * PACKET_SIZE);
    let mut datagram_position = 0u64;
    let mut send_error: Option<std::io::Error> = None;
    let send_err = |e: std::io::Error| Error::io("send", e);
    let start = clock.now();
    let mut next_status = start + cfg.status_interval;
    let mut input_open = true;
    let mut passes_done = 0u64;
    let mut stage_error: Option<Error> = None;
    let mut last_position = 0u64;

    loop {
        if stop.load(Ordering::Relaxed) {
            break;
        }
        let now = clock.now();
        if cfg.pace {
            while let Some(d) = pacer.next(input_open) {
                if d.at > now {
                    break;
                }
                let late = now.saturating_duration_since(d.at);
                stats.max_late = stats.max_late.max(late);
                stats.window_max_late = stats.window_max_late.max(late);
                if let Some(position) = pacer.pop(d.count, &mut datagram) {
                    sink.send(&datagram, position).map_err(send_err)?;
                    stats.datagrams += 1;
                    stats.packets_out += d.count as u64;
                    stats.position = position;
                }
            }
        }
        if now >= next_status {
            stats.elapsed = now.saturating_duration_since(start);
            stats.reanchors = pacer.schedule().reanchors();
            stats.dropped = pacer.dropped();
            stats.depth = pacer.depth(now);
            stats.pcr_discontinuities = fixer.pcr_discontinuities();
            on_status(&stats);
            stats.window_max_late = Duration::ZERO;
            while next_status <= now {
                next_status += cfg.status_interval.max(Duration::from_millis(1));
            }
        }
        if !input_open && pacer.is_empty() {
            break;
        }
        let want_input = input_open && (!cfg.pace || live || pacer.len() < cfg.burst);
        if !want_input {
            let wake = pacer.next(input_open).map_or(now + POLL, |d| d.at.min(now + POLL));
            clock.sleep_until(wake.min(next_status));
            continue;
        }
        let timeout = match pacer.next(true) {
            _ if !live => Duration::ZERO,
            Some(d) => d.at.saturating_duration_since(now).min(POLL),
            None => POLL,
        };
        let mut emit = |packet: &[u8; PACKET_SIZE], position: u64, arrival| {
            if cfg.pace {
                pacer.push(packet, position, arrival);
                return;
            }
            if datagram.is_empty() {
                datagram_position = position;
            }
            datagram.extend_from_slice(packet);
            if datagram.len() >= cfg.burst * PACKET_SIZE && send_error.is_none() {
                match sink.send(&datagram, datagram_position) {
                    Ok(()) => {
                        stats.datagrams += 1;
                        stats.packets_out += (datagram.len() / PACKET_SIZE) as u64;
                        stats.position = datagram_position;
                    }
                    Err(e) => send_error = Some(e),
                }
                datagram.clear();
            }
        };
        // Through the stage, if any; its output keeps the input packet's
        // position.
        let mut timed = |tp: mpegts::TimedPacket, emit: &mut dyn FnMut(&[u8; PACKET_SIZE], u64)| {
            last_position = tp.position;
            match stage.as_deref_mut() {
                None => emit(&tp.packet, tp.position),
                Some(_) if stage_error.is_some() => {}
                Some(s) => {
                    if let Err(e) = s.packet(&tp.packet, tp.clock, &mut |p| emit(p, tp.position)) {
                        stage_error = Some(e);
                    }
                }
            }
        };
        match input.read(timeout)? {
            ReadEvent::Data(chunk) => {
                let arrival = clock.now();
                for c in chunk.packets.chunks_exact_mut(PACKET_SIZE) {
                    let Ok(p) = <&mut [u8; PACKET_SIZE]>::try_from(c) else { continue };
                    fixer.fix(p);
                    stats.packets_in += 1;
                    timeline.push(p, &mut |tp| timed(tp, &mut |p, pos| emit(p, pos, arrival)));
                }
            }
            ReadEvent::Idle => {}
            ReadEvent::End => {
                passes_done += 1;
                if cfg.passes.map_or(true, |n| passes_done < n) {
                    input.rewind()?;
                    fixer.start_pass();
                    stats.pass += 1;
                } else {
                    let arrival = clock.now();
                    timeline.flush(&mut |tp| timed(tp, &mut |p, pos| emit(p, pos, arrival)));
                    if let (Some(s), None) = (stage.as_deref_mut(), &stage_error) {
                        let position = last_position;
                        if let Err(e) = s.finish(&mut |p| emit(p, position, arrival)) {
                            stage_error = Some(e);
                        }
                    }
                    input_open = false;
                }
            }
        }
        if let Some(e) = send_error.take() {
            return Err(send_err(e));
        }
        if let Some(e) = stage_error.take() {
            return Err(e);
        }
    }
    if !cfg.pace && !datagram.is_empty() && !stop.load(Ordering::Relaxed) {
        sink.send(&datagram, datagram_position).map_err(send_err)?;
        stats.datagrams += 1;
        stats.packets_out += (datagram.len() / PACKET_SIZE) as u64;
    }
    sink.flush().map_err(send_err)?;
    stats.elapsed = clock.now().saturating_duration_since(start);
    stats.reanchors = pacer.schedule().reanchors();
    stats.dropped = pacer.dropped();
    stats.pcr_discontinuities = fixer.pcr_discontinuities();
    stats.depth = Duration::ZERO;
    Ok(stats)
}
