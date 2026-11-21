//! Stream analysis for live monitoring and offline reports.
//!
//! A [`Monitor`] takes TS packets (with their arrival time, for a live
//! input) and keeps:
//!
//! - the programs and streams, continuity errors per PID, transport errors
//!   (the `mpegts` [`Demuxer`] and its [`Report`]);
//! - per PCR PID: PCR count, interval (minimum, average, maximum; ISO/IEC
//!   13818-1 2.7.2 allows at most 100 ms), discontinuities (flagged with
//!   discontinuity_indicator, 2.4.3.5) and unflagged jumps, and for a live
//!   input the PCR-versus-arrival jitter: the peak-to-peak spread, within
//!   each status window, of (arrival time - PCR time) relative to the
//!   first PCR of the window's time base;
//! - PAT and PMT repetition intervals in stream time (from the
//!   [`PcrTimeline`] position of the packet starting each section);
//! - per KLV stream (sync and async): ST 0601 packets decoded with `misb`,
//!   checksums, ST 0102 security markings, tag 2 time stamps, and for
//!   synchronous KLV how far ahead of its PTS each access unit arrived
//!   (MISB ST 1402: before the PTS and at most 1 s before it).
//!
//! [`Monitor::take_window`] gives the figures since the last call, for a
//! status line; [`Monitor::summary`] gives the totals as a [`Summary`].
//! Memory is bounded: per-PID tables have at most 8192 entries, the
//! timeline holds at most one PCR interval, and the demuxer's buffers are
//! limited by its [`Config`](mpegts::Config).

mod klv;

pub use klv::{security_marking, KlvSummary};

use crate::input::InputStats;
use crate::json::Json;
use crate::pacer::ticks_to_duration;
use crate::{Error, Result, PACKET_SIZE};
use klv::{KlvTrack, Range};
use mpegts::{
    pcr_delta, pts_delta, Codec, Demuxer, Output, PacketHeader, PcrTimeline, Report, Select, Subscription,
    SubscriptionId, TimedPacket, Unit, PCR_HZ,
};
use std::collections::BTreeMap;
use std::fmt;
use std::time::{Duration, Instant};

/// The longest PCR interval ISO/IEC 13818-1 2.7.2 allows: 100 ms.
pub const MAX_PCR_INTERVAL: Duration = Duration::from_millis(100);
const MAX_PCR_INTERVAL_TICKS: i64 = (PCR_HZ / 10) as i64;
/// A PCR step back, or forward by a second or more, without a
/// discontinuity_indicator is a jump, not an interval.
const PCR_JUMP_TICKS: i64 = PCR_HZ as i64;
const TICKS_PER_MS: f64 = 27_000.0;

/// Settings of a [`Monitor`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct MonitorConfig {
    /// Transport rate assumed until two PCRs have been seen, and for a
    /// stream without PCR (bits per second).
    pub fallback_bitrate: u64,
}

impl Default for MonitorConfig {
    fn default() -> Self {
        MonitorConfig { fallback_bitrate: 4_000_000 }
    }
}

/// Live and offline transport stream analysis (see the [module
/// documentation](self)).
#[derive(Debug)]
pub struct Monitor {
    timeline: PcrTimeline,
    demux: Demuxer,
    sync_sub: SubscriptionId,
    st: State,
}

#[derive(Debug)]
struct State {
    report: Report,
    revision: u64,
    pmt_pids: Vec<(u16, u16)>,
    /// Packets released by the timeline in this push, for the demuxer,
    /// with their program clock values.
    scratch: Vec<u8>,
    clocks: Vec<Option<u64>>,
    positions: Vec<u64>,
    /// Demuxer feed number of the first packet in `scratch`.
    scratch_first: u64,
    fed: u64,
    packets: u64,
    timed: u64,
    first_position: Option<u64>,
    last_position: u64,
    first_arrival: Option<Instant>,
    last_arrival: Option<Instant>,
    pcr: BTreeMap<u16, PcrTrack>,
    psi: BTreeMap<u16, IntervalTrack>,
    klv: BTreeMap<u16, KlvTrack>,
    input: Option<InputStats>,
    window: WindowBase,
}

#[derive(Debug, Default)]
struct WindowBase {
    start: Option<Instant>,
    packets: u64,
    cc_errors: u64,
    rtp_lost: u64,
}

#[derive(Debug, Default)]
struct PcrTrack {
    count: u64,
    last: Option<u64>,
    interval_ms: Range,
    interval_sum: u64,
    over_limit: u64,
    discontinuities: u64,
    jumps: u64,
    /// Arrival of the PCR the jitter is measured from, and the stream
    /// ticks since it.
    base: Option<(Instant, u64)>,
    window_offset_ms: Range,
    jitter_pp_ms: Option<f64>,
}

impl PcrTrack {
    fn on_pcr(&mut self, pcr: u64, discontinuity: bool, arrival: Option<Instant>) {
        self.count += 1;
        let mut rebase = true;
        if let Some(last) = self.last {
            let d = pcr_delta(pcr, last);
            if discontinuity {
                self.discontinuities += 1;
            } else if d <= 0 || d >= PCR_JUMP_TICKS {
                self.jumps += 1;
            } else {
                rebase = false;
                self.interval_ms.add(d as f64 / TICKS_PER_MS);
                self.interval_sum += d as u64;
                if d > MAX_PCR_INTERVAL_TICKS {
                    self.over_limit += 1;
                }
                if let Some((_, ticks)) = &mut self.base {
                    *ticks += d as u64;
                }
            }
        }
        self.last = Some(pcr);
        let Some(t) = arrival else { return };
        match self.base {
            Some((t0, ticks)) if !rebase => {
                let wall = t.saturating_duration_since(t0).as_secs_f64() * 1e3;
                self.window_offset_ms.add(wall - ticks as f64 / TICKS_PER_MS);
            }
            _ => {
                // A new time base: offsets before and after are not
                // comparable, so the window so far is closed.
                self.close_window();
                self.base = Some((t, 0));
                self.window_offset_ms.add(0.0);
            }
        }
    }

    fn close_window(&mut self) -> Option<f64> {
        let pp = self.window_offset_ms.get().map(|(min, max)| max - min);
        if let Some(pp) = pp {
            self.jitter_pp_ms = Some(self.jitter_pp_ms.map_or(pp, |j| j.max(pp)));
        }
        self.window_offset_ms = Range::default();
        pp
    }
}

#[derive(Debug, Default)]
struct IntervalTrack {
    count: u64,
    last: Option<u64>,
    interval_ms: Range,
}

impl IntervalTrack {
    fn on_section(&mut self, position: u64) {
        self.count += 1;
        if let Some(last) = self.last {
            self.interval_ms.add(position.saturating_sub(last) as f64 / TICKS_PER_MS);
        }
        self.last = Some(position);
    }
}

impl Monitor {
    /// A monitor with the default settings.
    pub fn new() -> Result<Monitor> {
        Monitor::with_config(MonitorConfig::default())
    }

    /// A monitor with these settings.
    pub fn with_config(cfg: MonitorConfig) -> Result<Monitor> {
        let mut demux = Demuxer::new();
        let subscribe = |demux: &mut Demuxer, codec| {
            demux
                .subscribe(Subscription::new(Select::Codec(codec), Unit::Pes).strip_metadata_au())
                .map_err(|e| Error::config(format!("KLV subscription: {e}")))
        };
        let sync_sub = subscribe(&mut demux, Codec::KlvSync)?;
        subscribe(&mut demux, Codec::KlvAsync)?;
        let st = State {
            report: demux.report(),
            revision: demux.revision(),
            pmt_pids: Vec::new(),
            scratch: Vec::new(),
            clocks: Vec::new(),
            positions: Vec::new(),
            scratch_first: 1,
            fed: 0,
            packets: 0,
            timed: 0,
            first_position: None,
            last_position: 0,
            first_arrival: None,
            last_arrival: None,
            pcr: BTreeMap::new(),
            psi: BTreeMap::new(),
            klv: BTreeMap::new(),
            input: None,
            window: WindowBase::default(),
        };
        Ok(Monitor { timeline: PcrTimeline::new(None, cfg.fallback_bitrate.max(1)), demux, sync_sub, st })
    }

    /// Add packets (whole 188-byte packets back to back; a trailing
    /// partial packet is ignored). `arrival` is when they arrived, for a
    /// live input; `None` for a file.
    pub fn push(&mut self, packets: &[u8], arrival: Option<Instant>) {
        let st = &mut self.st;
        if let Some(t) = arrival {
            st.first_arrival.get_or_insert(t);
            st.last_arrival = Some(t);
            st.window.start.get_or_insert(t);
        }
        st.begin_batch();
        for chunk in packets.chunks_exact(PACKET_SIZE) {
            let Ok(p) = <&[u8; PACKET_SIZE]>::try_from(chunk) else { continue };
            st.packets += 1;
            st.window.packets += 1;
            // PCRs are timed on arrival, before the timeline holds them, so
            // the arrival jitter is exact for every PCR PID.
            if p[1] & 0x80 == 0 && p[3] & 0x20 != 0 && p[4] >= 7 && p[5] & 0x10 != 0 {
                if let Ok(h) = PacketHeader::parse(p) {
                    if let Some(pcr) = h.pcr() {
                        st.pcr.entry(h.pid).or_default().on_pcr(pcr, h.discontinuity(), arrival);
                    }
                }
            }
            self.timeline.push(p, &mut |tp| st.on_timed(&tp));
        }
        self.feed_demux();
    }

    /// The end of the input: release what the timeline holds and finish
    /// the demuxer.
    pub fn finish(&mut self) {
        let st = &mut self.st;
        st.begin_batch();
        self.timeline.flush(&mut |tp| st.on_timed(&tp));
        self.feed_demux();
        self.st.clocks.clear();
        self.demux.finish();
        self.drain();
        self.refresh();
    }

    /// Counters of the input, included in the summary and status windows
    /// (RTP loss, framing). Call before [`take_window`](Self::take_window)
    /// and [`summary`](Self::summary).
    pub fn set_input_stats(&mut self, stats: InputStats) {
        self.st.input = Some(stats);
    }

    /// The latest report: programs, streams, per-PID counters.
    pub fn report(&self) -> &Report {
        &self.st.report
    }

    /// Changes whenever the programs or streams change (a new PAT or PMT
    /// version, a stream identified).
    pub fn revision(&self) -> u64 {
        self.st.revision
    }

    /// TS packets pushed so far.
    pub fn packets(&self) -> u64 {
        self.st.packets
    }

    fn feed_demux(&mut self) {
        if !self.st.scratch.is_empty() {
            self.demux.push(&self.st.scratch);
            self.st.fed += (self.st.scratch.len() / PACKET_SIZE) as u64;
        }
        self.drain();
        if self.demux.revision() != self.st.revision {
            self.refresh();
        }
        // After the demuxer, so a PMT in the same batch as the PAT that
        // announces it is already known.
        self.st.psi_sections();
    }

    fn drain(&mut self) {
        while let Some(o) = self.demux.pop_output() {
            self.st.on_output(&o, o.subscription == self.sync_sub);
            self.demux.recycle(o);
        }
    }

    fn refresh(&mut self) {
        self.st.revision = self.demux.revision();
        self.st.report = self.demux.report();
        self.st.pmt_pids = self.st.report.programs.iter().map(|p| (p.pmt_pid, p.number)).collect();
    }

    /// The figures since the last call (or since the first packet), for a
    /// status line. Closes the PCR jitter window.
    pub fn take_window(&mut self, now: Instant) -> WindowStats {
        self.refresh();
        let st = &mut self.st;
        let cc_total: u64 = st.report.pids.iter().map(|p| p.cc_errors).sum();
        let rtp_total = st.input.and_then(|i| i.rtp).map_or(0, |r| r.lost);
        let elapsed = st.window.start.map_or(Duration::ZERO, |s| now.saturating_duration_since(s));
        let secs = elapsed.as_secs_f64();
        let mut jitter: Option<f64> = None;
        for t in st.pcr.values_mut() {
            if let Some(pp) = t.close_window() {
                jitter = Some(jitter.map_or(pp, |j| j.max(pp)));
            }
        }
        let klv = st
            .klv
            .values_mut()
            .map(|k| KlvWindow {
                pid: k.pid,
                sync: k.sync,
                packets: std::mem::take(&mut k.window_packets),
                marking: k.last_marking.clone(),
                checksum_errors: k.checksum_mismatch + k.checksum_missing,
            })
            .collect();
        let w = WindowStats {
            elapsed,
            packets: st.window.packets,
            bitrate: (secs > 0.0).then_some(st.window.packets as f64 * (PACKET_SIZE * 8) as f64 / secs),
            cc_errors: cc_total.saturating_sub(st.window.cc_errors),
            total_cc_errors: cc_total,
            pcr_jitter_ms: jitter,
            klv,
            rtp_lost: rtp_total.saturating_sub(st.window.rtp_lost),
        };
        st.window = WindowBase { start: Some(now), packets: 0, cc_errors: cc_total, rtp_lost: rtp_total };
        w
    }

    /// The totals so far.
    pub fn summary(&mut self) -> Summary {
        self.refresh();
        let st = &mut self.st;
        // Stream time covered: the span of the timeline positions, plus one
        // packet's share so N packets cover N intervals.
        let stream_duration = st.first_position.filter(|_| st.timed > 1).map(|first| {
            let span = st.last_position.saturating_sub(first);
            ticks_to_duration(span.saturating_add(span / (st.timed - 1)))
        });
        let wall_duration = match (st.first_arrival, st.last_arrival) {
            (Some(a), Some(b)) if b > a => Some(b - a),
            _ => None,
        };
        let secs = stream_duration.filter(|d| !d.is_zero()).or(wall_duration).map(|d| d.as_secs_f64());
        let rate = |packets: u64| secs.filter(|&s| s > 0.0).map(|s| (packets as f64 * 1504.0 / s) as u64);
        let r = &st.report;
        let pids = r
            .pids
            .iter()
            .map(|p| PidSummary {
                pid: p.pid,
                packets: p.packets,
                cc_errors: p.cc_errors,
                transport_errors: p.transport_errors,
                bitrate: rate(p.packets),
                content: describe_pid(r, p.pid),
            })
            .collect();
        let pcr =
            st.pcr
                .iter_mut()
                .map(|(&pid, t)| {
                    t.close_window();
                    PcrSummary {
                        pid,
                        count: t.count,
                        interval_ms: t.interval_ms.get().map(|(min, max)| {
                            (min, t.interval_sum as f64 / t.interval_ms.n as f64 / TICKS_PER_MS, max)
                        }),
                        over_limit: t.over_limit,
                        discontinuities: t.discontinuities,
                        jumps: t.jumps,
                        jitter_pp_ms: t.jitter_pp_ms,
                    }
                })
                .collect();
        let psi = st
            .psi
            .iter()
            .map(|(&pid, t)| PsiSummary {
                pid,
                table: match st.pmt_pids.iter().find(|(p, _)| *p == pid) {
                    _ if pid == 0 => "PAT".to_string(),
                    Some((_, program)) => format!("PMT (program {program})"),
                    None => "PMT".to_string(),
                },
                sections: t.count,
                interval_ms: t.interval_ms.get(),
            })
            .collect();
        let klv = st.klv.values().map(|k| k.summary(secs)).collect();
        Summary {
            packets: st.packets,
            stream_duration,
            wall_duration,
            bitrate: rate(st.packets),
            cc_errors: r.pids.iter().map(|p| p.cc_errors).sum(),
            transport_errors: r.pids.iter().map(|p| p.transport_errors).sum(),
            input: st.input,
            pids,
            pcr,
            psi,
            klv,
            report: r.clone(),
        }
    }
}

impl State {
    fn begin_batch(&mut self) {
        self.scratch.clear();
        self.clocks.clear();
        self.positions.clear();
        self.scratch_first = self.fed + 1;
    }

    fn on_timed(&mut self, tp: &TimedPacket) {
        self.timed += 1;
        self.first_position.get_or_insert(tp.position);
        self.last_position = tp.position;
        self.scratch.extend_from_slice(&tp.packet);
        self.clocks.push(tp.clock);
        self.positions.push(tp.position);
    }

    /// PAT and PMT repetition: a section starts in a payload_unit_start
    /// packet of PID 0 or a PMT PID (ISO/IEC 13818-1 2.4.4.2); packets with
    /// transport_error_indicator set are not trusted.
    fn psi_sections(&mut self) {
        for (p, &position) in self.scratch.chunks_exact(PACKET_SIZE).zip(&self.positions) {
            if p[1] & 0xC0 != 0x40 {
                continue;
            }
            let pid = (u16::from(p[1] & 0x1F) << 8) | u16::from(p[2]);
            if pid == 0 || self.pmt_pids.iter().any(|&(pmt, _)| pmt == pid) {
                self.psi.entry(pid).or_default().on_section(position);
            }
        }
    }

    fn clock_of(&self, packet: u64) -> Option<u64> {
        let i = packet.checked_sub(self.scratch_first)?;
        self.clocks.get(usize::try_from(i).ok()?).copied().flatten()
    }

    fn on_output(&mut self, o: &Output, sync: bool) {
        let lead_ms = match (sync, o.pts, self.clock_of(o.packet)) {
            (true, Some(pts), Some(clock)) => Some(pts_delta(pts, clock / 300) as f64 / 90.0),
            _ => None,
        };
        self.klv.entry(o.pid).or_insert_with(|| KlvTrack::new(o.pid, sync)).push_pes(
            o.payload(),
            o.damaged || o.truncated,
            lead_ms,
        );
    }
}

fn describe_pid(r: &Report, pid: u16) -> String {
    match pid {
        0x0000 => return "PAT".into(),
        0x0001 => return "CAT".into(),
        mpegts::NULL_PID => return "null packets".into(),
        _ => {}
    }
    if let Some(p) = r.programs.iter().find(|p| p.pmt_pid == pid) {
        return format!("PMT (program {})", p.number);
    }
    if let Some(s) = r.stream(pid) {
        return format!("{} {}", s.kind(), s.codec.name());
    }
    if r.programs.iter().any(|p| p.pcr_pid == Some(pid)) {
        return "PCR".into();
    }
    String::new()
}

/// One KLV stream's figures in a status window.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct KlvWindow {
    /// The PID.
    pub pid: u16,
    /// Synchronous carriage.
    pub sync: bool,
    /// ST 0601 packets in the window.
    pub packets: u64,
    /// The marking of the last marked packet so far.
    pub marking: Option<String>,
    /// Checksum mismatches and missing checksums so far.
    pub checksum_errors: u64,
}

/// The figures of one status window (see [`Monitor::take_window`]).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct WindowStats {
    /// Wall time the window covers.
    pub elapsed: Duration,
    /// TS packets received in it.
    pub packets: u64,
    /// Bits per second by wall time.
    pub bitrate: Option<f64>,
    /// New continuity errors.
    pub cc_errors: u64,
    /// Continuity errors so far.
    pub total_cc_errors: u64,
    /// Largest PCR arrival jitter (peak to peak, ms) of any PCR PID.
    pub pcr_jitter_ms: Option<f64>,
    /// KLV streams.
    pub klv: Vec<KlvWindow>,
    /// New RTP packets missing.
    pub rtp_lost: u64,
}

/// One PID in a [`Summary`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct PidSummary {
    /// The PID.
    pub pid: u16,
    /// Packets.
    pub packets: u64,
    /// Continuity counter errors (ISO/IEC 13818-1 2.4.3.3).
    pub cc_errors: u64,
    /// Packets with transport_error_indicator set.
    pub transport_errors: u64,
    /// Bits per second over the stream time.
    pub bitrate: Option<u64>,
    /// What the PID carries: "PAT", "PMT (program 1)", "video H.264", ...
    pub content: String,
}

/// One PCR PID in a [`Summary`].
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct PcrSummary {
    /// The PID.
    pub pid: u16,
    /// PCRs seen.
    pub count: u64,
    /// Interval between consecutive PCRs (minimum, average, maximum, ms),
    /// not counting discontinuities and jumps.
    pub interval_ms: Option<(f64, f64, f64)>,
    /// Intervals over 100 ms (ISO/IEC 13818-1 2.7.2).
    pub over_limit: u64,
    /// PCRs with discontinuity_indicator set (2.4.3.5).
    pub discontinuities: u64,
    /// Steps back, or forward by 1 s or more, without the indicator.
    pub jumps: u64,
    /// Largest peak-to-peak PCR-versus-arrival jitter of any status window,
    /// ms (live inputs only).
    pub jitter_pp_ms: Option<f64>,
}

/// One PSI table PID in a [`Summary`].
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct PsiSummary {
    /// The PID.
    pub pid: u16,
    /// "PAT" or "PMT (program N)".
    pub table: String,
    /// Sections started.
    pub sections: u64,
    /// Repetition interval in stream time (minimum, maximum, ms).
    pub interval_ms: Option<(f64, f64)>,
}

/// The totals of a [`Monitor`].
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Summary {
    /// TS packets received.
    pub packets: u64,
    /// Stream time covered (PCR timeline).
    pub stream_duration: Option<Duration>,
    /// Wall time from the first to the last arrival (live inputs).
    pub wall_duration: Option<Duration>,
    /// Bits per second over the stream time.
    pub bitrate: Option<u64>,
    /// Continuity errors on all PIDs.
    pub cc_errors: u64,
    /// Packets with transport_error_indicator set.
    pub transport_errors: u64,
    /// The input's counters, if given with [`Monitor::set_input_stats`].
    pub input: Option<InputStats>,
    /// Every PID seen.
    pub pids: Vec<PidSummary>,
    /// Every PID carrying PCR.
    pub pcr: Vec<PcrSummary>,
    /// PAT and PMT PIDs.
    pub psi: Vec<PsiSummary>,
    /// KLV streams.
    pub klv: Vec<KlvSummary>,
    /// The demuxer's report (programs and streams).
    pub report: Report,
}

impl Summary {
    /// ST 0601 packets on all KLV streams.
    pub fn klv_packets(&self) -> u64 {
        self.klv.iter().map(|k| k.packets).sum()
    }

    /// The duration rates are computed over: stream time, or wall time
    /// for an input without timing.
    pub fn duration(&self) -> Option<Duration> {
        self.stream_duration.filter(|d| !d.is_zero()).or(self.wall_duration)
    }

    /// The summary as one JSON object.
    pub fn to_json(&self) -> String {
        self.json().to_string()
    }

    pub(crate) fn json(&self) -> Json {
        let ms = |d: Option<Duration>| d.map(|d| d.as_secs_f64() * 1e3);
        let input = self.input.map(|i| {
            Json::object()
                .with("bytes", i.framer.bytes)
                .with("format", i.framer.format.map(|f| f.name()))
                .with("bytes_skipped", i.framer.bytes_skipped)
                .with("sync_losses", i.framer.sync_losses)
                .with("datagrams", i.datagrams)
                .with("rtp_datagrams", i.rtp_datagrams)
                .with("unrecognized_datagrams", i.unrecognized_datagrams)
                .with("recv_buffer", i.recv_buffer)
                .with(
                    "rtp",
                    i.rtp.map(|r| {
                        Json::object()
                            .with("received", r.received)
                            .with("lost", r.lost)
                            .with("late", r.late)
                            .with("restarts", r.restarts)
                    }),
                )
        });
        let programs: Vec<Json> = self
            .report
            .programs
            .iter()
            .map(|p| {
                let streams: Vec<Json> = p
                    .streams
                    .iter()
                    .map(|s| {
                        Json::object()
                            .with("pid", s.pid)
                            .with("stream_type", s.stream_type)
                            .with("kind", s.kind().to_string())
                            .with("codec", s.codec.name())
                            .with("packets", s.packets)
                            .with("cc_errors", s.cc_errors)
                            .with("bitrate", s.bitrate)
                    })
                    .collect();
                Json::object()
                    .with("number", p.number)
                    .with("pmt_pid", p.pmt_pid)
                    .with("version", p.version)
                    .with("pcr_pid", p.pcr_pid)
                    .with("streams", Json::Array(streams))
            })
            .collect();
        let pids: Vec<Json> = self
            .pids
            .iter()
            .map(|p| {
                Json::object()
                    .with("pid", p.pid)
                    .with("packets", p.packets)
                    .with("cc_errors", p.cc_errors)
                    .with("transport_errors", p.transport_errors)
                    .with("bitrate", p.bitrate)
                    .with("content", p.content.as_str())
            })
            .collect();
        let pcr: Vec<Json> = self
            .pcr
            .iter()
            .map(|p| {
                Json::object()
                    .with("pid", p.pid)
                    .with("count", p.count)
                    .with("interval_min_ms", p.interval_ms.map(|i| i.0))
                    .with("interval_avg_ms", p.interval_ms.map(|i| i.1))
                    .with("interval_max_ms", p.interval_ms.map(|i| i.2))
                    .with("over_100ms", p.over_limit)
                    .with("discontinuities", p.discontinuities)
                    .with("jumps", p.jumps)
                    .with("jitter_pp_ms", p.jitter_pp_ms)
            })
            .collect();
        let psi: Vec<Json> = self
            .psi
            .iter()
            .map(|p| {
                Json::object()
                    .with("pid", p.pid)
                    .with("table", p.table.as_str())
                    .with("sections", p.sections)
                    .with("interval_min_ms", p.interval_ms.map(|i| i.0))
                    .with("interval_max_ms", p.interval_ms.map(|i| i.1))
            })
            .collect();
        Json::object()
            .with("packets", self.packets)
            .with("bytes", self.packets * PACKET_SIZE as u64)
            .with("stream_duration_ms", ms(self.stream_duration))
            .with("wall_duration_ms", ms(self.wall_duration))
            .with("bitrate", self.bitrate)
            .with("cc_errors", self.cc_errors)
            .with("transport_errors", self.transport_errors)
            .with("input", input)
            .with("transport_stream_id", self.report.transport_stream_id)
            .with("programs", Json::Array(programs))
            .with("pids", Json::Array(pids))
            .with("pcr", Json::Array(pcr))
            .with("psi", Json::Array(psi))
            .with("klv", Json::Array(self.klv.iter().map(KlvSummary::to_json).collect()))
    }
}

fn mbps(bps: Option<u64>) -> String {
    bps.map_or_else(|| "-".into(), |b| format!("{:.2}", b as f64 / 1e6))
}

impl fmt::Display for Summary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let secs = |d: Option<Duration>| d.map_or_else(|| "-".into(), |d| format!("{:.3} s", d.as_secs_f64()));
        writeln!(
            f,
            "packets    {} ({:.2} MB), {} Mb/s",
            self.packets,
            (self.packets * PACKET_SIZE as u64) as f64 / 1e6,
            mbps(self.bitrate)
        )?;
        writeln!(f, "duration   {} stream time, {} wall clock", secs(self.stream_duration), secs(self.wall_duration))?;
        if let Some(i) = &self.input {
            write!(
                f,
                "input      {}, {} bytes skipped, {} sync losses",
                i.framer.format.map_or("no packets found", |p| p.name()),
                i.framer.bytes_skipped,
                i.framer.sync_losses
            )?;
            if i.datagrams > 0 {
                write!(f, "; {} datagrams ({} RTP", i.datagrams, i.rtp_datagrams)?;
                if let Some(r) = i.rtp {
                    write!(f, ": {} lost, {} late, {} restarts", r.lost, r.late, r.restarts)?;
                }
                write!(f, ")")?;
                if i.unrecognized_datagrams > 0 {
                    write!(f, ", {} not TS", i.unrecognized_datagrams)?;
                }
            }
            if let Some(b) = i.recv_buffer {
                write!(f, "; receive buffer {} KiB", b / 1024)?;
            }
            writeln!(f)?;
        }
        writeln!(f, "errors     {} CC errors, {} transport errors", self.cc_errors, self.transport_errors)?;
        for p in &self.report.programs {
            write!(f, "program {} (PMT 0x{:04X}", p.number, p.pmt_pid)?;
            if let Some(v) = p.version {
                write!(f, ", version {v}")?;
            }
            match p.pcr_pid {
                Some(pid) if pid != mpegts::NULL_PID => write!(f, ", PCR 0x{pid:04X}")?,
                _ => write!(f, ", no PCR")?,
            }
            writeln!(f, ")")?;
            for s in &p.streams {
                writeln!(f, "  0x{:04X}  type 0x{:02X}  {} {}", s.pid, s.stream_type, s.kind(), s.codec.name())?;
            }
        }
        if !self.pids.is_empty() {
            writeln!(f, "PID      packets  CC err    Mb/s  content")?;
            for p in &self.pids {
                writeln!(
                    f,
                    "0x{:04X} {:>9} {:>7} {:>7}  {}",
                    p.pid,
                    p.packets,
                    p.cc_errors,
                    mbps(p.bitrate),
                    p.content
                )?;
            }
        }
        for p in &self.pcr {
            write!(f, "PCR 0x{:04X}: {} PCRs", p.pid, p.count)?;
            if let Some((min, avg, max)) = p.interval_ms {
                write!(f, ", interval {min:.1} / {avg:.1} / {max:.1} ms (min/avg/max), {} over 100 ms", p.over_limit)?;
            }
            write!(f, ", {} discontinuities, {} jumps", p.discontinuities, p.jumps)?;
            if let Some(j) = p.jitter_pp_ms {
                write!(f, ", arrival jitter {j:.2} ms p-p")?;
            }
            writeln!(f)?;
        }
        for p in &self.psi {
            write!(f, "{} 0x{:04X}: {} sections", p.table, p.pid, p.sections)?;
            if let Some((min, max)) = p.interval_ms {
                write!(f, ", every {min:.1} to {max:.1} ms")?;
            }
            writeln!(f)?;
        }
        for k in &self.klv {
            write!(
                f,
                "KLV 0x{:04X} ({}): {} ST 0601 packets in {} PES",
                k.pid,
                if k.sync { "sync" } else { "async" },
                k.packets,
                k.pes
            )?;
            if let Some(r) = k.rate_hz {
                write!(f, ", {r:.2}/s")?;
            }
            write!(
                f,
                "; checksums {} ok, {} bad, {} missing",
                k.checksum_valid, k.checksum_mismatch, k.checksum_missing
            )?;
            if k.malformed > 0 {
                write!(f, ", {} malformed", k.malformed)?;
            }
            if k.damaged_pes > 0 {
                write!(f, ", {} damaged PES", k.damaged_pes)?;
            }
            writeln!(f)?;
            let mut marks: Vec<String> = k.markings.iter().map(|(m, n)| format!("{m} x{n}")).collect();
            if k.unmarked > 0 {
                marks.push(format!("no security set x{}", k.unmarked));
            }
            if k.other_markings > 0 {
                marks.push(format!("other markings x{}", k.other_markings));
            }
            if !marks.is_empty() {
                writeln!(f, "  marking  {}", marks.join(", "))?;
            }
            if let (Some(a), Some(b)) = (&k.first_time, &k.last_time) {
                write!(f, "  tag 2    {a} to {b}")?;
                if k.time_backwards > 0 {
                    write!(f, " ({} steps back)", k.time_backwards)?;
                }
                writeln!(f)?;
            }
            if let Some((min, max)) = k.lead_ms {
                writeln!(f, "  lead     {min:.1} to {max:.1} ms before PTS, {} outside 0 to 1000 ms", k.lead_outside)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pcr_intervals_jumps_and_jitter() {
        let mut t = PcrTrack::default();
        let t0 = Instant::now();
        let ms = |n: u64| Duration::from_millis(n);
        t.on_pcr(1_000_000, false, Some(t0));
        t.on_pcr(1_000_000 + 40 * 27_000, false, Some(t0 + ms(41)));
        t.on_pcr(1_000_000 + 80 * 27_000, false, Some(t0 + ms(79)));
        t.on_pcr(1_000_000 + 200 * 27_000, false, Some(t0 + ms(200)));
        assert_eq!(t.over_limit, 1);
        assert_eq!(t.close_window().map(|j| (j * 1000.0).round()), Some(2000.0)); // -1 .. +1 ms
        t.on_pcr(5, true, Some(t0 + ms(240)));
        t.on_pcr(4, false, Some(t0 + ms(280)));
        assert_eq!((t.count, t.discontinuities, t.jumps), (6, 1, 1));
        assert_eq!(t.interval_ms.get(), Some((40.0, 120.0)));
        assert_eq!(t.jitter_pp_ms.map(|j| (j * 1000.0).round()), Some(2000.0));
    }

    #[test]
    fn monitor_counts_packets_and_survives_garbage() {
        let mut m = Monitor::new().unwrap();
        m.push(&[0x47; 188 * 3], None);
        m.push(&[0x00; 100], None);
        m.finish();
        let s = m.summary();
        assert_eq!(s.packets, 3);
        assert!(s.klv.is_empty());
        assert!(!s.to_json().is_empty());
        assert!(!s.to_string().is_empty());
    }
}
