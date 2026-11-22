//! `tsflow inject`: put a metadata file into a transport stream.

use super::{parse_pid, Context, EXIT_CHECK_FAILED};
use misb::mpegts::mux::{CellNumbering, ExistingKlv, InjectEvent, OnOverflow, ProgramSelect, RateMode, RecordFormat};
use misb::mux::{
    AnchorSpec, Carriage, FileMux, FileMuxConfig, FileMuxStats, Rate, Sample, VideoTimestampOptions, VideoTimestamps,
};
use misb::{LeapSeconds, MispTime, UtcDateTime};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;
use tsflow::clock::SystemClock;
use tsflow::endpoint::{Endpoint, DEFAULT_PACKETS_PER_DATAGRAM, MAX_PACKETS_PER_DATAGRAM};
use tsflow::framer::PacketFormat;
use tsflow::inject::{InjectStage, KlvFile, NdjsonFile, SourceError};
use tsflow::input::Input;
use tsflow::output::Output;
use tsflow::play::{self, PlayConfig, Stage};
use tsflow::Error;

const PROBE_BYTES: u64 = 8 << 20;
const DEFAULT_FALLBACK: u64 = 4_000_000;

const AFTER_HELP: &str = "\
Examples:
  tsflow inject video.ts out.ts --klv flight.klv --anchor first-frame=2024-05-01T12:00:00Z
  tsflow inject video.ts out.ts --ndjson track.ndjson --rate 30hz
  tsflow inject video.ts udp://239.1.1.1:5000 --klv flight.klv     (paced, to the network)
  tsflow inject udp://@:5000 udp://10.0.0.5:5000 --ndjson track.ndjson --anchor pts=900000@misp:1714564829000000

Metadata:
  --klv FILE     recorded ST 0601 packets, written byte for byte at their own
                 tag 2 time (PTS = the anchor applied to tag 2); damaged
                 packets and packets without tag 2 are skipped and counted
  --ndjson FILE  authoring records, one JSON object per line (misb::json):
                 each at its own time with --rate explicit, or sampled at
                 video frames with tag 2 = the frame's time

Anchor (where MISP time meets the program clock):
  source-start               the first video frame is at the first sample's time
  first-frame=TIME           the first video frame is at TIME
  pts=PTS@TIME               that 90 kHz PTS is at TIME
  TIME is UTC (2024-05-01T12:00:00.5Z, converted with the leap second
  table) or misp:MICROSECONDS (MISP time, the ST 0601 tag 2 count).

Rate: explicit (every sample at its own time), frame (every video frame),
every=N (every Nth frame), or a rate such as 30hz or 10hz (at most that
many per second, on video frames). With a frame rate and --interpolate,
each item moves between the records that carry it (linear, the short way
around for angles, along the great circle for positions, held for text and
codes; a record's \"interpolate\" object overrides per item).

Video timestamps (MISB ST 0604, in H.264/H.265 SEI): --video-anchor takes
MISP time from the input's timestamps when it has them (the best anchor);
--video-timestamps add|rewrite writes one into every access unit, with
the anchor's time, so video and metadata share a clock.

Every input packet goes out unchanged except the PAT, the target PMT
(rewritten with the KLV stream, version + 1) and the KLV PID. Sync KLV
arrives within 990 ms to 1 ms before its PTS, right before its video
frame (ISO/IEC 13818-1, MISB ST 1402.2); late samples are counted, never
sent late. Exit status 1 with --strict when any sample was not inserted.";

#[derive(Clone, Copy, clap::ValueEnum)]
pub(super) enum Mode {
    /// Synchronous KLV: stream_type 0x15, a PTS on every packet
    Sync,
    /// Asynchronous KLV: stream_type 0x06 (SMPTE RP 217), no PTS
    Async,
}

#[derive(Clone, Copy, clap::ValueEnum)]
pub(super) enum Existing {
    /// Keep it; sync joins an existing sync stream as a new metadata service
    Keep,
    /// Remove the program's KLV streams
    Replace,
    /// Stop with an error
    Fail,
}

impl From<Existing> for ExistingKlv {
    fn from(e: Existing) -> ExistingKlv {
        match e {
            Existing::Keep => ExistingKlv::Keep,
            Existing::Replace => ExistingKlv::Replace,
            Existing::Fail => ExistingKlv::Fail,
        }
    }
}

/// How synchronous KLV numbers its metadata access unit cells.
#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub(super) enum Cells {
    /// One counter per stream, +1 per cell (ISO/IEC 13818-1, MISB ST 1402)
    Stream,
    /// From 0 in each access unit: a workaround for receivers that require it, such as MediaMTX
    AccessUnit,
}

impl From<Cells> for CellNumbering {
    fn from(c: Cells) -> CellNumbering {
        match c {
            Cells::Stream => CellNumbering::PerStream,
            Cells::AccessUnit => CellNumbering::PerAccessUnit,
        }
    }
}

/// MISB ST 0604 timestamps in the video.
#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub(super) enum TimestampMode {
    /// Never change the video
    Off,
    /// Add a timestamp to access units without one
    Add,
    /// Every access unit gets the muxer's time; existing timestamps are replaced
    Rewrite,
}

impl From<TimestampMode> for VideoTimestamps {
    fn from(m: TimestampMode) -> VideoTimestamps {
        match m {
            TimestampMode::Off => VideoTimestamps::Off,
            TimestampMode::Add => VideoTimestamps::AddWhenMissing,
            TimestampMode::Rewrite => VideoTimestamps::Rewrite,
        }
    }
}

#[derive(clap::Args)]
#[command(after_help = AFTER_HELP)]
pub struct Args {
    /// Input stream: a file, - for standard input, or udp:// or rtp://
    input: Endpoint,

    /// Output: a file, - for standard output, or udp:// or rtp://
    output: Endpoint,

    /// Recorded ST 0601 packets (.klv), - for standard input
    #[arg(long, value_name = "FILE", conflicts_with = "ndjson", required_unless_present = "ndjson")]
    klv: Option<PathBuf>,

    /// NDJSON authoring records, - for standard input
    #[arg(long, value_name = "FILE")]
    ndjson: Option<PathBuf>,

    /// KLV carriage
    #[arg(long, value_enum, default_value_t = Mode::Sync)]
    mode: Mode,

    /// Which samples go in: explicit, frame, every=N, or a rate like 30hz
    #[arg(long, value_name = "RATE", default_value = "explicit", value_parser = parse_rate)]
    rate: Rate,

    /// Where MISP time meets the program clock: source-start, first-frame=TIME or pts=PTS@TIME
    #[arg(long, value_name = "ANCHOR", default_value = "source-start", value_parser = parse_anchor)]
    anchor: AnchorSpec,

    /// Move recorded packets' PTS to the nearest video frame (tag 2 unchanged)
    #[arg(long)]
    snap: bool,

    /// When sampling at frames, how long a sample stays valid, in ms (0: until the next sample)
    #[arg(long, value_name = "MS", default_value_t = 3000)]
    hold: u32,

    /// When sampling at frames, interpolate between samples (linear, the short way for angles,
    /// great circle for positions; per-record "interpolate" hints) instead of holding the latest
    #[arg(long)]
    interpolate: bool,

    /// Take MISP time from the video's ST 0604 timestamps when it carries them (ahead of --anchor)
    #[arg(long)]
    video_anchor: bool,

    /// Write ST 0604 timestamps into H.264/H.265 video: off, add (where missing) or rewrite
    #[arg(long, value_enum, default_value_t = TimestampMode::Off)]
    video_timestamps: TimestampMode,

    /// Written timestamps assert lock to an absolute time reference (ST 0603 Time Status)
    #[arg(long)]
    time_locked: bool,

    /// H.265: write nanosecond ST 0604 timestamps
    #[arg(long)]
    nanoseconds: bool,

    /// What to do when the program already carries KLV
    #[arg(long, value_enum, default_value_t = Existing::Keep)]
    existing: Existing,

    /// Program to inject into [default: the first with video]
    #[arg(long, value_name = "N")]
    program: Option<u16>,

    /// PID for the KLV stream [default: a free PID]
    #[arg(long, value_name = "PID", value_parser = parse_pid)]
    pid: Option<u16>,

    /// metadata_service_id [default: the lowest unused]
    #[arg(long, value_name = "N")]
    service_id: Option<u8>,

    /// How sync KLV numbers its metadata cells
    #[arg(long, value_enum, default_value_t = Cells::Stream)]
    cell_numbering: Cells,

    /// Keep the packet count and every PCR in place: KLV and tables replace null packets
    #[arg(long)]
    cbr: bool,

    /// With --cbr, drop units that find no room instead of stopping
    #[arg(long, requires = "cbr")]
    drop_on_overflow: bool,

    /// Longest gap between PAT (and PMT) copies, in ms
    #[arg(long, value_name = "MS", default_value_t = 100)]
    psi_interval: u32,

    /// Pace the output by the PCR [default: when the output is a socket]
    #[arg(long, conflicts_with = "no_pace")]
    pace: bool,

    /// Write as fast as possible
    #[arg(long)]
    no_pace: bool,

    /// Output delay in milliseconds when pacing
    #[arg(long, value_name = "MS", default_value_t = 200, value_parser = clap::value_parser!(u64).range(0..=600_000))]
    latency: u64,

    /// TS packets per datagram [default: 7, or the output's pkt_size]
    #[arg(long, value_name = "N")]
    burst: Option<usize>,

    /// Exit with status 1 when any sample was skipped or dropped
    #[arg(long)]
    strict: bool,

    /// Print the final summary as JSON on standard output
    #[arg(long)]
    json: bool,
}

/// TIME: UTC, or misp:MICROSECONDS.
pub(super) fn parse_time(s: &str) -> Result<MispTime, String> {
    if let Some(us) = s.strip_prefix("misp:") {
        return us.parse().map(MispTime::from_micros).map_err(|_| format!("\"{us}\" is not a count of microseconds"));
    }
    let utc: UtcDateTime = s
        .parse()
        .map_err(|e| format!("\"{s}\": {e} (expected UTC like 2024-05-01T12:00:00Z, or misp:MICROSECONDS)"))?;
    MispTime::from_utc(utc, LeapSeconds::builtin()).ok_or_else(|| format!("\"{s}\" is before the MISP epoch"))
}

fn parse_anchor(s: &str) -> Result<AnchorSpec, String> {
    if s == "source-start" {
        return Ok(AnchorSpec::SourceStart);
    }
    if let Some(t) = s.strip_prefix("first-frame=") {
        return parse_time(t).map(AnchorSpec::FirstFrame);
    }
    if let Some(rest) = s.strip_prefix("pts=") {
        let (pts, t) = rest.split_once('@').ok_or("pts=PTS@TIME")?;
        let pts: u64 = pts.parse().map_err(|_| format!("\"{pts}\" is not a PTS"))?;
        if pts >= 1 << 33 {
            return Err("a PTS is below 2^33".into());
        }
        return Ok(AnchorSpec::Pts { pts, misp: parse_time(t)? });
    }
    Err(format!("\"{s}\": expected source-start, first-frame=TIME or pts=PTS@TIME"))
}

pub(super) fn parse_rate(s: &str) -> Result<Rate, String> {
    let lower = s.to_ascii_lowercase();
    let rate = match lower.as_str() {
        "explicit" => Rate::Explicit,
        "frame" => Rate::EveryFrame,
        _ => {
            if let Some(n) = lower.strip_prefix("every=") {
                Rate::EveryNthFrame(n.parse().map_err(|_| format!("\"{n}\" is not a frame count"))?)
            } else if let Some(hz) = lower.strip_suffix("hz") {
                Rate::Hz(hz.parse().map_err(|_| format!("\"{hz}\" is not a rate"))?)
            } else {
                return Err(format!("\"{s}\": expected explicit, frame, every=N or a rate like 30hz"));
            }
        }
    };
    rate.validate().map_err(|e| e.to_string())?;
    Ok(rate)
}

fn open(path: &PathBuf) -> Result<Box<dyn Read>, Error> {
    if path.as_os_str() == "-" {
        return Ok(Box::new(std::io::stdin()));
    }
    File::open(path)
        .map(|f| Box::new(f) as Box<dyn Read>)
        .map_err(|e| Error::Config(format!("open {}: {e}", path.display())))
}

pub(super) fn utc(t: MispTime) -> String {
    t.to_utc(LeapSeconds::builtin()).map_or_else(|| format!("misp:{}", t.as_micros()), |u| u.to_string())
}

pub fn run(a: Args, ctx: &Context) -> Result<ExitCode, Error> {
    if let Some(b) = a.burst {
        if !(1..=MAX_PACKETS_PER_DATAGRAM).contains(&b) {
            return Err(Error::Config(format!("--burst must be 1 to {MAX_PACKETS_PER_DATAGRAM}")));
        }
    }
    let metadata = a.klv.as_ref().or(a.ndjson.as_ref()).expect("clap requires one");
    if metadata.as_os_str() == "-" && a.input.to_string() == "-" {
        return Err(Error::Config("the stream and the metadata cannot both come from standard input".into()));
    }
    let mut input = Input::open(&a.input)?;
    let probe = if input.is_seekable() { Some(play::probe(&mut input, PROBE_BYTES)?) } else { None };
    if let Some(p) = &probe {
        if p.format.is_none() {
            return Err(Error::Config(format!("{}: no transport stream packets found", input.description())));
        }
    }

    let carriage = match a.mode {
        Mode::Sync => Carriage::Sync,
        Mode::Async => Carriage::Async,
    };
    let mut cfg = FileMuxConfig::new(carriage);
    cfg.schedule.rate = a.rate;
    cfg.schedule.anchor = a.anchor;
    cfg.schedule.snap_to_frames = a.snap;
    cfg.schedule.max_hold_ms = (a.hold > 0).then_some(a.hold);
    if a.interpolate {
        cfg.schedule.interpolate = Some(misb::track::TrackConfig::new());
    }
    cfg.schedule.video_anchor = a.video_anchor;
    let mut ts = VideoTimestampOptions::new(a.video_timestamps.into());
    ts.locked = a.time_locked.then_some(true);
    ts.nanoseconds = a.nanoseconds;
    cfg.video_timestamps = ts;
    let mut inject = cfg.inject.clone().with_psi_interval_ms(a.psi_interval).with_existing_klv(a.existing.into());
    inject.sync.cell_numbering = a.cell_numbering.into();
    if let Some(n) = a.program {
        inject = inject.with_program(ProgramSelect::Number(n));
    }
    if let Some(pid) = a.pid {
        inject = inject.with_pid(pid);
    }
    if let Some(id) = a.service_id {
        inject = inject.with_service_id(id);
    }
    if a.cbr {
        let on_overflow = if a.drop_on_overflow { OnOverflow::DropOldest } else { OnOverflow::Fail };
        inject = inject.with_rate(RateMode::Cbr { on_overflow });
    }
    cfg.inject = inject;

    let name = metadata.display().to_string();
    let reader = open(metadata)?;
    let (source, source_error): (Box<dyn Iterator<Item = Sample>>, SourceError) = if a.klv.is_some() {
        let f = KlvFile::new(reader, name.clone());
        let e = f.error();
        (Box::new(f), e)
    } else {
        let f = NdjsonFile::new(Box::new(BufReader::new(reader)), name.clone());
        let e = f.error();
        (Box::new(f), e)
    };
    // An M2TS file written to a file keeps its 192-byte records: the
    // injector takes them whole and stamps what it inserts between its
    // neighbours' arrival times.
    let m2ts = probe.as_ref().is_some_and(|p| p.format == Some(PacketFormat::M2ts))
        && !a.pace
        && !matches!(a.output, Endpoint::Net(_));
    if m2ts {
        cfg.inject = cfg.inject.clone().with_record_format(RecordFormat::M2ts);
        let Endpoint::File(in_path) = &a.input else {
            return Err(Error::Config("M2TS input must be a file".into()));
        };
        let mut mux = FileMux::new(cfg, source).map_err(|e| Error::Inject(e.to_string()))?;
        eprintln!(
            "tsflow inject: {} + {} -> {} (192-byte M2TS records kept, no pacing)",
            in_path.display(),
            name,
            a.output
        );
        let result = inject_records(in_path, &a.output, &mut mux, &source_error);
        let summary = Summary::of(&mut mux);
        let (records_in, records_out, stats) = result?;
        return finish(&a, summary, stats, records_in, records_out);
    }
    let mut stage = InjectStage::new(cfg, source, source_error)?;

    let mut output = Output::open(&a.output, a.burst)?;
    let burst = output.packets_per_datagram().or(a.burst).unwrap_or(DEFAULT_PACKETS_PER_DATAGRAM);
    let mut pcfg = PlayConfig::default();
    pcfg.pace = if a.pace {
        true
    } else if a.no_pace {
        false
    } else {
        output.is_network()
    };
    pcfg.latency = Duration::from_millis(a.latency);
    pcfg.burst = burst;
    pcfg.pcr_pid = probe.as_ref().and_then(|p| p.pcr_pid);
    pcfg.fallback_bitrate = probe.as_ref().and_then(|p| p.bitrate).unwrap_or(DEFAULT_FALLBACK);

    eprintln!(
        "tsflow inject: {} + {} -> {} ({} KLV, {}{})",
        input.description(),
        name,
        output.description(),
        match a.mode {
            Mode::Sync => "sync",
            Mode::Async => "async",
        },
        if pcfg.pace { format!("paced, latency {} ms", a.latency) } else { "no pacing".into() },
        if a.cbr { ", constant bitrate" } else { "" },
    );
    let quiet = ctx.quiet;
    let mut status = |s: &play::PlayStats| {
        if !quiet {
            eprintln!(
                "{} inject | {} packets | stream {:.1} s",
                super::stamp(s.elapsed),
                s.packets_out,
                s.position as f64 / 27e6
            );
        }
    };
    let result = play::play_with(
        &mut input,
        &mut output,
        &pcfg,
        &SystemClock,
        &ctx.stop,
        Some(&mut stage as &mut dyn Stage),
        &mut status,
    );
    // Events say what the injector decided; print them even after an error.
    let summary = Summary::of(stage.mux());
    let played = result?;
    finish(&a, summary, stage.stats(), played.packets_in, played.packets_out)
}

/// What the muxer decided, collected before its result is looked at.
struct Summary {
    target: Option<misb::mpegts::mux::Target>,
    anchor: Option<misb::mux::Anchor>,
    warnings: Vec<String>,
}

impl Summary {
    fn of<S: Iterator<Item = Sample>>(mux: &mut FileMux<S>) -> Summary {
        let mut warnings = Vec::new();
        while let Some(ev) = mux.pop_event() {
            if let InjectEvent::Warning(w) = ev {
                warnings.push(format!("{w:?}"));
            }
        }
        Summary { target: mux.injector().target().cloned(), anchor: mux.anchor(), warnings }
    }
}

/// Read 192-byte records from `input`, inject, write records to `output`.
/// Returns records in, records out, and the final counters.
fn inject_records<S: Iterator<Item = Sample>>(
    input: &std::path::Path,
    output: &Endpoint,
    mux: &mut FileMux<S>,
    source_error: &SourceError,
) -> Result<(u64, u64, FileMuxStats), Error> {
    let mut reader = File::open(input).map_err(|e| Error::Config(format!("open {}: {e}", input.display())))?;
    let mut writer: Box<dyn Write> = match output {
        Endpoint::File(p) => Box::new(BufWriter::new(
            File::create(p).map_err(|e| Error::Config(format!("create {}: {e}", p.display())))?,
        )),
        _ => Box::new(BufWriter::new(std::io::stdout())),
    };
    let (mut records_in, mut records_out) = (0u64, 0u64);
    let mut buf = vec![0u8; 192 * 1024];
    let mut out = Vec::with_capacity(buf.len() + 64 * 192);
    loop {
        let n = reader.read(&mut buf).map_err(|e| Error::Config(format!("read {}: {e}", input.display())))?;
        if n == 0 {
            break;
        }
        records_in += (n / 192) as u64;
        let r = mux.push(&buf[..n], &mut out);
        records_out += (out.len() / 192) as u64;
        writer.write_all(&out).map_err(|e| Error::Config(format!("write {output}: {e}")))?;
        out.clear();
        r.map_err(|e| Error::Inject(e.to_string()))?;
        if let Some(e) = source_error.get() {
            return Err(Error::Inject(e));
        }
    }
    let stats = mux.finish(&mut out);
    records_out += (out.len() / 192) as u64;
    writer.write_all(&out).and_then(|()| writer.flush()).map_err(|e| Error::Config(format!("write {output}: {e}")))?;
    let stats = stats.map_err(|e| Error::Inject(e.to_string()))?;
    if let Some(e) = source_error.get() {
        return Err(Error::Inject(e));
    }
    Ok((records_in, records_out, stats))
}

/// Print the outcome and pick the exit status.
fn finish(
    a: &Args,
    summary: Summary,
    stats: FileMuxStats,
    packets_in: u64,
    packets_out: u64,
) -> Result<ExitCode, Error> {
    let Summary { target, anchor, warnings } = summary;
    let played = Played { packets_in, packets_out };
    let skipped = skipped(&stats);

    if a.json {
        println!("{}", summary_json(&stats, target.as_ref(), anchor, played.packets_in, played.packets_out));
    }
    if let Some(t) = &target {
        eprintln!(
            "tsflow inject: program {} (PMT 0x{:04X}), KLV on PID 0x{:04X}{}{}",
            t.program,
            t.pmt_pid,
            t.klv_pid,
            t.service_id.map_or(String::new(), |s| format!(", service {s}")),
            if t.merged { ", joined to the existing sync stream" } else { "" },
        );
    }
    if let Some(an) = anchor {
        eprintln!("tsflow inject: anchor PTS {} = {}", misb::mux::wrap_pts(an.pts), utc(an.misp));
    }
    for w in &warnings {
        eprintln!("tsflow inject: warning: {w}");
    }
    let s = &stats.schedule;
    let i = &stats.inject;
    eprintln!(
        "tsflow inject: {} KLV units inserted from {} samples; {} packets in, {} out (+{} KLV, {} PSI); skipped: {} late, {} out of order, {} stale, {} unused; dropped by the injector: {} late, {} overflow, {} discontinuity, {} unplaced",
        i.units_inserted,
        s.samples,
        played.packets_in,
        played.packets_out,
        i.klv_packets,
        i.psi_packets,
        s.late,
        s.out_of_order,
        s.stale,
        s.unused,
        i.dropped_late,
        i.dropped_overflow,
        i.dropped_discontinuity,
        i.dropped_unplaced,
    );
    if a.strict && skipped > 0 {
        eprintln!("tsflow inject: --strict: {skipped} samples were not inserted");
        return Ok(ExitCode::from(EXIT_CHECK_FAILED));
    }
    Ok(ExitCode::SUCCESS)
}

/// Packets (or records) in and out.
struct Played {
    packets_in: u64,
    packets_out: u64,
}

/// Samples that did not make it into the output.
fn skipped(stats: &FileMuxStats) -> u64 {
    let (s, i) = (&stats.schedule, &stats.inject);
    s.late
        + s.out_of_order
        + s.unused
        + i.dropped_late
        + i.dropped_overflow
        + i.dropped_discontinuity
        + i.dropped_unplaced
}

fn summary_json(
    stats: &FileMuxStats,
    target: Option<&misb::mpegts::mux::Target>,
    anchor: Option<misb::mux::Anchor>,
    packets_in: u64,
    packets_out: u64,
) -> String {
    let (s, i) = (&stats.schedule, &stats.inject);
    let target = target.map_or("null".to_string(), |t| {
        format!(
            "{{\"program\":{},\"pmt_pid\":{},\"klv_pid\":{},\"service_id\":{},\"merged\":{}}}",
            t.program,
            t.pmt_pid,
            t.klv_pid,
            t.service_id.map_or("null".to_string(), |v| v.to_string()),
            t.merged
        )
    });
    let anchor = anchor.map_or("null".to_string(), |an| {
        format!("{{\"pts\":{},\"misp_us\":{}}}", misb::mux::wrap_pts(an.pts), an.misp.as_micros())
    });
    format!(
        "{{\"packets_in\":{packets_in},\"packets_out\":{packets_out},\"units_inserted\":{},\"klv_packets\":{},\"psi_packets\":{},\
\"samples\":{},\"late\":{},\"out_of_order\":{},\"stale\":{},\"unused\":{},\"snapped\":{},\
\"dropped_late\":{},\"dropped_overflow\":{},\"dropped_discontinuity\":{},\"dropped_unplaced\":{},\
\"skipped\":{},\"target\":{target},\"anchor\":{anchor}}}",
        i.units_inserted,
        i.klv_packets,
        i.psi_packets,
        s.samples,
        s.late,
        s.out_of_order,
        s.stale,
        s.unused,
        s.snapped,
        i.dropped_late,
        i.dropped_overflow,
        i.dropped_discontinuity,
        i.dropped_unplaced,
        skipped(stats),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn anchors_rates_and_times() {
        assert_eq!(parse_anchor("source-start"), Ok(AnchorSpec::SourceStart));
        assert_eq!(parse_anchor("first-frame=misp:42"), Ok(AnchorSpec::FirstFrame(MispTime::from_micros(42))));
        // The handbook example: 2022-07-12T09:05:03.000700Z.
        assert_eq!(
            parse_anchor("pts=900000@2022-07-12T09:05:03.000700Z"),
            Ok(AnchorSpec::Pts { pts: 900_000, misp: MispTime::from_micros(1_657_616_732_000_618) })
        );
        assert!(parse_anchor("pts=900000").is_err());
        assert!(parse_anchor("pts=8589934592@misp:1").is_err());
        assert!(parse_anchor("first-frame=yesterday").is_err());
        assert_eq!(parse_rate("explicit"), Ok(Rate::Explicit));
        assert_eq!(parse_rate("frame"), Ok(Rate::EveryFrame));
        assert_eq!(parse_rate("every=3"), Ok(Rate::EveryNthFrame(3)));
        assert_eq!(parse_rate("30Hz"), Ok(Rate::Hz(30.0)));
        assert!(parse_rate("every=0").is_err());
        assert!(parse_rate("0hz").is_err());
        assert!(parse_rate("fast").is_err());
    }
}
