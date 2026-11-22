//! `tsflow live`: inject KLV into a live stream from a control channel.

use super::inject::{parse_rate, parse_time, utc, Cells, Existing, Mode, TimestampMode};
use super::{parse_pid, parse_seconds, stamp, Context};
use misb::mpegts::mux::{InjectEvent, OnOverflow, ProgramSelect, RateMode};
use misb::mux::{Carriage, LiveAnchor, LiveMux, LiveMuxConfig, Rate, VideoTimestampOptions};
use misb::state::StalePolicy;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;
use tsflow::clock::SystemClock;
use tsflow::control::{Control, ControlSource};
use tsflow::endpoint::{Endpoint, DEFAULT_PACKETS_PER_DATAGRAM, MAX_PACKETS_PER_DATAGRAM};
use tsflow::input::Input;
use tsflow::live::{self, LiveConfig, MispClock};
use tsflow::output::Output;
use tsflow::Error;

const AFTER_HELP: &str = "\
Examples:
  tsflow live udp://@:5000 udp://10.0.0.5:5000 --state start.ndjson --control udp://127.0.0.1:6000
  ffmpeg ... -f mpegts - | tsflow live - udp://239.1.1.1:5000 --control unix:/run/klv.sock
  tsflow live srt-relay.ts out.ts --state values.ndjson --anchor first-frame=2024-05-01T12:00:00Z

Control lines (one JSON object per line, misb::json commands; each line is
applied whole, between two packets, and shows on the next video frame):
  {\"set\": {\"SensorLatitude\": 38.8977, \"SensorLongitude\": -77.0365, \"SlantRange\": null}}
  {\"unknown\": [\"SlantRange\"]}    the item is sent as unknown (zero length)
  {\"clear\": [\"MissionId\"]}      the item is no longer sent
  {\"clear_all\": true}  {\"version\": 19}  {\"emit\": true} (a packet now)
Keys are tag numbers, tag names (SensorLatitude) or the standard's names;
values are engineering units, as in tsflow inject --ndjson records.

Each video frame the rate admits gets one ST 0601 packet built from the
current values: tag 2 is the frame's MISP time, and (sync) its PTS is the
frame's PTS, sent right before the frame. The output leaves as the input
arrives: nothing is paced or held (unless --video-timestamps is on, which
holds each frame until its first slice has been read). A file input is
read as fast as possible; play it through tsflow play for real time.

Anchor: wall (the first frame is at the wall-clock time it arrived),
first-frame=TIME, or pts=PTS@TIME; TIME is UTC or misp:MICROSECONDS.
--video-anchor takes MISP time from the video's ST 0604 timestamps when it
carries them.";

#[derive(clap::Args)]
#[command(after_help = AFTER_HELP)]
pub struct Args {
    /// Input stream: udp:// or rtp:// (usually), - for standard input, or a file
    input: Endpoint,

    /// Output: udp:// or rtp://, - for standard output, or a file
    output: Endpoint,

    /// Control lines applied before the stream starts (the initial values)
    #[arg(long, value_name = "FILE")]
    state: Option<PathBuf>,

    /// Where control lines come from while running: -, udp://@:PORT, unix:PATH or a file; may be repeated
    #[arg(long, value_name = "SOURCE")]
    control: Vec<ControlSource>,

    /// KLV carriage
    #[arg(long, value_enum, default_value_t = Mode::Sync)]
    mode: Mode,

    /// Which frames get metadata: frame, every=N, or a rate like 30hz [default: 30hz sync, 10hz async]
    #[arg(long, value_name = "RATE", value_parser = parse_rate)]
    rate: Option<Rate>,

    /// Where MISP time meets the program clock: wall, first-frame=TIME or pts=PTS@TIME
    #[arg(long, value_name = "ANCHOR", default_value = "wall", value_parser = parse_live_anchor)]
    anchor: LiveAnchor,

    /// Take MISP time from the video's ST 0604 timestamps when it carries them
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

    /// A value older than MS milliseconds is stale: left out (or --stale-unknown) [default: never]
    #[arg(long, value_name = "MS", value_parser = clap::value_parser!(u64).range(1..))]
    max_age: Option<u64>,

    /// Send stale values as unknown instead of leaving them out
    #[arg(long, requires = "max_age")]
    stale_unknown: bool,

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

    /// TS packets per datagram written at most [default: 7, or the output's pkt_size]
    #[arg(long, value_name = "N")]
    burst: Option<usize>,

    /// Stop after S seconds
    #[arg(long, value_name = "S", value_parser = parse_seconds)]
    duration: Option<Duration>,

    /// Stop once nothing has arrived for S seconds
    #[arg(long, value_name = "S", value_parser = parse_seconds)]
    idle_exit: Option<Duration>,

    /// Stop once N TS packets have arrived
    #[arg(long, value_name = "N", value_parser = clap::value_parser!(u64).range(1..))]
    count: Option<u64>,

    /// Print the final summary as JSON on standard output
    #[arg(long)]
    json: bool,
}

/// wall, first-frame=TIME or pts=PTS@TIME.
fn parse_live_anchor(s: &str) -> Result<LiveAnchor, String> {
    if s == "wall" {
        return Ok(LiveAnchor::WallClock);
    }
    if let Some(t) = s.strip_prefix("first-frame=") {
        return parse_time(t).map(LiveAnchor::FirstFrame);
    }
    if let Some(rest) = s.strip_prefix("pts=") {
        let (pts, t) = rest.split_once('@').ok_or("pts=PTS@TIME")?;
        let pts: u64 = pts.parse().map_err(|_| format!("\"{pts}\" is not a PTS"))?;
        if pts >= 1 << 33 {
            return Err("a PTS is below 2^33".into());
        }
        return Ok(LiveAnchor::Pts { pts, misp: parse_time(t)? });
    }
    Err(format!("\"{s}\": expected wall, first-frame=TIME or pts=PTS@TIME"))
}

fn config(a: &Args) -> LiveMuxConfig {
    let carriage = match a.mode {
        Mode::Sync => Carriage::Sync,
        Mode::Async => Carriage::Async,
    };
    let mut cfg = LiveMuxConfig::new(carriage);
    if let Some(rate) = a.rate {
        cfg.rate = rate;
    }
    cfg.anchor = a.anchor;
    cfg.video_anchor = a.video_anchor;
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
    cfg
}

pub fn run(a: Args, ctx: &Context) -> Result<ExitCode, Error> {
    if let Some(b) = a.burst {
        if !(1..=MAX_PACKETS_PER_DATAGRAM).contains(&b) {
            return Err(Error::Config(format!("--burst must be 1 to {MAX_PACKETS_PER_DATAGRAM}")));
        }
    }
    let stdin_input = a.input.to_string() == "-";
    if stdin_input && a.control.contains(&ControlSource::Stdin) {
        return Err(Error::Config("the stream and the control lines cannot both come from standard input".into()));
    }
    let mut mux = LiveMux::new(config(&a)).map_err(|e| Error::Config(e.to_string()))?;
    if let Some(ms) = a.max_age {
        let s = mux.state_mut().staleness_mut();
        s.max_age = Some(Duration::from_millis(ms));
        s.policy = if a.stale_unknown { StalePolicy::Unknown } else { StalePolicy::Omit };
    }
    let clock = SystemClock;
    let misp = MispClock::system(&clock)?;
    if let Some(path) = &a.state {
        let text = std::fs::read_to_string(path).map_err(|e| Error::Config(format!("read {}: {e}", path.display())))?;
        let now = misp.at(tsflow::clock::Clock::now(&clock));
        for (n, line) in text.lines().enumerate().filter(|(_, l)| !l.trim().is_empty()) {
            mux.apply_line(line, now).map_err(|e| Error::Config(format!("{} line {}: {e}", path.display(), n + 1)))?;
        }
    }
    let controls = a.control.iter().map(Control::open).collect::<Result<Vec<_>, _>>()?;
    let mut input = Input::open(&a.input)?;
    let mut output = Output::open(&a.output, a.burst)?;
    let burst = output.packets_per_datagram().or(a.burst).unwrap_or(DEFAULT_PACKETS_PER_DATAGRAM);
    let mut cfg = LiveConfig::default();
    cfg.burst = burst;
    cfg.duration = a.duration;
    cfg.idle_exit = a.idle_exit;
    cfg.max_packets = a.count;

    let listening = input.local_addr().map(|addr| format!(" (bound {addr})")).unwrap_or_default();
    let control_list: Vec<String> = controls
        .iter()
        .map(|c| match c.local_addr() {
            Some(addr) => format!("{} (bound {addr})", c.description()),
            None => c.description().to_string(),
        })
        .collect();
    eprintln!(
        "tsflow live: listening on {}{listening} -> {} ({} KLV{}{}){}",
        input.description(),
        output.description(),
        match a.mode {
            Mode::Sync => "sync",
            Mode::Async => "async",
        },
        if a.cbr { ", constant bitrate" } else { "" },
        if a.video_timestamps != TimestampMode::Off { ", ST 0604 in the video" } else { "" },
        if control_list.is_empty() { String::new() } else { format!("; control: {}", control_list.join(", ")) },
    );

    let quiet = ctx.quiet;
    let mut status = |s: &live::LiveStats| {
        if !quiet {
            eprintln!(
                "{} live | {} packets in, {} out | {} frames, {} KLV units | {} control lines",
                stamp(s.elapsed),
                s.packets_in,
                s.packets_out,
                s.mux.frames,
                s.mux.units,
                s.control_lines,
            );
        }
    };
    let mut on_control = |line: &tsflow::control::ControlLine, outcome: &live::ControlOutcome| {
        if let Err(e) = outcome {
            eprintln!("tsflow live: {}: {e} (not applied)", line.origin);
        }
    };
    let result = live::run(
        &mut input,
        &mut output,
        &mut mux,
        &controls,
        &cfg,
        &clock,
        misp,
        &ctx.stop,
        &mut status,
        &mut on_control,
    );
    let target = mux.injector().target().cloned();
    let anchor = mux.anchor();
    let mut warnings = Vec::new();
    while let Some(ev) = mux.pop_event() {
        if let InjectEvent::Warning(w) = ev {
            warnings.push(format!("{w:?}"));
        }
    }
    let (reason, stats) = result?;
    if a.json {
        println!("{}", summary_json(&stats, reason.describe()));
    }
    if let Some(t) = &target {
        eprintln!(
            "tsflow live: program {} (PMT 0x{:04X}), KLV on PID 0x{:04X}{}{}",
            t.program,
            t.pmt_pid,
            t.klv_pid,
            t.service_id.map_or(String::new(), |s| format!(", service {s}")),
            if t.merged { ", joined to the existing sync stream" } else { "" },
        );
    }
    if let Some(an) = anchor {
        eprintln!("tsflow live: anchor PTS {} = {}", misb::mux::wrap_pts(an.pts), utc(an.misp));
    }
    for w in &warnings {
        eprintln!("tsflow live: warning: {w}");
    }
    if let Some(e) = mux.last_encode_error() {
        eprintln!("tsflow live: warning: a snapshot did not encode: {e}");
    }
    let m = &stats.mux;
    eprintln!(
        "tsflow live: {} after {:.1} s; {} packets in, {} out; {} frames, {} KLV units ({} on request, {} with nothing set, {} refused); {} control lines, {} refused; ST 0604 timestamps: {} found, {} written",
        reason.describe(),
        stats.elapsed.as_secs_f64(),
        stats.packets_in,
        stats.packets_out,
        m.frames,
        m.units,
        m.emitted,
        m.empty,
        m.refused,
        stats.control_lines,
        stats.control_errors,
        m.video.found,
        m.video.written,
    );
    Ok(ExitCode::SUCCESS)
}

fn summary_json(s: &live::LiveStats, reason: &str) -> String {
    let m = &s.mux;
    format!(
        "{{\"stop\":\"{reason}\",\"packets_in\":{},\"packets_out\":{},\"frames\":{},\"units\":{},\"emitted\":{},\"empty\":{},\
\"late\":{},\"refused\":{},\"encode_errors\":{},\"control_lines\":{},\"control_errors\":{},\"klv_packets\":{},\
\"psi_packets\":{},\"timestamps_found\":{},\"timestamps_written\":{},\"discontinuities\":{}}}",
        s.packets_in,
        s.packets_out,
        m.frames,
        m.units,
        m.emitted,
        m.empty,
        m.late,
        m.refused,
        m.encode_errors,
        s.control_lines,
        s.control_errors,
        m.inject.klv_packets,
        m.inject.psi_packets,
        m.video.found,
        m.video.written,
        m.discontinuities,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use misb::MispTime;

    #[test]
    fn anchors() {
        assert_eq!(parse_live_anchor("wall"), Ok(LiveAnchor::WallClock));
        assert_eq!(parse_live_anchor("first-frame=misp:42"), Ok(LiveAnchor::FirstFrame(MispTime::from_micros(42))));
        assert_eq!(
            parse_live_anchor("pts=900000@misp:7"),
            Ok(LiveAnchor::Pts { pts: 900_000, misp: MispTime::from_micros(7) })
        );
        assert!(parse_live_anchor("source-start").is_err());
    }
}
