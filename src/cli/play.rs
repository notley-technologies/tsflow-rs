//! `tsflow play`: real-time playback.

use super::{mbps, parse_pid, stamp, Context};
use std::process::ExitCode;
use std::time::Duration;
use tsflow::clock::SystemClock;
use tsflow::endpoint::{Endpoint, DEFAULT_PACKETS_PER_DATAGRAM, MAX_PACKETS_PER_DATAGRAM};
use tsflow::input::Input;
use tsflow::output::Output;
use tsflow::play::{self, PlayConfig, PlayStats};
use tsflow::{Error, PACKET_SIZE};

/// Bytes of a file read to find its format, PCR PID and bitrate.
const PROBE_BYTES: u64 = 8 << 20;
const DEFAULT_FALLBACK: u64 = 4_000_000;

const AFTER_HELP: &str = "\
Examples:
  tsflow play input.ts udp://239.1.1.1:5000?ttl=4 --loop
  tsflow play input.ts rtp://10.0.0.5:5004
  tsflow play input.m2ts out.ts                  (192-byte M2TS to TS, at wire pace)
  tsflow play input.ts out.ts --no-pace          (as fast as possible)
  tsflow play udp://@:5000 udp://10.0.0.5:5000 --latency 300   (re-pace a live stream)

Each packet leaves at its time on the program clock (PCR interpolation), the
first one --latency after it is read; a datagram leaves when its last packet
is due. When the file loops, the first PCR of each new pass carries a
discontinuity_indicator and continuity counters are rewritten to continue,
so a receiver sees a time base change and no CC errors.";

#[derive(clap::Args)]
#[command(after_help = AFTER_HELP)]
pub struct Args {
    /// Input: a file, - for standard input, or udp:// or rtp:// to re-pace a live stream
    input: Endpoint,

    /// Output: udp://host:port, rtp://host:port, a file, or - for standard output
    output: Endpoint,

    /// Start the file again at its end, forever
    #[arg(long = "loop", conflicts_with = "passes")]
    looped: bool,

    /// Play the file this many times
    #[arg(long, value_name = "N", value_parser = clap::value_parser!(u64).range(1..))]
    passes: Option<u64>,

    /// Output delay in milliseconds (at most 600000); for a live input, the de-jitter buffer
    #[arg(long, value_name = "MS", default_value_t = 200, value_parser = clap::value_parser!(u64).range(0..=600_000))]
    latency: u64,

    /// TS packets per datagram [default: 7, or the output's pkt_size]
    #[arg(long, value_name = "N")]
    burst: Option<usize>,

    /// Transport rate in bit/s before two PCRs are seen, and for a stream without PCR [default: the file's average rate, else 4000000]
    #[arg(long, value_name = "BPS", value_parser = clap::value_parser!(u64).range(1..))]
    rate_fallback: Option<u64>,

    /// Follow this PID's PCR [default: the program with video, else the first PCR seen]
    #[arg(long, value_name = "PID", value_parser = parse_pid)]
    pcr_pid: Option<u16>,

    /// Copy as fast as possible instead of pacing
    #[arg(long)]
    no_pace: bool,
}

pub fn run(a: Args, ctx: &Context) -> Result<ExitCode, Error> {
    if let Some(b) = a.burst {
        if !(1..=MAX_PACKETS_PER_DATAGRAM).contains(&b) {
            return Err(Error::Config(format!("--burst must be 1 to {MAX_PACKETS_PER_DATAGRAM}")));
        }
    }
    let mut input = Input::open(&a.input)?;
    let repeats = a.looped || a.passes.is_some_and(|n| n > 1);
    if repeats && !input.is_seekable() {
        return Err(Error::Config(format!("{}: only a file can loop", input.description())));
    }
    let probe = if input.is_seekable() { Some(play::probe(&mut input, PROBE_BYTES)?) } else { None };
    if let Some(p) = &probe {
        if p.format.is_none() {
            return Err(Error::Config(format!("{}: no transport stream packets found", input.description())));
        }
    }
    let mut output = Output::open(&a.output, a.burst)?;
    let burst = output.packets_per_datagram().or(a.burst).unwrap_or(DEFAULT_PACKETS_PER_DATAGRAM);
    let pcr_pid = a.pcr_pid.or_else(|| probe.as_ref().and_then(|p| p.pcr_pid));
    let fallback = a.rate_fallback.or_else(|| probe.as_ref().and_then(|p| p.bitrate)).unwrap_or(DEFAULT_FALLBACK);
    let mut cfg = PlayConfig::default();
    cfg.latency = Duration::from_millis(a.latency);
    cfg.burst = burst;
    cfg.pace = !a.no_pace;
    cfg.passes = if a.looped { None } else { Some(a.passes.unwrap_or(1)) };
    cfg.fallback_bitrate = fallback;
    cfg.pcr_pid = pcr_pid;

    let mut about = Vec::new();
    if let Some(p) = &probe {
        about.push(p.format.map_or("unknown format", |f| f.name()).to_string());
        if let Some(b) = p.bitrate {
            about.push(format!("{:.2} Mb/s", b as f64 / 1e6));
        }
    }
    about.push(match pcr_pid {
        Some(pid) => format!("PCR PID 0x{pid:04X}"),
        None if probe.is_some() => format!("no PCR found: pacing at {:.2} Mb/s", fallback as f64 / 1e6),
        None => "first PCR PID seen".to_string(),
    });
    let framing = if output.is_rtp() {
        format!("{burst} packets per RTP datagram")
    } else if output.is_network() {
        format!("{burst} packets per datagram")
    } else {
        "file".to_string()
    };
    eprintln!(
        "tsflow play: {} ({}) -> {} ({}), {}{}",
        input.description(),
        about.join(", "),
        output.description(),
        framing,
        if cfg.pace { format!("latency {} ms", a.latency) } else { "no pacing".into() },
        match cfg.passes {
            None => ", looping".to_string(),
            Some(1) => String::new(),
            Some(n) => format!(", {n} passes"),
        }
    );

    let mut last_out = 0u64;
    let mut last_elapsed = Duration::ZERO;
    let quiet = ctx.quiet;
    let mut status = |s: &PlayStats| {
        let window = s.elapsed.saturating_sub(last_elapsed);
        let rate = mbps(s.packets_out - last_out, window);
        last_out = s.packets_out;
        last_elapsed = s.elapsed;
        if quiet {
            return;
        }
        let mut line = format!(
            "{} play {rate:6.2} Mb/s | pass {} | {} packets | stream {:.1} s",
            stamp(s.elapsed),
            s.pass,
            s.packets_out,
            s.position as f64 / 27e6
        );
        if cfg.pace {
            line +=
                &format!(" | queue {} ms | late {:.1} ms", s.depth.as_millis(), s.window_max_late.as_secs_f64() * 1e3);
        }
        if s.reanchors > 0 {
            line += &format!(" | re-anchored x{}", s.reanchors);
        }
        eprintln!("{line}");
    };
    let stats = play::play(&mut input, &mut output, &cfg, &SystemClock, &ctx.stop, &mut status)?;
    let out = output.stats();
    eprintln!(
        "tsflow play: {} {} packets ({:.2} MB) in {} {}, {} pass{}, {:.3} s of stream in {:.3} s{}{}{}",
        if ctx.stop.load(std::sync::atomic::Ordering::Relaxed) { "stopped after" } else { "sent" },
        stats.packets_out,
        (stats.packets_out * PACKET_SIZE as u64) as f64 / 1e6,
        stats.datagrams,
        if output.is_network() { "datagrams" } else { "writes" },
        stats.pass,
        if stats.pass == 1 { "" } else { "es" },
        stats.position as f64 / 27e6,
        stats.elapsed.as_secs_f64(),
        if cfg.pace { format!(", worst lateness {:.1} ms", stats.max_late.as_secs_f64() * 1e3) } else { String::new() },
        if stats.reanchors > 0 { format!(", {} re-anchors", stats.reanchors) } else { String::new() },
        if out.refused > 0 { format!(", {} datagrams refused (nobody listening)", out.refused) } else { String::new() },
    );
    Ok(ExitCode::SUCCESS)
}
