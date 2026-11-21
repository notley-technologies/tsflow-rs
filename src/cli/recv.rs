//! `tsflow recv`: receive and monitor.

use super::{checks_exit, mbps, parse_seconds, stamp, Context};
use std::process::ExitCode;
use std::time::{Duration, Instant};
use tsflow::clock::SystemClock;
use tsflow::endpoint::Endpoint;
use tsflow::expect::{self, Expectation};
use tsflow::input::Input;
use tsflow::monitor::{Monitor, WindowStats};
use tsflow::output::Output;
use tsflow::recv::{self, RecvConfig};
use tsflow::Error;

const AFTER_HELP: &str = "\
Examples:
  tsflow recv udp://@:5000 --forward udp://127.0.0.1:5001
  tsflow recv rtp://@239.1.1.1:5004?iface=10.0.0.2 --out recording.ts
  tsflow recv udp://@:5000 --idle-exit 2 --expect klv-marking=US//UNCLASSIFIED --expect no-cc-errors --json

Expectations (repeat --expect; exit status 1 when one fails):
  no-cc-errors, max-cc-errors=N, min-packets=N, klv-streams=N,
  klv-marking=MARKING, min-klv-rate=HZ, klv-checksums, max-pcr-interval=MS,
  max-pcr-jitter=MS, max-psi-interval=MS, no-rtp-loss

Status lines go to standard error once a second; the summary (or --json)
goes to standard output when the input ends, goes quiet (--idle-exit), the
--duration passes, or on Ctrl-C.";

#[derive(clap::Args)]
#[command(after_help = AFTER_HELP)]
pub struct Args {
    /// Input: udp://@:port, udp://@group:port, rtp://..., a file, or - for standard input
    input: Endpoint,

    /// Pass every datagram on as received (before analysis) to this output, for a player such as VLC
    #[arg(long, value_name = "OUTPUT")]
    forward: Option<Endpoint>,

    /// Record the received TS packets to this file
    #[arg(long, value_name = "FILE")]
    out: Option<Endpoint>,

    /// Stop once nothing has arrived for S seconds (counted from the first datagram)
    #[arg(long, value_name = "S", value_parser = parse_seconds)]
    idle_exit: Option<Duration>,

    /// Stop after S seconds
    #[arg(long, value_name = "S", value_parser = parse_seconds)]
    duration: Option<Duration>,

    /// Fail (exit status 1) unless this holds; may be repeated
    #[arg(long, value_name = "EXPECTATION")]
    expect: Vec<Expectation>,

    /// Print the summary and checks as one JSON object on standard output
    #[arg(long)]
    json: bool,
}

pub fn run(a: Args, ctx: &Context) -> Result<ExitCode, Error> {
    if let Some(Endpoint::Net(_)) = &a.out {
        return Err(Error::Config("--out records to a file; use --forward to send the stream on".into()));
    }
    let mut input = Input::open(&a.input)?;
    let mut forward = a.forward.as_ref().map(|e| Output::open(e, None)).transpose()?;
    let mut record = a.out.as_ref().map(|e| Output::open(e, None)).transpose()?;
    let mut monitor = Monitor::new()?;
    match input.local_addr() {
        Some(addr) => {
            let stats = input.stats();
            eprintln!(
                "tsflow recv: listening on {} (bound {addr}{})",
                input.description(),
                stats.recv_buffer.map(|b| format!(", receive buffer {} KiB", b / 1024)).unwrap_or_default()
            );
        }
        None => eprintln!("tsflow recv: reading {}", input.description()),
    }
    if let Some(f) = &forward {
        eprintln!("tsflow recv: forwarding to {}", f.description());
    }
    let mut cfg = RecvConfig::default();
    cfg.idle_exit = a.idle_exit;
    cfg.duration = a.duration;
    let start = Instant::now();
    let mut revision = monitor.revision();
    let quiet = ctx.quiet;
    let description = input.description().to_string();
    let mut status = |m: &mut Monitor, now: Instant| {
        let w = m.take_window(now);
        if quiet {
            return;
        }
        if m.revision() != revision {
            revision = m.revision();
            print_programs(m);
        }
        eprintln!("{}", status_line(&w, now.saturating_duration_since(start), m.packets(), &description));
    };
    let reason = recv::run(
        &mut input,
        &mut monitor,
        forward.as_mut(),
        record.as_mut(),
        &cfg,
        &SystemClock,
        &ctx.stop,
        &mut status,
    )?;
    let summary = monitor.summary();
    let (checks, ok) = expect::evaluate(&a.expect, &summary);
    let stopped = reason.describe();
    if a.json {
        println!("{}", expect::report_json(&summary, &checks, Some(stopped)));
    } else {
        println!("tsflow recv: {} ({stopped})", input.description());
        print!("{summary}");
        for c in &checks {
            println!("{c}");
        }
    }
    Ok(checks_exit(ok))
}

fn print_programs(m: &Monitor) {
    for p in &m.report().programs {
        let streams: Vec<String> =
            p.streams.iter().map(|s| format!("0x{:04X} {} {}", s.pid, s.kind(), s.codec.name())).collect();
        eprintln!(
            "tsflow recv: program {} (PMT 0x{:04X}{}): {}",
            p.number,
            p.pmt_pid,
            p.pcr_pid.map(|pid| format!(", PCR 0x{pid:04X}")).unwrap_or_default(),
            if streams.is_empty() { "no streams".to_string() } else { streams.join(", ") }
        );
    }
}

fn status_line(w: &WindowStats, elapsed: Duration, total: u64, input: &str) -> String {
    if total == 0 {
        return format!("{} waiting for data on {input}", stamp(elapsed));
    }
    let mut line = format!(
        "{} {:6.2} Mb/s | CC errors {}{}",
        stamp(elapsed),
        mbps(w.packets, w.elapsed),
        w.total_cc_errors,
        if w.cc_errors > 0 { format!(" (+{})", w.cc_errors) } else { String::new() }
    );
    if let Some(j) = w.pcr_jitter_ms {
        line += &format!(" | PCR jitter {j:.1} ms");
    }
    let secs = w.elapsed.as_secs_f64();
    for k in &w.klv {
        line += &format!(
            " | KLV 0x{:04X} {:.0}/s {}",
            k.pid,
            if secs > 0.0 { k.packets as f64 / secs } else { 0.0 },
            k.marking.as_deref().unwrap_or("unmarked")
        );
        if k.checksum_errors > 0 {
            line += &format!(" ({} bad checksums)", k.checksum_errors);
        }
    }
    if w.rtp_lost > 0 {
        line += &format!(" | RTP lost {}", w.rtp_lost);
    }
    line
}
