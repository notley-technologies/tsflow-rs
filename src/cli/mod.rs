//! Subcommands and what they share.
//!
//! Adding a subcommand: write a module with an `Args` struct deriving
//! `clap::Args` and a `run(args, &Context) -> Result<ExitCode, Error>`
//! function, then add a variant to [`Command`] and a line to the match in
//! [`main`]. Library code does the work; the module parses, prints and
//! picks the exit status.

mod analyze;
mod inject;
mod live;
mod play;
mod recv;

use clap::{Parser, Subcommand};
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Every check passed (or none was asked for).
pub const EXIT_OK: u8 = 0;
/// An `--expect` check failed.
pub const EXIT_CHECK_FAILED: u8 = 1;
/// A usage error (from clap) or a runtime error.
pub const EXIT_ERROR: u8 = 2;

const AFTER_HELP: &str = "\
Endpoints:
  file.ts                    a file (188, 192 or 204-byte packets when reading)
  -                          standard input or output
  udp://host:port            send to host:port
  udp://@:port               receive on port, every interface
  udp://@239.1.1.1:port      receive multicast (join the group)
  rtp://...                  the same with RTP (RFC 3550, payload type 33);
                             inputs detect RTP or raw TS by themselves
  Options after ?, joined by &: ttl=N, iface=A (or localaddr=A), pkt_size=N,
  buffer_size=N, source=A (SSM), reuse=0|1, loop=0|1, ssrc=N, pt=N

Exit status: 0 success, 1 an --expect check failed, 2 usage or runtime error.
Ctrl-C stops cleanly with a summary; a second Ctrl-C exits at once.";

#[derive(Parser)]
#[command(
    name = "tsflow",
    version,
    about = "MPEG transport stream toolkit: real-time playback, UDP/RTP send and receive, monitoring, analysis, KLV injection (files and live)",
    after_help = AFTER_HELP
)]
struct Cli {
    /// Print no status lines (the final summary and errors still appear)
    #[arg(short, long, global = true)]
    quiet: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Play a stream in real time, paced by its PCR, to UDP/RTP or a file
    Play(play::Args),
    /// Receive and monitor a stream, optionally forwarding and recording it
    Recv(recv::Args),
    /// Report on a file: programs, continuity, PCR, PSI, KLV
    Analyze(analyze::Args),
    /// Put a .klv or NDJSON metadata file into a stream as MISB ST 1402.2 KLV
    Inject(inject::Args),
    /// Put KLV into a live stream from a control channel, as it passes
    Live(live::Args),
}

/// What every subcommand gets.
pub struct Context {
    /// No status lines.
    pub quiet: bool,
    /// Set by Ctrl-C.
    pub stop: Arc<AtomicBool>,
}

pub fn main() -> ExitCode {
    let cli = Cli::parse();
    let stop = Arc::new(AtomicBool::new(false));
    let handler_stop = stop.clone();
    if let Err(e) = ctrlc::set_handler(move || {
        if handler_stop.swap(true, Ordering::SeqCst) {
            // Second Ctrl-C: the user does not want to wait.
            std::process::exit(130);
        }
    }) {
        eprintln!("tsflow: warning: cannot handle Ctrl-C ({e}); it will end the program without a summary");
    }
    let ctx = Context { quiet: cli.quiet, stop };
    let result = match cli.command {
        Command::Play(args) => play::run(args, &ctx),
        Command::Recv(args) => recv::run(args, &ctx),
        Command::Analyze(args) => analyze::run(args, &ctx),
        Command::Inject(args) => inject::run(args, &ctx),
        Command::Live(args) => live::run(args, &ctx),
    };
    match result {
        Ok(code) => code,
        Err(e) => {
            eprintln!("tsflow: {e}");
            ExitCode::from(EXIT_ERROR)
        }
    }
}

/// A PID: decimal or 0x hexadecimal, 0 to 0x1FFE.
pub fn parse_pid(s: &str) -> Result<u16, String> {
    let n = match s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        Some(hex) => u16::from_str_radix(hex, 16),
        None => s.parse(),
    }
    .map_err(|_| format!("\"{s}\" is not a PID"))?;
    if n >= 0x1FFF {
        return Err("a PID is 0 to 0x1FFE".into());
    }
    Ok(n)
}

/// Seconds, possibly fractional.
pub fn parse_seconds(s: &str) -> Result<Duration, String> {
    let v: f64 = s.parse().map_err(|_| format!("\"{s}\" is not a number of seconds"))?;
    if !(v.is_finite() && v > 0.0 && v < 1e9) {
        return Err("must be a positive number of seconds".into());
    }
    Ok(Duration::from_secs_f64(v))
}

/// Megabits per second for status lines.
pub fn mbps(packets: u64, over: Duration) -> f64 {
    let secs = over.as_secs_f64();
    if secs <= 0.0 {
        return 0.0;
    }
    packets as f64 * (tsflow::PACKET_SIZE * 8) as f64 / secs / 1e6
}

/// Whole seconds since the start, for the status line prefix.
pub fn stamp(elapsed: Duration) -> String {
    format!("[{:>5}s]", elapsed.as_secs())
}

/// The exit status for the outcome of the `--expect` checks.
pub fn checks_exit(ok: bool) -> ExitCode {
    ExitCode::from(if ok { EXIT_OK } else { EXIT_CHECK_FAILED })
}
