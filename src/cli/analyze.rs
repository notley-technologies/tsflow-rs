//! `tsflow analyze`: offline report.

use super::{checks_exit, Context};
use std::process::ExitCode;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tsflow::endpoint::Endpoint;
use tsflow::expect::{self, Expectation};
use tsflow::input::{Input, ReadEvent};
use tsflow::monitor::Monitor;
use tsflow::Error;

const AFTER_HELP: &str = "\
Examples:
  tsflow analyze input.ts
  tsflow analyze input.m2ts --json
  tsflow analyze out.ts --expect no-cc-errors --expect max-psi-interval=100

The same report as tsflow recv, from stream time only: programs and streams
(the mpegts report), continuity errors per PID, PCR intervals, PAT and PMT
repetition, bitrates, and every KLV stream decoded (ST 0601 packet rate,
checksums, ST 0102 security marking, tag 2 time span). Memory stays bounded
whatever the file size.";

#[derive(clap::Args)]
#[command(after_help = AFTER_HELP)]
pub struct Args {
    /// File to analyze (188, 192 or 204-byte packets), or - for standard input
    input: Endpoint,

    /// Print the report as one JSON object
    #[arg(long)]
    json: bool,

    /// Fail (exit status 1) unless this holds; may be repeated (see tsflow recv --help)
    #[arg(long, value_name = "EXPECTATION")]
    expect: Vec<Expectation>,
}

pub fn run(a: Args, ctx: &Context) -> Result<ExitCode, Error> {
    if let Endpoint::Net(_) = &a.input {
        return Err(Error::Config("analyze reads files; use tsflow recv for a network input".into()));
    }
    let mut input = Input::open(&a.input)?;
    let mut monitor = Monitor::new()?;
    while !ctx.stop.load(Ordering::Relaxed) {
        match input.read(Duration::ZERO)? {
            ReadEvent::Data(chunk) => monitor.push(chunk.packets, None),
            ReadEvent::Idle => {}
            ReadEvent::End => break,
        }
    }
    monitor.finish();
    monitor.set_input_stats(input.stats());
    let summary = monitor.summary();
    let (checks, ok) = expect::evaluate(&a.expect, &summary);
    if a.json {
        println!("{}", expect::report_json(&summary, &checks, None));
    } else {
        println!("tsflow analyze: {}", input.description());
        print!("{summary}");
        for c in &checks {
            println!("{c}");
        }
    }
    if summary.packets == 0 {
        return Err(Error::Config(format!("{}: no transport stream packets found", input.description())));
    }
    Ok(checks_exit(ok))
}
