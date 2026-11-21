//! Throughput of the analysis path and of the unpaced playback path, on a
//! synthetic stream (see `synth`).
//!
//! ```text
//! cargo run --release --example throughput -- 600    # 600 s of stream, 226 MB
//! ```
//!
//! Three measurements: the monitor alone on a buffer in memory; `analyze`
//! as the command runs it (file input, framer, monitor); and `play
//! --no-pace` from a file to a file (framer, PCR timeline, loop fixer,
//! datagram grouping, buffered writes). The file has just been written, so
//! it is read from the page cache: the figures are CPU cost, not disk
//! speed.

#[path = "../tests/common/mod.rs"]
mod common;

use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};
use tsflow::clock::SystemClock;
use tsflow::endpoint::Endpoint;
use tsflow::input::{Input, ReadEvent};
use tsflow::monitor::Monitor;
use tsflow::output::Output;
use tsflow::play::{self, PlayConfig};

fn report(what: &str, bytes: usize, elapsed: Duration) {
    let secs = elapsed.as_secs_f64();
    println!(
        "{what:<34} {:>8.0} MB/s  {:>6.2} M packets/s  ({:.3} s)",
        bytes as f64 / 1e6 / secs,
        (bytes / common::PACKET_SIZE) as f64 / 1e6 / secs,
        secs
    );
}

fn main() -> Result<(), tsflow::Error> {
    let seconds: f64 = std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(600.0);
    let stream = common::generate(&common::Synth::frames((seconds * 25.0) as usize));
    let bytes = stream.data.len();
    println!("synthetic stream: {:.0} s, {} packets, {:.1} MB", seconds, stream.packets, bytes as f64 / 1e6);

    let t = Instant::now();
    let mut monitor = Monitor::new()?;
    for chunk in stream.data.chunks(1394 * common::PACKET_SIZE) {
        monitor.push(chunk, None);
    }
    monitor.finish();
    let summary = monitor.summary();
    report("monitor (in memory)", bytes, t.elapsed());
    assert_eq!(summary.packets, stream.packets as u64);

    let input_path = common::temp_file("throughput-in.ts", &stream.data);
    let t = Instant::now();
    let mut input = Input::open(&Endpoint::File(input_path.clone()))?;
    let mut monitor = Monitor::new()?;
    while let ReadEvent::Data(chunk) = input.read(Duration::ZERO)? {
        monitor.push(chunk.packets, None);
    }
    monitor.finish();
    let summary = monitor.summary();
    report("analyze (file, framer, monitor)", bytes, t.elapsed());
    assert_eq!((summary.packets, summary.cc_errors), (stream.packets as u64, 0));

    let output_path = common::temp_path("throughput-out.ts");
    let t = Instant::now();
    let mut input = Input::open(&Endpoint::File(input_path.clone()))?;
    let mut output = Output::open(&Endpoint::File(output_path.clone()), None)?;
    let mut cfg = PlayConfig::default();
    cfg.pace = false;
    let stats = play::play(&mut input, &mut output, &cfg, &SystemClock, &AtomicBool::new(false), &mut |_| {})?;
    drop(output);
    report("play --no-pace (file to file)", bytes, t.elapsed());
    assert_eq!(stats.packets_out, stream.packets as u64);

    let _ = std::fs::remove_file(input_path);
    let _ = std::fs::remove_file(output_path);
    Ok(())
}
