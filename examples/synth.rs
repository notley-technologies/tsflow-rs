//! Write a synthetic test stream: H.264-like video at 25 fps with PCR,
//! synchronous KLV on every frame and asynchronous KLV at 5 Hz (MISB ST
//! 0601 with an ST 0102 US//UNCLASSIFIED security set), PAT and PMT every
//! 80 ms, as a 3.008 Mbit/s constant-rate multiplex.
//!
//! ```text
//! cargo run --release --example synth -- test.ts 60     # 60 seconds
//! tsflow play test.ts udp://127.0.0.1:5000 --loop
//! ```

#[path = "../tests/common/mod.rs"]
mod common;

use std::io::Write;

fn main() {
    let mut args = std::env::args().skip(1);
    let (Some(path), seconds) = (args.next(), args.next()) else {
        eprintln!("usage: synth OUTPUT.ts [SECONDS (default 10)]");
        std::process::exit(2);
    };
    let seconds: f64 = match seconds.as_deref().map(str::parse).transpose() {
        Ok(s) => s.unwrap_or(10.0),
        Err(_) => {
            eprintln!("synth: SECONDS must be a number");
            std::process::exit(2);
        }
    };
    let frames = (seconds * 25.0).round().max(1.0) as usize;
    let mut file = match std::fs::File::create(&path) {
        Ok(f) => std::io::BufWriter::new(f),
        Err(e) => {
            eprintln!("synth: {path}: {e}");
            std::process::exit(1);
        }
    };
    let mut g = common::Generator::new(&common::Synth::frames(frames));
    let mut buf = Vec::new();
    for _ in 0..frames {
        buf.clear();
        g.frame(&mut buf);
        if let Err(e) = file.write_all(&buf) {
            eprintln!("synth: {path}: {e}");
            std::process::exit(1);
        }
    }
    if let Err(e) = file.flush() {
        eprintln!("synth: {path}: {e}");
        std::process::exit(1);
    }
    eprintln!(
        "synth: wrote {path}: {} packets ({:.1} MB), {:.1} s, {} sync and {} async ST 0601 packets",
        g.packets,
        (g.packets * common::PACKET_SIZE) as f64 / 1e6,
        frames as f64 / 25.0,
        g.sync_klv,
        g.async_klv
    );
}
