//! `tsflow inject`: metadata files into synthetic streams, checked with
//! tsflow's own monitor, the mpegts demuxer and, when configured, the
//! patched ffprobe and TSDuck.

mod common;

use common::*;
use misb::encode::{Classification, Input, Security, St0601Builder};
use misb::{st0601::Tag, MispTime};
use mpegts::{Codec, Demuxer, Select, Subscription, Unit};
use std::process::{Command, Output, Stdio};

fn tsflow() -> Command {
    Command::new(env!("CARGO_BIN_EXE_tsflow"))
}

fn run(args: &[&str]) -> Output {
    tsflow().args(args).output().unwrap()
}

fn text(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

/// Video only: the stream to inject into.
fn video(frames: usize) -> Stream {
    generate(&Synth { frames, sync_klv: false, async_klv_every: None, ..Synth::default() })
}

/// A recorded packet at MISP time `t` with a US//UNCLASSIFIED security set.
fn packet(t: u64) -> Vec<u8> {
    let security = Security::new(Classification::Unclassified, 0x0D, "//US", 0x0D, "US").encode().unwrap();
    let mut b = St0601Builder::new(MispTime::from_micros(t));
    b.set(Tag::MissionId, "INJECTED").unwrap();
    b.set(Tag::SensorLatitude, 38.9).unwrap();
    b.set(Tag::SecurityLocalSet, Input::Set(security)).unwrap();
    b.finish().unwrap()
}

/// KLV units on the output: (PTS, payload).
fn read_back(ts: &[u8], codec: Codec) -> Vec<(Option<u64>, Vec<u8>)> {
    let mut d = Demuxer::new();
    let mut sub = Subscription::new(Select::Codec(codec), Unit::Pes);
    if codec == Codec::KlvSync {
        sub = sub.strip_metadata_au();
    }
    d.subscribe(sub).unwrap();
    d.push(ts);
    d.finish();
    let mut units = Vec::new();
    while let Some(o) = d.pop_output() {
        units.push((o.pts, o.payload().to_vec()));
        d.recycle(o);
    }
    units
}

/// The patched ffprobe and TSDuck, when configured (MPEGTS_FFPROBE, the
/// directory MPEGTS_TSDUCK as in the mpegts tests).
fn oracles(path: &std::path::Path, units: usize) {
    if let Ok(ffprobe) = std::env::var("MPEGTS_FFPROBE") {
        let out = Command::new(&ffprobe)
            .args(["-v", "error", "-select_streams", "d", "-show_entries", "packet=pts", "-of", "csv=p=0"])
            .arg(path)
            .output()
            .unwrap_or_else(|e| panic!("{ffprobe}: {e}"));
        let stderr = text(&out.stderr);
        let other: Vec<&str> = stderr.lines().filter(|l| !l.starts_with("[h264 @")).collect();
        assert!(out.status.success() && other.is_empty(), "{stderr}");
        assert_eq!(text(&out.stdout).lines().filter(|l| !l.is_empty()).count(), units, "ffprobe packets");
    }
    if let Ok(bin) = std::env::var("MPEGTS_TSDUCK") {
        let tsp = std::path::Path::new(&bin).join("tsp");
        let out = Command::new(&tsp)
            .args(["-I", "file"])
            .arg(path)
            .args(["-P", "continuity", "-O", "drop"])
            .output()
            .unwrap_or_else(|e| panic!("{}: {e}", tsp.display()));
        assert!(out.status.success() && out.stderr.is_empty(), "TSDuck: {}", text(&out.stderr));
    }
}

#[test]
fn a_klv_file_goes_in_byte_for_byte() {
    let input = temp_file("inject-klv-in.ts", &video(100).data);
    // 40 packets at 10 Hz from the first frame's time.
    let packets: Vec<Vec<u8>> = (0..40).map(|k| packet(MISP_START + k * 100_000)).collect();
    let klv = temp_file("inject-klv.klv", &packets.concat());
    let output = temp_path("inject-klv-out.ts");
    let anchor = format!("first-frame=misp:{MISP_START}");
    let o = run(&[
        "inject",
        input.to_str().unwrap(),
        output.to_str().unwrap(),
        "--klv",
        klv.to_str().unwrap(),
        "--anchor",
        &anchor,
        "--strict",
        "--json",
    ]);
    assert_eq!(o.status.code(), Some(0), "{}", text(&o.stderr));
    let json = text(&o.stdout);
    assert!(json.contains("\"units_inserted\":40") && json.contains("\"skipped\":0"), "{json}");
    let err = text(&o.stderr);
    assert!(err.contains("40 KLV units inserted from 40 samples"), "{err}");

    let out = std::fs::read(&output).unwrap();
    let units = read_back(&out, Codec::KlvSync);
    assert_eq!(units.len(), 40);
    // The first frame is presented at START_PCR / 300 + 18000.
    let first_pts = START_PCR / 300 + 18_000;
    for (k, (pts, data)) in units.iter().enumerate() {
        assert_eq!(data, &packets[k]);
        assert_eq!(*pts, Some(first_pts + 9000 * k as u64));
    }
    // tsflow's own analysis of the result.
    let a = run(&[
        "analyze",
        output.to_str().unwrap(),
        "--expect",
        "klv-marking=US//UNCLASSIFIED",
        "--expect",
        "no-cc-errors",
    ]);
    assert_eq!(a.status.code(), Some(0), "{}", text(&a.stdout));
    oracles(&output, 40);
    for p in [input, klv, output] {
        std::fs::remove_file(p).unwrap();
    }
}

#[test]
fn ndjson_records_sampled_at_frames() {
    let input = temp_file("inject-nd-in.ts", &video(75).data);
    let mut lines = String::new();
    for k in 0..3u64 {
        lines += &format!(
            "{{\"time_us\": {}, \"items\": {{\"SensorLatitude\": {}, \"MissionId\": \"ND\"}}}}\n\n",
            MISP_START + k * 1_000_000,
            30.0 + k as f64
        );
    }
    let nd = temp_file("inject-nd.ndjson", lines.as_bytes());
    let output = temp_path("inject-nd-out.ts");
    let o = run(&[
        "inject",
        input.to_str().unwrap(),
        output.to_str().unwrap(),
        "--ndjson",
        nd.to_str().unwrap(),
        "--rate",
        "frame",
        "--mode",
        "async",
    ]);
    assert_eq!(o.status.code(), Some(0), "{}", text(&o.stderr));
    let out = std::fs::read(&output).unwrap();
    let units = read_back(&out, Codec::KlvAsync);
    assert_eq!(units.len(), 75, "every frame at 25 fps for 3 s");
    let lat = |d: &[u8]| misb::decode(d).unwrap().get(Tag::SensorLatitude).unwrap().as_f64().unwrap();
    assert!((lat(&units[0].1) - 30.0).abs() < 1e-6);
    assert!((lat(&units[74].1) - 32.0).abs() < 1e-6);
    oracles(&output, 75);
    for p in [input, nd, output] {
        std::fs::remove_file(p).unwrap();
    }
}

#[test]
fn cbr_keeps_the_size() {
    let stream = video(50);
    let input = temp_file("inject-cbr-in.ts", &stream.data);
    let packets: Vec<Vec<u8>> = (0..20).map(|k| packet(MISP_START + k * 100_000)).collect();
    let klv = temp_file("inject-cbr.klv", &packets.concat());
    let output = temp_path("inject-cbr-out.ts");
    let o =
        run(&["inject", input.to_str().unwrap(), output.to_str().unwrap(), "--klv", klv.to_str().unwrap(), "--cbr"]);
    assert_eq!(o.status.code(), Some(0), "{}", text(&o.stderr));
    let out = std::fs::read(&output).unwrap();
    assert_eq!(out.len(), stream.data.len());
    assert_eq!(read_back(&out, Codec::KlvSync).len(), 20);
    for p in [input, klv, output] {
        std::fs::remove_file(p).unwrap();
    }
}

#[test]
fn errors_and_strict() {
    let input = temp_file("inject-err-in.ts", &video(25).data);
    let i = input.to_str().unwrap();
    let out = temp_path("inject-err-out.ts");
    let o = out.to_str().unwrap();
    // A missing file, a bad line, bad options: status 2.
    let bad = temp_file("inject-err.ndjson", b"{\"time_us\": 5}\n{\"time_us\": 6, \"items\": {\"Nope\": 1}}\n");
    for args in [
        vec!["inject", i, o, "--klv", "/nonexistent.klv"],
        vec!["inject", i, o, "--ndjson", bad.to_str().unwrap()],
        vec!["inject", i, o],
        vec!["inject", i, o, "--klv", "x.klv", "--rate", "fast"],
        vec!["inject", i, o, "--klv", "x.klv", "--anchor", "first-frame=tomorrow"],
    ] {
        let r = run(&args);
        assert_eq!(r.status.code(), Some(2), "{args:?}: {}", text(&r.stderr));
    }
    let r = run(&["inject", i, o, "--ndjson", bad.to_str().unwrap()]);
    assert!(text(&r.stderr).contains("line 2"), "{}", text(&r.stderr));
    // Packets beyond the 1 s of video are not inserted: --strict says so.
    let packets: Vec<Vec<u8>> = (0..40).map(|k| packet(MISP_START + k * 100_000)).collect();
    let klv = temp_file("inject-err.klv", &packets.concat());
    let r = run(&["inject", i, o, "--klv", klv.to_str().unwrap(), "--strict"]);
    assert_eq!(r.status.code(), Some(1), "{}", text(&r.stderr));
    assert!(text(&r.stderr).contains("--strict"));
    let r = run(&["inject", i, o, "--klv", klv.to_str().unwrap()]);
    assert_eq!(r.status.code(), Some(0));
    for p in [input, bad, klv, out] {
        std::fs::remove_file(p).unwrap();
    }
}

#[test]
fn paced_to_a_socket_and_received() {
    let input = temp_file("inject-udp-in.ts", &video(50).data);
    let packets: Vec<Vec<u8>> = (0..15).map(|k| packet(MISP_START + k * 100_000)).collect();
    let klv = temp_file("inject-udp.klv", &packets.concat());
    let port = 15_000 + (std::process::id() % 5000) as u16;
    let listen = format!("udp://@127.0.0.1:{port}");
    let send = format!("udp://127.0.0.1:{port}");
    let recv = tsflow()
        .args([
            "recv",
            &listen,
            "--idle-exit",
            "1",
            "--expect",
            "klv-marking=US//UNCLASSIFIED",
            "--expect",
            "no-cc-errors",
            "-q",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(300));
    let o = run(&["inject", input.to_str().unwrap(), &send, "--klv", klv.to_str().unwrap(), "-q"]);
    assert_eq!(o.status.code(), Some(0), "{}", text(&o.stderr));
    assert!(text(&o.stderr).contains("paced"), "{}", text(&o.stderr));
    let r = recv.wait_with_output().unwrap();
    assert_eq!(r.status.code(), Some(0), "{}\n{}", text(&r.stdout), text(&r.stderr));
    for p in [input, klv] {
        std::fs::remove_file(p).unwrap();
    }
}
