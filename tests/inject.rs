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

/// sequence_number of each synchronous KLV cell on the output, in order.
fn cell_numbers(ts: &[u8]) -> Vec<u8> {
    let mut d = Demuxer::new();
    d.subscribe(Subscription::new(Select::Codec(Codec::KlvSync), Unit::Pes)).unwrap();
    d.push(ts);
    d.finish();
    let mut seqs = Vec::new();
    while let Some(o) = d.pop_output() {
        seqs.push(o.payload()[1]);
        d.recycle(o);
    }
    seqs
}

#[test]
fn cells_are_numbered_per_stream_unless_asked() {
    // ISO/IEC 13818-1 2.12.4.1 and MISB ST 1402: one counter per stream
    // (DECISIONS X-8); --cell-numbering access-unit starts every access
    // unit at 0, for receivers such as MediaMTX.
    let stream = video(30);
    let input = temp_file("cells-in.ts", &stream.data);
    let state = temp_file("cells-state.ndjson", b"{\"set\": {\"SensorLatitude\": 38.9}}\n");
    let packets: Vec<Vec<u8>> = (0..20).map(|k| packet(MISP_START + k * 100_000)).collect();
    let klv = temp_file("cells.klv", &packets.concat());
    let output = temp_path("cells-out.ts");
    let (i, o, st, k) =
        (input.to_str().unwrap(), output.to_str().unwrap(), state.to_str().unwrap(), klv.to_str().unwrap());
    for (args, per_stream) in [
        (vec!["live", i, o, "--state", st, "--rate", "frame", "-q"], true),
        (vec!["live", i, o, "--state", st, "--rate", "frame", "-q", "--cell-numbering", "access-unit"], false),
        (vec!["inject", i, o, "--klv", k], true),
        (vec!["inject", i, o, "--klv", k, "--cell-numbering", "access-unit"], false),
    ] {
        let out = run(&args);
        assert!(out.status.success(), "{args:?}: {}", text(&out.stderr));
        let seqs = cell_numbers(&std::fs::read(&output).unwrap());
        assert!(seqs.len() >= 20, "{args:?}: {seqs:?}");
        let expected: Vec<u8> = (0..seqs.len()).map(|n| if per_stream { n as u8 } else { 0 }).collect();
        assert_eq!(seqs, expected, "{args:?}");
    }
    let out = run(&["live", i, o, "--cell-numbering", "per-pes"]);
    assert_eq!(out.status.code(), Some(2));
    for p in [input, state, klv, output] {
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

/// Start `tsflow recv` and wait for its "listening" line, so nothing is
/// sent before the socket exists (no sleeping).
fn start_recv(args: &[&str]) -> (std::process::Child, std::thread::JoinHandle<String>) {
    use std::io::{BufRead, BufReader, Read};
    let mut child = tsflow().arg("recv").args(args).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
    let mut stderr = BufReader::new(child.stderr.take().unwrap());
    let mut line = String::new();
    stderr.read_line(&mut line).unwrap();
    assert!(line.contains("listening on"), "{line}");
    let drain = std::thread::spawn(move || {
        let mut rest = String::new();
        let _ = stderr.read_to_string(&mut rest);
        line + &rest
    });
    (child, drain)
}

#[test]
fn paced_to_a_socket_and_received() {
    let input = temp_file("inject-udp-in.ts", &video(50).data);
    let packets: Vec<Vec<u8>> = (0..15).map(|k| packet(MISP_START + k * 100_000)).collect();
    let klv = temp_file("inject-udp.klv", &packets.concat());
    // The output to a file first: the same input and metadata give the same
    // packets, so the receiver can stop on their exact count.
    let file = temp_path("inject-udp-out.ts");
    let o = run(&["inject", input.to_str().unwrap(), file.to_str().unwrap(), "--klv", klv.to_str().unwrap(), "-q"]);
    assert_eq!(o.status.code(), Some(0), "{}", text(&o.stderr));
    let count = (std::fs::read(&file).unwrap().len() / PACKET_SIZE).to_string();
    let port = std::net::UdpSocket::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let (recv, stderr) = start_recv(&[
        &format!("udp://127.0.0.1:{port}"),
        "--count",
        &count,
        "--duration",
        "60",
        "--expect",
        "klv-marking=US//UNCLASSIFIED",
        "--expect",
        "no-cc-errors",
        "--json",
    ]);
    let o = run(&[
        "inject",
        input.to_str().unwrap(),
        &format!("udp://127.0.0.1:{port}"),
        "--klv",
        klv.to_str().unwrap(),
        "-q",
    ]);
    assert_eq!(o.status.code(), Some(0), "{}", text(&o.stderr));
    assert!(text(&o.stderr).contains("paced"), "{}", text(&o.stderr));
    let r = recv.wait_with_output().unwrap();
    let json = text(&r.stdout);
    assert_eq!(r.status.code(), Some(0), "{json}\n{}", stderr.join().unwrap());
    assert!(json.contains("\"stopped\":\"packet count reached\""), "{json}");
    assert!(json.contains(&format!("\"packets\":{count}")), "{json}");
    for p in [input, klv, file] {
        std::fs::remove_file(p).unwrap();
    }
}

#[test]
fn live_from_a_file_with_initial_values() {
    let s = video(40);
    let input = temp_file("live-cli-in.ts", &s.data);
    let output = temp_path("live-cli-out.ts");
    let state = temp_file(
        "live-cli-state.ndjson",
        b"{\"set\": {\"SensorLatitude\": 38.8977, \"SensorLongitude\": -77.0365, \"MissionId\": \"LIVE-CLI\"}}\n\n{\"unknown\": [\"SlantRange\"]}\n",
    );
    let out = run(&[
        "live",
        input.to_str().unwrap(),
        output.to_str().unwrap(),
        "--state",
        state.to_str().unwrap(),
        "--rate",
        "frame",
        "--json",
        "-q",
    ]);
    let stderr = text(&out.stderr);
    assert!(out.status.success(), "{stderr}");
    assert!(stderr.contains("end of input") && stderr.contains("40 frames, 40 KLV units"), "{stderr}");
    let summary = text(&out.stdout);
    assert!(summary.starts_with("{\"stop\":\"end of input\"") && summary.contains("\"units\":40,"), "{summary}");
    let ts = std::fs::read(&output).unwrap();
    let units = read_back(&ts, Codec::KlvSync);
    assert_eq!(units.len(), 40);
    for (_, bytes) in &units {
        let p = misb::decode(bytes).unwrap();
        assert!(p.is_ok());
        assert_eq!(p.get(Tag::MissionId).and_then(misb::Value::as_str), Some("LIVE-CLI"));
        assert_eq!(p.get(Tag::SlantRange), Some(&misb::Value::Unknown));
    }
    oracles(&output, 40);
    for p in [input, output, state] {
        std::fs::remove_file(p).unwrap();
    }
}

#[test]
fn live_refuses_a_bad_state_line() {
    let s = video(5);
    let input = temp_file("live-cli-bad-in.ts", &s.data);
    let state =
        temp_file("live-cli-bad.ndjson", b"{\"set\": {\"SensorLatitude\": 1}}\n{\"set\": {\"SensorLatitude\": 100}}\n");
    let out = run(&["live", input.to_str().unwrap(), "-", "--state", state.to_str().unwrap()]);
    assert_eq!(out.status.code(), Some(2));
    let stderr = text(&out.stderr);
    assert!(stderr.contains("line 2") && stderr.contains("Sensor Latitude"), "{stderr}");
    let out = run(&["live", "-", "-", "--control", "-"]);
    assert_eq!(out.status.code(), Some(2));
    assert!(text(&out.stderr).contains("cannot both come from standard input"));
    for p in [input, state] {
        std::fs::remove_file(p).unwrap();
    }
}

#[test]
fn ndjson_records_interpolated_at_frames() {
    let input = temp_file("inject-interp-in.ts", &video(50).data);
    // Two records 2 s apart; slant range and heading move between them,
    // the mission name holds.
    let lines = format!(
        "{{\"time_us\": {}, \"items\": {{\"SlantRange\": 1000, \"PlatformHeadingAngle\": 350, \"MissionId\": \"IP\"}}}}\n\
         {{\"time_us\": {}, \"items\": {{\"SlantRange\": 3000, \"PlatformHeadingAngle\": 10}}}}\n",
        MISP_START,
        MISP_START + 2_000_000
    );
    let nd = temp_file("inject-interp.ndjson", lines.as_bytes());
    let output = temp_path("inject-interp-out.ts");
    let o = run(&[
        "inject",
        input.to_str().unwrap(),
        output.to_str().unwrap(),
        "--ndjson",
        nd.to_str().unwrap(),
        "--rate",
        "frame",
        "--interpolate",
    ]);
    assert_eq!(o.status.code(), Some(0), "{}", text(&o.stderr));
    let units = read_back(&std::fs::read(&output).unwrap(), Codec::KlvSync);
    assert_eq!(units.len(), 50, "every frame at 25 fps for 2 s");
    for (n, (_, bytes)) in units.iter().enumerate() {
        let p = misb::decode(bytes).unwrap();
        let f = n as f64 / 50.0;
        let range = p.get(Tag::SlantRange).and_then(misb::Value::as_f64).unwrap();
        assert!((range - (1000.0 + 2000.0 * f)).abs() < 0.01, "frame {n}: {range}");
        let heading = p.get(Tag::PlatformHeadingAngle).and_then(misb::Value::as_f64).unwrap();
        let want = (350.0 + 20.0 * f) % 360.0;
        assert!((heading - want).abs().min(360.0 - (heading - want).abs()) < 0.01, "frame {n}: {heading}");
        assert_eq!(p.get(Tag::MissionId).and_then(misb::Value::as_str), Some("IP"));
    }
    for p in [input, nd, output] {
        std::fs::remove_file(p).unwrap();
    }
}

#[test]
fn m2ts_records_stay_m2ts() {
    let input = temp_file("inject-m2ts-in.m2ts", &to_m2ts(&video(40).data));
    let mut lines = String::new();
    for k in 0..20u64 {
        lines += &format!("{{\"time_us\": {}, \"items\": {{\"SensorLatitude\": {}}}}}\n", MISP_START + k * 80_000, k);
    }
    let nd = temp_file("inject-m2ts.ndjson", lines.as_bytes());
    let output = temp_path("inject-m2ts-out.m2ts");
    let o =
        run(&["inject", input.to_str().unwrap(), output.to_str().unwrap(), "--ndjson", nd.to_str().unwrap(), "--json"]);
    let stderr = text(&o.stderr);
    assert_eq!(o.status.code(), Some(0), "{stderr}");
    assert!(stderr.contains("192-byte M2TS records kept"), "{stderr}");
    assert!(text(&o.stdout).contains("\"units_inserted\":20"), "{}", text(&o.stdout));
    let out = std::fs::read(&output).unwrap();
    assert_eq!(out.len() % 192, 0);
    assert!(out.chunks(192).all(|r| r[4] == 0x47), "every record holds a packet after its 4-byte header");
    let ts: Vec<u8> = out.chunks(192).flat_map(|r| r[4..].to_vec()).collect();
    assert_eq!(read_back(&ts, Codec::KlvSync).len(), 20);
    for p in [input, nd, output] {
        std::fs::remove_file(p).unwrap();
    }
}
