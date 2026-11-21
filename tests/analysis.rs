//! Offline analysis of synthetic streams: the monitor, the framer through
//! a file input, and the expectations.

mod common;

use common::*;
use std::time::Duration;
use tsflow::expect::{self, Expectation};
use tsflow::framer::PacketFormat;
use tsflow::input::{Input, ReadEvent};
use tsflow::monitor::{Monitor, Summary};

fn analyze_bytes(data: &[u8]) -> Summary {
    let mut m = Monitor::new().unwrap();
    // Uneven chunks, as reads deliver them.
    for chunk in data.chunks(188 * 37) {
        m.push(chunk, None);
    }
    m.finish();
    m.summary()
}

fn analyze_file(name: &str, data: &[u8]) -> (Summary, tsflow::input::InputStats) {
    let path = temp_file(name, data);
    let mut input = Input::open(&tsflow::endpoint::Endpoint::File(path.clone())).unwrap();
    let mut m = Monitor::new().unwrap();
    while let ReadEvent::Data(chunk) = input.read(Duration::ZERO).unwrap() {
        m.push(chunk.packets, None);
    }
    m.finish();
    m.set_input_stats(input.stats());
    std::fs::remove_file(path).unwrap();
    (m.summary(), input.stats())
}

#[test]
fn synthetic_stream_is_reported_exactly() {
    let s = generate(&Synth::frames(50));
    let r = analyze_bytes(&s.data);
    assert_eq!(r.packets, s.packets as u64);
    assert_eq!((r.cc_errors, r.transport_errors), (0, 0));
    // 50 frames of 40 ms.
    let d = r.stream_duration.unwrap();
    assert!((d.as_secs_f64() - 2.0).abs() < 0.002, "{d:?}");
    let rate = r.bitrate.unwrap() as f64;
    assert!((rate / MUX_RATE as f64 - 1.0).abs() < 0.002, "{rate}");
    // One program, video plus both KLV carriages.
    let program = &r.report.programs[0];
    assert_eq!((program.number, program.pmt_pid, program.pcr_pid), (1, PMT_PID, Some(VIDEO_PID)));
    assert_eq!(program.streams.len(), 3);
    // PCR every frame: 40 ms, shifted by the two PSI packets every other frame.
    let pcr = &r.pcr[0];
    assert_eq!((pcr.pid, pcr.count, pcr.over_limit, pcr.discontinuities, pcr.jumps), (VIDEO_PID, 50, 0, 0, 0));
    let (min, avg, max) = pcr.interval_ms.unwrap();
    assert_eq!((min, max), (39.0, 41.0));
    assert!((avg - 40.0).abs() < 0.03);
    assert_eq!(pcr.jitter_pp_ms, None, "no arrival times offline");
    // PAT and PMT every other frame: exactly 80 ms of stream time, except
    // that the first ones come before the first PCR and are timed with it
    // (79 and 79.5 ms to the next).
    assert_eq!(r.psi.len(), 2);
    for p in &r.psi {
        let (min, max) = p.interval_ms.unwrap();
        assert_eq!((p.sections, max), (25, 80.0), "{}", p.table);
        assert!((79.0..=79.5).contains(&min), "{}: {min}", p.table);
    }
    assert_eq!(r.psi[1].table, "PMT (program 1)");
    // KLV: every frame synchronously, every fifth frame asynchronously.
    assert_eq!(r.klv.len(), 2);
    let sync = r.klv.iter().find(|k| k.sync).unwrap();
    let asynchronous = r.klv.iter().find(|k| !k.sync).unwrap();
    assert_eq!((sync.pid, sync.packets, sync.pes), (SYNC_KLV_PID, s.sync_klv as u64, s.sync_klv as u64));
    assert_eq!((asynchronous.pid, asynchronous.packets), (ASYNC_KLV_PID, s.async_klv as u64));
    for k in &r.klv {
        assert!(k.checksums_ok());
        assert_eq!((k.malformed, k.unmarked, k.time_backwards, k.damaged_pes), (0, 0, 0, 0));
        assert_eq!(k.markings, vec![("US//UNCLASSIFIED".to_string(), k.packets)]);
        assert_eq!(k.first_time.as_deref(), Some("2023-11-14T22:13:20.000000Z"));
    }
    assert!((sync.rate_hz.unwrap() - 25.0).abs() < 0.05);
    // The sync KLV unit is written after the video frame, so it arrives
    // 200 ms minus the frame's 50 to 70 packets before its PTS.
    let (lead_min, lead_max) = sync.lead_ms.unwrap();
    assert!(lead_min > 160.0 && lead_max < 200.0, "{lead_min}..{lead_max}");
    assert_eq!(sync.lead_outside, 0);
    assert_eq!(asynchronous.lead_ms, None);
    // Per-PID table.
    let null = r.pids.iter().find(|p| p.pid == 0x1FFF).unwrap();
    assert_eq!(null.content, "null packets");
    assert!(r.pids.iter().any(|p| p.pid == 0 && p.content == "PAT"));
    assert!(r.pids.iter().any(|p| p.pid == VIDEO_PID && p.content.contains("H.264")));
    // Text and JSON render.
    let text = r.to_string();
    assert!(text.contains("US//UNCLASSIFIED x50"), "{text}");
    let json = r.to_json();
    assert!(json.starts_with("{\"packets\":4000,"), "{json}");
    assert!(json.contains("\"marking\":\"US//UNCLASSIFIED\""));
}

#[test]
fn continuity_errors_and_markings_are_counted() {
    let mut cfg = Synth::frames(10);
    cfg.classification = misb::encode::Classification::Secret;
    let s = generate(&cfg);
    let damaged = drop_packet(&s.data, VIDEO_PID, 100);
    let r = analyze_bytes(&damaged);
    assert_eq!(r.cc_errors, 1);
    assert_eq!(r.pids.iter().find(|p| p.pid == VIDEO_PID).unwrap().cc_errors, 1);
    assert_eq!(r.klv[0].markings[0].0, "US//SECRET");
}

#[test]
fn m2ts_and_damaged_files_are_read_through_the_framer() {
    let s = generate(&Synth::frames(20));
    let (ts, ts_input) = analyze_file("plain.ts", &s.data);
    let (m2ts, m2ts_input) = analyze_file("records.m2ts", &to_m2ts(&s.data));
    assert_eq!(ts_input.framer.format, Some(PacketFormat::Ts));
    assert_eq!(m2ts_input.framer.format, Some(PacketFormat::M2ts));
    assert_eq!(m2ts_input.framer.bytes_skipped, 0);
    assert_eq!(ts.packets, m2ts.packets);
    assert_eq!(ts.klv, m2ts.klv);
    assert_eq!(ts.pcr, m2ts.pcr);
    // Garbage in front and a damaged stretch in the middle.
    let mut messy = vec![0x12; 1000];
    messy.extend_from_slice(&s.data[..188 * 500]);
    messy.extend_from_slice(&[0x47, 0x00, 0x99]);
    messy.extend_from_slice(&s.data[188 * 500..]);
    let (r, input) = analyze_file("messy.ts", &messy);
    assert_eq!(r.packets, s.packets as u64);
    assert_eq!((input.framer.bytes_skipped, input.framer.sync_losses), (1003, 1));
    assert_eq!(r.cc_errors, 0);
}

#[test]
fn expectations_pass_and_fail() {
    let s = generate(&Synth::frames(50));
    let r = analyze_bytes(&s.data);
    let check = |e: &str| e.parse::<Expectation>().unwrap().check(&r);
    for e in [
        "no-cc-errors",
        "max-cc-errors=0",
        "min-packets=4000",
        "klv-streams=2",
        "klv-marking=US//UNCLASSIFIED",
        "min-klv-rate=5",
        "klv-checksums",
        "max-pcr-interval=41",
        "max-psi-interval=100",
        "no-rtp-loss",
    ] {
        let c = check(e);
        assert!(c.passed, "{c}");
    }
    for e in [
        "min-packets=4001",
        "klv-streams=1",
        "klv-marking=US//SECRET",
        "min-klv-rate=25.5",
        "max-pcr-interval=40",
        "max-psi-interval=79",
        "max-pcr-jitter=10",
    ] {
        let c = check(e);
        assert!(!c.passed, "{c}");
    }
    assert_eq!(check("klv-marking=US//SECRET").detail, "0x0101 US//UNCLASSIFIED x50; 0x0102 US//UNCLASSIFIED x10");
    // Without KLV, KLV expectations fail.
    let mut cfg = Synth::frames(5);
    cfg.sync_klv = false;
    cfg.async_klv_every = None;
    let plain = analyze_bytes(&generate(&cfg).data);
    let (checks, ok) =
        expect::evaluate(&["klv-marking=US//UNCLASSIFIED".parse().unwrap(), "min-klv-rate=1".parse().unwrap()], &plain);
    assert!(!ok);
    assert!(checks.iter().all(|c| !c.passed));
    let json = expect::report_json(&plain, &checks, None);
    assert!(json.contains("\"passed\":false"), "{json}");
}
