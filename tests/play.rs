//! Playback on a simulated clock (exact timing) and to files (looping,
//! format conversion).

mod common;

use common::*;
use mpegts::PcrTimeline;
use std::sync::atomic::AtomicBool;
use std::time::Duration;
use tsflow::clock::ManualClock;
use tsflow::endpoint::Endpoint;
use tsflow::input::{Input, ReadEvent};
use tsflow::monitor::{Monitor, Summary};
use tsflow::output::{Output, Sink};
use tsflow::pacer::ticks_to_duration;
use tsflow::play::{self, PlayConfig, PlayStats};

/// Records what is sent and when, on the simulated clock.
struct Capture<'a> {
    clock: &'a ManualClock,
    sends: Vec<(Duration, Vec<u8>, u64)>,
}

impl Sink for Capture<'_> {
    fn send(&mut self, packets: &[u8], position: u64) -> std::io::Result<()> {
        self.sends.push((self.clock.elapsed(), packets.to_vec(), position));
        Ok(())
    }
}

/// The position the timeline gives each packet.
fn positions(data: &[u8]) -> Vec<u64> {
    let mut t = PcrTimeline::new(None, MUX_RATE);
    let mut out = Vec::new();
    for p in data.chunks_exact(PACKET_SIZE) {
        t.push(p.try_into().unwrap(), &mut |tp| out.push(tp.position));
    }
    t.flush(&mut |tp| out.push(tp.position));
    out
}

fn open(path: &std::path::Path) -> Input {
    Input::open(&Endpoint::File(path.to_path_buf())).unwrap()
}

fn config(latency_ms: u64) -> PlayConfig {
    let mut cfg = PlayConfig::default();
    cfg.latency = Duration::from_millis(latency_ms);
    cfg.fallback_bitrate = MUX_RATE;
    cfg
}

fn analyze(path: &std::path::Path) -> Summary {
    let mut input = open(path);
    let mut m = Monitor::new().unwrap();
    while let ReadEvent::Data(chunk) = input.read(Duration::ZERO).unwrap() {
        m.push(chunk.packets, None);
    }
    m.finish();
    m.summary()
}

#[test]
fn every_datagram_leaves_exactly_when_its_last_packet_is_due() {
    let s = generate(&Synth::frames(50));
    let path = temp_file("exact.ts", &s.data);
    let clock = ManualClock::new();
    let mut sink = Capture { clock: &clock, sends: Vec::new() };
    let mut statuses = Vec::new();
    let stats = play::play(&mut open(&path), &mut sink, &config(100), &clock, &AtomicBool::new(false), &mut |st| {
        statuses.push(st.clone())
    })
    .unwrap();
    std::fs::remove_file(&path).unwrap();
    // Every packet, in order, unchanged.
    let sent: Vec<u8> = sink.sends.iter().flat_map(|(_, p, _)| p.iter().copied()).collect();
    assert_eq!(sent, s.data);
    let pos = positions(&s.data);
    assert_eq!(pos[0], 0);
    let mut index = 0;
    for (i, (at, packets, first)) in sink.sends.iter().enumerate() {
        let n = packets.len() / PACKET_SIZE;
        assert!(n == 7 || i == sink.sends.len() - 1, "datagram {i} has {n} packets");
        assert_eq!(*first, pos[index]);
        // Due = latency + position of its last packet / 27 MHz, exactly.
        assert_eq!(*at, Duration::from_millis(100) + ticks_to_duration(pos[index + n - 1]), "datagram {i}");
        index += n;
    }
    assert_eq!(index, s.packets);
    assert_eq!(sink.sends.len(), s.packets.div_ceil(7));
    assert_eq!((stats.reanchors, stats.max_late, stats.dropped), (0, Duration::ZERO, 0));
    assert_eq!((stats.packets_in, stats.packets_out), (4000, 4000));
    // 2 s of stream, one status a second.
    assert_eq!(statuses.len(), 2);
    assert_eq!(statuses[0].elapsed, Duration::from_secs(1));
    // The last packet left at latency + its position.
    assert_eq!(clock.elapsed(), Duration::from_millis(100) + ticks_to_duration(pos[s.packets - 1]));
}

/// What a receiver measures of that pacing, still on the simulated clock:
/// the send times are fed to the monitor as arrival times. A datagram
/// leaves when its last packet is due, so a PCR packet waits at most six
/// packet times for the rest of its datagram: that is the whole arrival
/// jitter, and the arrivals span the stream exactly.
#[test]
fn a_receiver_measures_the_pacing_exactly() {
    let s = generate(&Synth::frames(50));
    let path = temp_file("receiver-view.ts", &s.data);
    let clock = ManualClock::new();
    let mut sink = Capture { clock: &clock, sends: Vec::new() };
    play::play(&mut open(&path), &mut sink, &config(50), &clock, &AtomicBool::new(false), &mut |_| {}).unwrap();
    std::fs::remove_file(&path).unwrap();
    // Only differences between instants matter; any base instant works.
    let origin = std::time::Instant::now();
    let mut m = Monitor::new().unwrap();
    for (at, packets, _) in &sink.sends {
        m.push(packets, Some(origin + *at));
    }
    m.finish();
    let r = m.summary();
    assert_eq!((r.packets, r.cc_errors), (s.packets as u64, 0));
    let six_packets = ticks_to_duration(6 * TICKS_PER_PACKET).as_secs_f64() * 1e3;
    let jitter = r.pcr[0].jitter_pp_ms.unwrap();
    assert!(jitter <= six_packets + 1e-6, "PCR arrival jitter {jitter} ms, at most {six_packets} ms");
    assert!(jitter > 0.0, "the PCR packets do wait for their datagrams");
    let first = sink.sends.first().unwrap().0;
    let last = sink.sends.last().unwrap().0;
    assert_eq!(r.wall_duration, Some(last - first));
    let pos = positions(&s.data);
    assert_eq!(last - first, ticks_to_duration(pos[s.packets - 1]) - ticks_to_duration(pos[6]));
}

#[test]
fn looping_keeps_positions_continuous_and_the_stream_valid() {
    let s = generate(&Synth::frames(25));
    let path = temp_file("loop.ts", &s.data);
    let clock = ManualClock::new();
    let mut sink = Capture { clock: &clock, sends: Vec::new() };
    let mut cfg = config(50);
    cfg.passes = Some(3);
    let stats = play::play(&mut open(&path), &mut sink, &cfg, &clock, &AtomicBool::new(false), &mut |_| {}).unwrap();
    assert_eq!((stats.pass, stats.packets_out, stats.pcr_discontinuities), (3, 6000, 2));
    assert_eq!(stats.reanchors, 0);
    // Positions never go back, and the loop points have no gap or burst:
    // three passes take three times one pass (within one packet).
    let firsts: Vec<u64> = sink.sends.iter().map(|s| s.2).collect();
    assert!(firsts.windows(2).all(|w| w[1] > w[0]));
    let one_pass = ticks_to_duration(s.duration_ticks());
    let total = clock.elapsed() - Duration::from_millis(50);
    let error = total.as_secs_f64() - 3.0 * one_pass.as_secs_f64();
    assert!(error.abs() < 0.002, "{total:?} for 3 x {one_pass:?}");
    // Written out, the result is one valid stream.
    let out = temp_path("loop-out.ts");
    let data: Vec<u8> = sink.sends.iter().flat_map(|(_, p, _)| p.iter().copied()).collect();
    std::fs::write(&out, data).unwrap();
    let r = analyze(&out);
    std::fs::remove_file(&out).unwrap();
    std::fs::remove_file(&path).unwrap();
    assert_eq!((r.packets, r.cc_errors), (6000, 0));
    assert_eq!((r.pcr[0].discontinuities, r.pcr[0].jumps), (2, 0));
    for k in &r.klv {
        assert_eq!(k.time_backwards, 2, "tag 2 goes back once per loop");
        assert!(k.checksums_ok());
    }
}

#[test]
fn unpaced_copy_converts_m2ts_and_loops_to_a_file() {
    let s = generate(&Synth::frames(30));
    let m2ts = temp_file("convert.m2ts", &to_m2ts(&s.data));
    let out = temp_path("convert-out.ts");
    let mut output = Output::open(&Endpoint::File(out.clone()), None).unwrap();
    let mut cfg = config(0);
    cfg.pace = false;
    let clock = ManualClock::new();
    let stats: PlayStats =
        play::play(&mut open(&m2ts), &mut output, &cfg, &clock, &AtomicBool::new(false), &mut |_| {}).unwrap();
    drop(output);
    assert_eq!(stats.packets_out, s.packets as u64);
    assert_eq!(std::fs::read(&out).unwrap(), s.data);
    // Two passes, as fast as possible.
    cfg.passes = Some(2);
    let mut output = Output::open(&Endpoint::File(out.clone()), None).unwrap();
    play::play(&mut open(&m2ts), &mut output, &cfg, &clock, &AtomicBool::new(false), &mut |_| {}).unwrap();
    drop(output);
    let r = analyze(&out);
    assert_eq!((r.packets, r.cc_errors, r.pcr[0].discontinuities), (2 * s.packets as u64, 0, 1));
    std::fs::remove_file(&out).unwrap();
    std::fs::remove_file(&m2ts).unwrap();
}

#[test]
fn a_stop_request_ends_playback_and_stdin_cannot_loop() {
    let s = generate(&Synth::frames(25));
    let path = temp_file("stop.ts", &s.data);
    let clock = ManualClock::new();
    let mut sink = Capture { clock: &clock, sends: Vec::new() };
    let stop = AtomicBool::new(false);
    let mut cfg = config(10);
    cfg.passes = None;
    let mut calls = 0;
    let stats = play::play(&mut open(&path), &mut sink, &cfg, &clock, &stop, &mut |_| {
        calls += 1;
        if calls == 3 {
            stop.store(true, std::sync::atomic::Ordering::Relaxed);
        }
    })
    .unwrap();
    // Looping forever, stopped after three seconds of status.
    assert!(stats.pass >= 3 && stats.packets_out > 0);
    std::fs::remove_file(&path).unwrap();
    let mut stdin = Input::open(&Endpoint::Stdio).unwrap();
    let err = play::play(&mut stdin, &mut sink, &cfg, &clock, &stop, &mut |_| {}).unwrap_err();
    assert!(err.to_string().contains("cannot loop"), "{err}");
}
