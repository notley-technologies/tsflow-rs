//! Real sockets on 127.0.0.1: a synthetic stream played in real time and
//! received by the recv machinery, as UDP and RTP, looping, forwarded.

mod common;

use common::*;
use std::sync::atomic::AtomicBool;
use std::thread::{self, JoinHandle};
use std::time::Duration;
use tsflow::clock::SystemClock;
use tsflow::endpoint::Endpoint;
use tsflow::input::Input;
use tsflow::monitor::{Monitor, Summary};
use tsflow::output::Output;
use tsflow::pacer::ticks_to_duration;
use tsflow::play::{self, PlayConfig, PlayStats};
use tsflow::recv::{self, RecvConfig, StopReason};

/// Datagram timing on a busy test machine is not exact; these bound what a
/// working pacer delivers by a wide margin.
const DURATION_TOLERANCE: f64 = 0.10;
const MAX_JITTER_MS: f64 = 25.0;

fn listen(scheme: &str) -> (Input, u16) {
    let input = Input::open(&format!("{scheme}://127.0.0.1:0").parse().unwrap()).unwrap();
    let port = input.local_addr().unwrap().port();
    (input, port)
}

/// Receive on a thread until the input goes quiet, optionally forwarding.
fn receive(mut input: Input, forward: Option<Endpoint>) -> JoinHandle<(Summary, StopReason)> {
    thread::spawn(move || {
        let mut monitor = Monitor::new().unwrap();
        let mut forward = forward.map(|e| Output::open(&e, None).unwrap());
        let mut cfg = RecvConfig::default();
        cfg.idle_exit = Some(Duration::from_millis(700));
        cfg.duration = Some(Duration::from_secs(30));
        cfg.status_interval = Duration::from_millis(500);
        let reason = recv::run(
            &mut input,
            &mut monitor,
            forward.as_mut(),
            None,
            &cfg,
            &SystemClock,
            &AtomicBool::new(false),
            &mut |m, now| {
                m.take_window(now);
            },
        )
        .unwrap();
        (monitor.summary(), reason)
    })
}

fn play_to(data: &[u8], name: &str, output: &str, passes: u64) -> PlayStats {
    let path = temp_file(name, data);
    let mut input = Input::open(&Endpoint::File(path.clone())).unwrap();
    let mut output = Output::open(&output.parse().unwrap(), None).unwrap();
    let mut cfg = PlayConfig::default();
    cfg.latency = Duration::from_millis(50);
    cfg.fallback_bitrate = MUX_RATE;
    cfg.passes = Some(passes);
    let stats = play::play(&mut input, &mut output, &cfg, &SystemClock, &AtomicBool::new(false), &mut |_| {}).unwrap();
    std::fs::remove_file(path).unwrap();
    stats
}

/// The stream arrived complete, intact and at its own pace.
fn check_delivery(r: &Summary, s: &Stream, passes: u64) {
    let n = s.packets as u64 * passes;
    assert_eq!(r.packets, n);
    assert_eq!(r.cc_errors, 0);
    let input = r.input.unwrap();
    assert_eq!(input.datagrams, n.div_ceil(7));
    // From the first datagram to the last: the stream's duration minus the
    // first datagram's span.
    let expected = ticks_to_duration(s.duration_ticks() * passes).as_secs_f64();
    let wall = r.wall_duration.unwrap().as_secs_f64();
    assert!((wall / expected - 1.0).abs() < DURATION_TOLERANCE, "arrivals spanned {wall:.3} s for {expected:.3} s");
    let jitter = r.pcr[0].jitter_pp_ms.unwrap();
    assert!(jitter < MAX_JITTER_MS, "PCR arrival jitter {jitter:.2} ms");
    let sync = r.klv.iter().find(|k| k.sync).unwrap();
    assert_eq!(sync.packets, s.sync_klv as u64 * passes);
    assert_eq!(sync.markings, vec![("US//UNCLASSIFIED".to_string(), sync.packets)]);
}

#[test]
fn udp_delivers_every_packet_at_the_stream_pace() {
    let s = generate(&Synth::frames(50));
    let (input, port) = listen("udp");
    let rx = receive(input, None);
    let stats = play_to(&s.data, "udp.ts", &format!("udp://127.0.0.1:{port}"), 1);
    let (r, reason) = rx.join().unwrap();
    assert_eq!(reason, StopReason::Idle);
    assert_eq!(stats.packets_out, s.packets as u64);
    check_delivery(&r, &s, 1);
    let input = r.input.unwrap();
    assert_eq!((input.rtp_datagrams, input.rtp), (0, None));
}

#[test]
fn rtp_is_detected_and_its_sequence_checked() {
    let s = generate(&Synth::frames(40));
    let (input, port) = listen("rtp");
    let rx = receive(input, None);
    play_to(&s.data, "rtp.ts", &format!("rtp://127.0.0.1:{port}?ssrc=1234"), 1);
    let (r, _) = rx.join().unwrap();
    check_delivery(&r, &s, 1);
    let input = r.input.unwrap();
    assert_eq!(input.rtp_datagrams, input.datagrams);
    let rtp = input.rtp.unwrap();
    assert_eq!((rtp.received, rtp.lost, rtp.late, rtp.restarts), (input.datagrams, 0, 0, 0));
}

#[test]
fn a_looping_file_is_one_continuous_stream_for_the_receiver() {
    let s = generate(&Synth::frames(25));
    let (input, port) = listen("udp");
    let rx = receive(input, None);
    let stats = play_to(&s.data, "loop.ts", &format!("udp://127.0.0.1:{port}"), 3);
    let (r, _) = rx.join().unwrap();
    assert_eq!((stats.pass, stats.pcr_discontinuities), (3, 2));
    check_delivery(&r, &s, 3);
    let pcr = &r.pcr[0];
    assert_eq!((pcr.discontinuities, pcr.jumps), (2, 0));
}

#[test]
fn forwarding_relays_every_datagram_unchanged() {
    let s = generate(&Synth::frames(30));
    let (tap, tap_port) = listen("rtp");
    let tap_rx = receive(tap, None);
    let (input, port) = listen("rtp");
    let rx = receive(input, Some(format!("rtp://127.0.0.1:{tap_port}").parse().unwrap()));
    play_to(&s.data, "forward.ts", &format!("rtp://127.0.0.1:{port}"), 1);
    let (r, _) = rx.join().unwrap();
    let (t, _) = tap_rx.join().unwrap();
    check_delivery(&r, &s, 1);
    assert_eq!((t.packets, t.cc_errors), (r.packets, 0));
    let (a, b) = (r.input.unwrap(), t.input.unwrap());
    assert_eq!(a.datagrams, b.datagrams);
    // The RTP headers were relayed as they were: same sequence, no gaps.
    assert_eq!(b.rtp.unwrap().received, a.rtp.unwrap().received);
    assert_eq!(b.rtp.unwrap().lost, 0);
    let klv = |x: &Summary| -> Vec<_> {
        x.klv.iter().map(|k| (k.pid, k.packets, k.checksum_valid, k.markings.clone(), k.first_time.clone())).collect()
    };
    assert_eq!(klv(&t), klv(&r));
}
