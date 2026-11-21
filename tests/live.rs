//! Live injection: the loop of `tsflow live` with a file input on a
//! simulated clock, and over real sockets where a control line must change
//! the very next frame. No test measures time; sockets run in lockstep and
//! real time is only a hang guard.

mod common;

use common::*;
use misb::mux::{Carriage, LiveMux, LiveMuxConfig, Rate};
use misb::st0601::Tag;
use misb::{MispTime, Value};
use mpegts::{Codec, Demuxer, Select, Subscription, Unit};
use std::io;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;
use tsflow::clock::{Clock, ManualClock, SystemClock};
use tsflow::control::Control;
use tsflow::endpoint::Endpoint;
use tsflow::input::Input;
use tsflow::live::{self, LiveConfig, MispClock};
use tsflow::output::{Output, Sink};
use tsflow::recv::StopReason;

const T0: u64 = 1_765_000_000_000_000;
const HANG_GUARD: Duration = Duration::from_secs(60);

/// What the loop wrote, and how many packets so far (shared with a
/// sender waiting on it).
#[derive(Clone, Default)]
struct Collected(Arc<(Mutex<Vec<u8>>, Condvar)>);

impl Collected {
    fn wait_for_packets(&self, n: usize) -> bool {
        let (data, changed) = &*self.0;
        let (_d, wait) = changed.wait_timeout_while(data.lock().unwrap(), HANG_GUARD, |d| d.len() / 188 < n).unwrap();
        !wait.timed_out()
    }

    fn take(&self) -> Vec<u8> {
        std::mem::take(&mut *self.0 .0.lock().unwrap())
    }
}

impl Sink for Collected {
    fn send(&mut self, packets: &[u8], _position: u64) -> io::Result<()> {
        let (data, changed) = &*self.0;
        data.lock().unwrap().extend_from_slice(packets);
        changed.notify_all();
        Ok(())
    }
}

/// A stream with video only (no KLV of its own).
fn video(frames: usize) -> Stream {
    generate(&Synth { frames, sync_klv: false, async_klv_every: None, ..Synth::default() })
}

/// The synchronous KLV units on the output: (PTS, packet).
fn klv(ts: &[u8]) -> Vec<(u64, misb::Packet)> {
    let mut d = Demuxer::new();
    d.subscribe(Subscription::new(Select::Codec(Codec::KlvSync), Unit::Pes).strip_metadata_au()).unwrap();
    d.push(ts);
    d.finish();
    let mut units = Vec::new();
    while let Some(o) = d.pop_output() {
        let p = misb::decode(o.payload()).unwrap();
        assert!(p.is_ok(), "{p:?}");
        units.push((o.pts.unwrap(), p));
        d.recycle(o);
    }
    units
}

fn lat(p: &misb::Packet) -> f64 {
    let v = p.get(Tag::SensorLatitude).and_then(Value::as_f64).unwrap();
    (v * 1e6).round() / 1e6
}

fn time_of(p: &misb::Packet) -> u64 {
    p.get(Tag::PrecisionTimeStamp).and_then(Value::as_time).unwrap()
}

fn live_mux() -> LiveMux {
    let mut cfg = LiveMuxConfig::new(Carriage::Sync);
    cfg.rate = Rate::EveryFrame;
    let mut mux = LiveMux::new(cfg).unwrap();
    mux.state_mut().set(Tag::SensorLatitude, 12.5, MispTime::from_micros(T0)).unwrap();
    mux.state_mut().set(Tag::MissionId, "LIVE", MispTime::from_micros(T0)).unwrap();
    mux
}

#[test]
fn a_file_goes_through_with_metadata_on_every_frame() {
    let s = video(50);
    let path = temp_file("live-in.ts", &s.data);
    let mut input = Input::open(&Endpoint::File(path.clone())).unwrap();
    let mut out = Collected::default();
    let mut mux = live_mux();
    let clock = ManualClock::new();
    let misp = MispClock::new(clock.now(), MispTime::from_micros(T0));
    let (reason, stats) = live::run(
        &mut input,
        &mut out,
        &mut mux,
        &[],
        &LiveConfig::default(),
        &clock,
        misp,
        &AtomicBool::new(false),
        &mut |_| {},
        &mut |_, _| {},
    )
    .unwrap();
    std::fs::remove_file(path).unwrap();
    assert_eq!(reason, StopReason::EndOfInput);
    let data = out.take();
    let units = klv(&data);
    assert_eq!(units.len(), 50);
    let pts0 = units[0].0;
    for (pts, p) in &units {
        // The simulated clock stands still: the first frame arrived at T0,
        // and every frame is timed by its PTS from there.
        assert_eq!(time_of(p), T0 + (pts - pts0) * 100 / 9);
        assert_eq!(lat(p), 12.5);
        assert_eq!(p.get(Tag::MissionId).and_then(Value::as_str), Some("LIVE"));
    }
    assert_eq!(stats.packets_in, s.packets as u64);
    assert_eq!(stats.packets_out, (data.len() / 188) as u64);
    let m = &stats.mux;
    assert_eq!((m.frames, m.units), (50, 50));
    assert_eq!(stats.packets_out, stats.packets_in + m.inject.klv_packets + m.inject.psi_extra_packets);
}

#[test]
fn a_control_line_changes_the_next_frame() {
    let s = video(30);
    let input = Input::open(&"udp://127.0.0.1:0".parse().unwrap()).unwrap();
    let port = input.local_addr().unwrap().port();
    let control = Control::open(&"udp://127.0.0.1:0".parse().unwrap()).unwrap();
    let control_addr = control.local_addr().unwrap();
    let out = Collected::default();
    let (applied_tx, applied_rx) = mpsc::channel();
    let total = s.packets as u64;
    let loop_out = out.clone();
    let runner = thread::spawn(move || {
        let mut input = input;
        let mut out = loop_out;
        let mut mux = live_mux();
        let mut cfg = LiveConfig::default();
        cfg.max_packets = Some(total);
        cfg.duration = Some(HANG_GUARD);
        let clock = SystemClock;
        let misp = MispClock::new(clock.now(), MispTime::from_micros(T0));
        live::run(
            &mut input,
            &mut out,
            &mut mux,
            &[control],
            &cfg,
            &clock,
            misp,
            &AtomicBool::new(false),
            &mut |_| {},
            &mut |line, outcome| {
                applied_tx.send((line.text.clone(), outcome.is_ok())).unwrap();
            },
        )
        .unwrap()
    });

    // Each frame in datagrams of up to 7 packets, one at a time: the next
    // goes once everything sent so far has come out (UDP may reorder
    // back-to-back datagrams on loopback). Before frame 15, a control line.
    let mut sender = Output::open(&format!("udp://127.0.0.1:{port}").parse().unwrap(), None).unwrap();
    let control_sender = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let mut sent = 0usize;
    for (k, frame) in s.data.chunks(PACKETS_PER_FRAME * 188).enumerate() {
        if k == 15 {
            control_sender.send_to(br#"{"set": {"SensorLatitude": 22.5}}"#, control_addr).unwrap();
            let (text, ok) = applied_rx.recv_timeout(HANG_GUARD).expect("the control line was applied");
            assert!(ok, "{text:?}");
        }
        for datagram in frame.chunks(7 * 188) {
            sender.send(datagram, 0).unwrap();
            sent += datagram.len() / 188;
            assert!(out.wait_for_packets(sent), "the live loop stopped passing packets on");
        }
    }
    let (reason, stats) = runner.join().unwrap();
    assert_eq!(reason, StopReason::Count);
    assert_eq!(stats.control_lines, 1);
    let units = klv(&out.take());
    assert_eq!(units.len(), 30);
    let pts0 = units[0].0;
    for (pts, p) in &units {
        let k = (pts - pts0) / 3600;
        assert_eq!(lat(p), if k >= 15 { 22.5 } else { 12.5 }, "frame {k}");
    }
}
