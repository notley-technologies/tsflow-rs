//! Real sockets on 127.0.0.1: a synthetic stream played over UDP and RTP and
//! read back by tsflow's input, monitor and receive loop.
//!
//! These tests check the data path only: every packet arrives, in order and
//! unchanged, continuity holds, RTP is framed and numbered, forwarding
//! relays datagrams as they were. They never measure time. The sender runs
//! on a simulated clock (its exact pacing is tested in tests/play.rs), and
//! instead of real-time pacing it sends in lockstep with the last receiver:
//! each datagram only once the previous one has been read ([`WINDOW`]).
//! Receivers stop on an exact packet count. Real time appears only as a
//! hang guard that turns lost data into a failure instead of a test that
//! never ends.

mod common;

use common::*;
use std::io;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use tsflow::clock::ManualClock;
use tsflow::endpoint::Endpoint;
use tsflow::input::{Input, ReadEvent};
use tsflow::monitor::{Monitor, Summary};
use tsflow::output::{Output, Sink};
use tsflow::play::{self, PlayConfig, PlayStats};
use tsflow::recv::{self, RecvConfig, StopReason};

/// Datagrams in flight at most. UDP does not promise order, and loopback
/// does reorder back-to-back datagrams when the sending thread moves to
/// another CPU between them (each is queued on the current CPU's backlog;
/// seen here as whole datagrams out of order). With one in flight the
/// order is certain, nothing can overflow, and the tests can check every
/// byte.
const WINDOW: u64 = 1;
/// Hang guard only: data that has not arrived by then is lost.
const HANG_GUARD: Duration = Duration::from_secs(60);
/// How long one socket read waits before checking the hang guard.
const READ_STEP: Duration = Duration::from_millis(100);

/// Datagrams the last receiver has read, shared with the sender.
#[derive(Clone, Default)]
struct Progress(Arc<(Mutex<u64>, Condvar)>);

impl Progress {
    fn add(&self) {
        let (count, changed) = &*self.0;
        *count.lock().unwrap() += 1;
        changed.notify_all();
    }

    /// Wait until at least `n` datagrams were read; false if the hang guard
    /// ran out first.
    fn wait_for(&self, n: u64) -> bool {
        let (count, changed) = &*self.0;
        let (_count, wait) = changed.wait_timeout_while(count.lock().unwrap(), HANG_GUARD, |got| *got < n).unwrap();
        !wait.timed_out()
    }
}

/// An output that sends a datagram only when fewer than [`WINDOW`] are
/// still unread at the far end.
struct FlowControlled {
    out: Output,
    sent: u64,
    progress: Progress,
}

impl Sink for FlowControlled {
    fn send(&mut self, packets: &[u8], position: u64) -> io::Result<()> {
        if self.sent >= WINDOW && !self.progress.wait_for(self.sent + 1 - WINDOW) {
            return Err(io::Error::new(io::ErrorKind::TimedOut, "the receiver stopped reading: datagrams were lost"));
        }
        self.out.send(packets, position)?;
        self.sent += 1;
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.out.flush()
    }
}

fn listen(scheme: &str) -> (Input, u16) {
    let input = Input::open(&format!("{scheme}://127.0.0.1:0").parse().unwrap()).unwrap();
    let port = input.local_addr().unwrap().port();
    (input, port)
}

/// What a receiver got.
struct Received {
    summary: Summary,
    /// The TS packets, back to back.
    data: Vec<u8>,
}

/// Read `input` on a thread until `packets` TS packets have arrived,
/// counting each datagram in `progress`.
fn collect(mut input: Input, packets: u64, progress: Progress) -> JoinHandle<Received> {
    thread::spawn(move || {
        let mut monitor = Monitor::new().unwrap();
        let mut data = Vec::new();
        let give_up = std::time::Instant::now() + HANG_GUARD;
        while ((data.len() / PACKET_SIZE) as u64) < packets && std::time::Instant::now() < give_up {
            if let ReadEvent::Data(chunk) = input.read(READ_STEP).unwrap() {
                monitor.push(chunk.packets, None);
                data.extend_from_slice(chunk.packets);
                progress.add();
            }
        }
        monitor.finish();
        monitor.set_input_stats(input.stats());
        Received { summary: monitor.summary(), data }
    })
}

/// Play `data` to `output` on a simulated clock, flow controlled by
/// `progress`.
fn play_to(data: &[u8], name: &str, output: &str, passes: u64, progress: &Progress) -> PlayStats {
    let path = temp_file(name, data);
    let mut input = Input::open(&Endpoint::File(path.clone())).unwrap();
    let out = Output::open(&output.parse().unwrap(), None).unwrap();
    let mut sink = FlowControlled { out, sent: 0, progress: progress.clone() };
    let mut cfg = PlayConfig::default();
    cfg.latency = Duration::from_millis(50);
    cfg.fallback_bitrate = MUX_RATE;
    cfg.passes = Some(passes);
    let clock = ManualClock::new();
    let stats = play::play(&mut input, &mut sink, &cfg, &clock, &AtomicBool::new(false), &mut |_| {}).unwrap();
    std::fs::remove_file(path).unwrap();
    stats
}

/// The stream arrived complete and intact.
fn check_delivery(r: &Summary, s: &Stream, passes: u64) {
    let n = s.packets as u64 * passes;
    assert_eq!(r.packets, n);
    assert_eq!(r.cc_errors, 0);
    assert_eq!(r.input.unwrap().datagrams, n.div_ceil(7));
    let sync = r.klv.iter().find(|k| k.sync).unwrap();
    assert_eq!(sync.packets, s.sync_klv as u64 * passes);
    assert_eq!(sync.markings, vec![("US//UNCLASSIFIED".to_string(), sync.packets)]);
}

#[test]
fn udp_carries_every_packet_unchanged() {
    let s = generate(&Synth::frames(50));
    let (input, port) = listen("udp");
    let progress = Progress::default();
    let rx = collect(input, s.packets as u64, progress.clone());
    let stats = play_to(&s.data, "udp.ts", &format!("udp://127.0.0.1:{port}"), 1, &progress);
    let r = rx.join().unwrap();
    assert_eq!(stats.packets_out, s.packets as u64);
    assert_eq!(r.data, s.data, "the packets as sent, in order");
    check_delivery(&r.summary, &s, 1);
    let input = r.summary.input.unwrap();
    assert_eq!((input.rtp_datagrams, input.rtp), (0, None));
}

#[test]
fn rtp_is_detected_and_its_sequence_checked() {
    let s = generate(&Synth::frames(40));
    let (input, port) = listen("rtp");
    let progress = Progress::default();
    let rx = collect(input, s.packets as u64, progress.clone());
    play_to(&s.data, "rtp.ts", &format!("rtp://127.0.0.1:{port}?ssrc=1234"), 1, &progress);
    let r = rx.join().unwrap();
    assert_eq!(r.data, s.data, "the RTP payloads are the packets as sent");
    check_delivery(&r.summary, &s, 1);
    let input = r.summary.input.unwrap();
    assert_eq!(input.rtp_datagrams, input.datagrams);
    let rtp = input.rtp.unwrap();
    assert_eq!((rtp.received, rtp.lost, rtp.late, rtp.restarts), (input.datagrams, 0, 0, 0));
}

#[test]
fn a_looping_file_is_one_continuous_stream_for_the_receiver() {
    let s = generate(&Synth::frames(25));
    let (input, port) = listen("udp");
    let progress = Progress::default();
    let rx = collect(input, 3 * s.packets as u64, progress.clone());
    let stats = play_to(&s.data, "loop.ts", &format!("udp://127.0.0.1:{port}"), 3, &progress);
    let r = rx.join().unwrap();
    assert_eq!((stats.pass, stats.pcr_discontinuities), (3, 2));
    check_delivery(&r.summary, &s, 3);
    let pcr = &r.summary.pcr[0];
    assert_eq!((pcr.discontinuities, pcr.jumps), (2, 0));
}

#[test]
fn forwarding_relays_every_datagram_unchanged() {
    // sender -> [rtp] -> recv::run, forwarding -> [rtp] -> tap
    let s = generate(&Synth::frames(30));
    let packets = s.packets as u64;
    let (tap, tap_port) = listen("rtp");
    let progress = Progress::default();
    let tap_rx = collect(tap, packets, progress.clone());
    let (mut input, port) = listen("rtp");
    let stop = Arc::new(AtomicBool::new(false));
    let relay_stop = stop.clone();
    let relay = thread::spawn(move || {
        let mut forward = Output::open(&format!("rtp://127.0.0.1:{tap_port}").parse().unwrap(), None).unwrap();
        let mut monitor = Monitor::new().unwrap();
        let mut cfg = RecvConfig::default();
        cfg.max_packets = Some(packets);
        let clock = ManualClock::new();
        let reason =
            recv::run(&mut input, &mut monitor, Some(&mut forward), None, &cfg, &clock, &relay_stop, &mut |_, _| {})
                .unwrap();
        (monitor.summary(), reason)
    });
    play_to(&s.data, "forward.ts", &format!("rtp://127.0.0.1:{port}"), 1, &progress);
    let t = tap_rx.join().unwrap();
    // The relay has stopped on its count by now; the flag only frees it if
    // data was lost.
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let (r, reason) = relay.join().unwrap();
    assert_eq!(reason, StopReason::Count);
    check_delivery(&r, &s, 1);
    check_delivery(&t.summary, &s, 1);
    assert_eq!(t.data, s.data);
    let (a, b) = (r.input.unwrap(), t.summary.input.unwrap());
    assert_eq!(a.datagrams, b.datagrams);
    // The RTP headers were relayed as they were: same sequence, no gaps.
    assert_eq!(b.rtp.unwrap().received, a.rtp.unwrap().received);
    assert_eq!(b.rtp.unwrap().lost, 0);
}
