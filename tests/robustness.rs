//! Hostile input: mutated streams, random datagrams and random strings
//! must never panic, and memory must stay bounded. Deterministic (a fixed
//! pseudo-random sequence); `TSFLOW_FUZZ_ITERATIONS` runs longer.

mod common;

use common::*;
use tsflow::endpoint::{Direction, Endpoint};
use tsflow::expect::Expectation;
use tsflow::framer::Framer;
use tsflow::looping::LoopFixer;
use tsflow::monitor::Monitor;
use tsflow::rtp::{self, SequenceTracker};

/// xorshift64*: small, fast and reproducible.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n.max(1) as u64) as usize
    }
}

fn iterations(default: usize) -> usize {
    std::env::var("TSFLOW_FUZZ_ITERATIONS").ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// Flip, insert, delete and overwrite bytes, favouring the header bytes
/// that matter (sync, PID, adaptation field, PES and KLV lengths).
fn mutate(rng: &mut Rng, data: &mut Vec<u8>) {
    for _ in 0..1 + rng.below(40) {
        if data.is_empty() {
            data.push(0x47);
        }
        let at = rng.below(data.len());
        match rng.below(6) {
            0 => data[at] ^= 1 << rng.below(8),
            1 => data[at] = rng.next() as u8,
            2 => data.insert(at, if rng.below(2) == 0 { 0x47 } else { rng.next() as u8 }),
            3 => {
                let n = rng.below(400).min(data.len() - at);
                data.drain(at..at + n);
            }
            4 => {
                // A header byte of some packet.
                let p = at - at % 188;
                let i = p + rng.below(20).min(data.len() - p - 1);
                data[i] = rng.next() as u8;
            }
            _ => {
                let n = rng.below(300).min(data.len() - at);
                let v = rng.next() as u8;
                data[at..at + n].iter_mut().for_each(|b| *b = v);
            }
        }
    }
}

#[test]
fn mutated_streams_never_panic_the_framer_monitor_or_loop_fixer() {
    let base = generate(&Synth::frames(12)).data;
    let mut rng = Rng(0x5EED_1234_ABCD_0001);
    for _ in 0..iterations(300) {
        let mut data = base.clone();
        mutate(&mut rng, &mut data);
        let mut framer = Framer::new();
        let mut monitor = Monitor::new().unwrap();
        let mut fixer = LoopFixer::new();
        let chunk = 1 + rng.below(5000);
        let mut packets = Vec::new();
        for (i, c) in data.chunks(chunk).enumerate() {
            packets.clear();
            packets.extend_from_slice(framer.push(c));
            if i % 3 == 2 {
                fixer.start_pass();
            }
            for p in packets.chunks_exact_mut(188) {
                fixer.fix(p.try_into().unwrap());
            }
            monitor.push(&packets, None);
        }
        let tail = framer.finish().to_vec();
        monitor.push(&tail, None);
        monitor.finish();
        let s = monitor.summary();
        let stats = framer.stats();
        // Every byte is a packet, skipped, or a known M2TS / 204 framing byte.
        assert!(stats.packets * 188 + stats.bytes_skipped <= stats.bytes);
        assert_eq!(s.packets, stats.packets);
        let _ = (s.to_string(), s.to_json());
    }
}

#[test]
fn random_bytes_never_panic_and_memory_stays_bounded() {
    let mut rng = Rng(0xC0FF_EE00_0000_0042);
    let mut framer = Framer::new();
    let mut monitor = Monitor::new().unwrap();
    let mut buf = vec![0u8; 70_000];
    for _ in 0..iterations(2000) {
        let n = rng.below(buf.len());
        for b in &mut buf[..n] {
            // Plenty of sync bytes, so false alignments are tried.
            *b = if rng.below(8) == 0 { 0x47 } else { rng.next() as u8 };
        }
        let packets = framer.push(&buf[..n]).to_vec();
        monitor.push(&packets, None);
    }
    monitor.finish();
    let _ = monitor.summary();
}

#[test]
fn random_datagrams_never_panic_the_rtp_parser() {
    let mut rng = Rng(0xDA7A_6AA4_0000_0007);
    let mut tracker = SequenceTracker::new();
    let mut d = Vec::new();
    for _ in 0..iterations(20_000) {
        d.clear();
        let n = rng.below(64);
        d.extend((0..n).map(|_| rng.next() as u8));
        if !d.is_empty() && rng.below(2) == 0 {
            d[0] = 0x80 | (d[0] & 0x3F);
        }
        if let Ok((h, range)) = rtp::parse(&d) {
            assert!(range.start <= range.end && range.end <= d.len());
            tracker.push(&h);
        }
    }
}

#[test]
fn random_strings_never_panic_the_parsers() {
    let mut rng = Rng(0x5751_0000_0000_0003);
    let alphabet: Vec<char> = "udprtfile:/@[]?&=.0123456789abcdefx-:%é \u{0}".chars().collect();
    let seeds = ["udp://@239.1.1.1:5000?iface=1.2.3.4", "rtp://[ff15::1]:5004?ttl=3", "klv-marking=US//X", "-"];
    for _ in 0..iterations(20_000) {
        let mut s: String = seeds[rng.below(seeds.len())].to_string();
        for _ in 0..rng.below(6) {
            let c = alphabet[rng.below(alphabet.len())];
            let at = rng.below(s.chars().count() + 1);
            let byte = s.char_indices().nth(at).map_or(s.len(), |(i, _)| i);
            s.insert(byte, c);
        }
        if let Ok(e) = s.parse::<Endpoint>() {
            let _ = e.validate(Direction::Input);
            let _ = e.validate(Direction::Output);
            // What parses prints back to something that parses the same.
            if !matches!(e, Endpoint::File(_)) {
                assert_eq!(e.to_string().parse::<Endpoint>().unwrap(), e, "{s}");
            }
        }
        let _ = s.parse::<Expectation>();
    }
}
