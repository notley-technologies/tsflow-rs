//! Synthetic transport streams for the tests and examples, built with the
//! `mpegts::mux` writers and `misb::encode`. No captured data is used.
//!
//! The stream is a constant-rate multiplex: every 40 ms frame (25 fps)
//! takes exactly [`PACKETS_PER_FRAME`] packets: PAT and PMT every second
//! frame, an H.264-like video PES with PTS and a PCR in its first packet,
//! a synchronous KLV access unit with the frame's PTS, an asynchronous KLV
//! packet every few frames, and null packets to fill the slot. The PCR of a
//! packet is exactly `START_PCR + index * TICKS_PER_PACKET`, so the
//! interpolated position of every packet is known exactly.

#![allow(dead_code)]

use misb::encode::{Classification, Input, Security, St0601Builder};
use misb::{st0601::Tag, MispTime};
use mpegts::mux::{PesUnit, PesWriter, ProgramTables, StreamSpec, NULL_PACKET};

pub const PACKET_SIZE: usize = 188;
pub const VIDEO_PID: u16 = 0x0100;
pub const SYNC_KLV_PID: u16 = 0x0101;
pub const ASYNC_KLV_PID: u16 = 0x0102;
pub const PMT_PID: u16 = 0x1000;
pub const PACKETS_PER_FRAME: usize = 80;
/// 40 ms of 27 MHz ticks over 80 packets: a 3.008 Mbit/s multiplex.
pub const TICKS_PER_PACKET: u64 = 1_080_000 / PACKETS_PER_FRAME as u64;
pub const MUX_RATE: u64 = PACKETS_PER_FRAME as u64 * 1504 * 25;
pub const START_PCR: u64 = 27_000_000 * 10;
/// 2023-11-14T22:13:20 UTC as MISP time (ST 0603: UTC + 37 s - 8.000082 s).
pub const MISP_START: u64 = 1_700_000_000_000_000 + 28_999_918;

/// What to put in the stream.
#[derive(Debug, Clone)]
pub struct Synth {
    pub frames: usize,
    pub sync_klv: bool,
    /// One asynchronous KLV packet every this many frames.
    pub async_klv_every: Option<usize>,
    pub classification: Classification,
    /// ST 0102 classifying country, e.g. "//US".
    pub country: &'static str,
}

impl Default for Synth {
    fn default() -> Self {
        Synth {
            frames: 50,
            sync_klv: true,
            async_klv_every: Some(5),
            classification: Classification::Unclassified,
            country: "//US",
        }
    }
}

impl Synth {
    pub fn frames(frames: usize) -> Synth {
        Synth { frames, ..Synth::default() }
    }
}

/// A generated stream and what it contains.
#[derive(Debug, Clone)]
pub struct Stream {
    pub data: Vec<u8>,
    pub packets: usize,
    pub frames: usize,
    pub sync_klv: usize,
    pub async_klv: usize,
}

impl Stream {
    /// Stream time covered, in 27 MHz ticks.
    pub fn duration_ticks(&self) -> u64 {
        self.packets as u64 * TICKS_PER_PACKET
    }
}

fn st0601(frame: usize, security: &[u8]) -> Vec<u8> {
    let mut b = St0601Builder::new(MispTime::from_micros(MISP_START + frame as u64 * 40_000));
    b.set(Tag::MissionId, "SYNTH").unwrap();
    b.set(Tag::PlatformHeadingAngle, (frame as f64 * 1.5) % 360.0).unwrap();
    b.set(Tag::SecurityLocalSet, Input::Set(security.to_vec())).unwrap();
    b.finish().unwrap()
}

/// Writes a synthetic stream one frame slot at a time, so long streams
/// need not be held in memory.
pub struct Generator {
    cfg: Synth,
    tables: ProgramTables,
    video_writer: PesWriter,
    sync_writer: PesWriter,
    async_writer: PesWriter,
    security: Vec<u8>,
    frame: Vec<u8>,
    /// Frames and packets written so far.
    pub frames: usize,
    pub packets: usize,
    pub sync_klv: usize,
    pub async_klv: usize,
}

impl Generator {
    pub fn new(cfg: &Synth) -> Generator {
        let mut tables = ProgramTables::new(1);
        tables.add_program(1, PMT_PID, Some(VIDEO_PID)).unwrap();
        let video = StreamSpec::h264();
        let sync = StreamSpec::klv_sync(0);
        let asynchronous = StreamSpec::klv_async();
        tables.add_stream(1, VIDEO_PID, &video).unwrap();
        if cfg.sync_klv {
            tables.add_stream(1, SYNC_KLV_PID, &sync).unwrap();
        }
        if cfg.async_klv_every.is_some() {
            tables.add_stream(1, ASYNC_KLV_PID, &asynchronous).unwrap();
        }
        Generator {
            cfg: cfg.clone(),
            tables,
            video_writer: PesWriter::new(VIDEO_PID, &video).unwrap(),
            sync_writer: PesWriter::new(SYNC_KLV_PID, &sync).unwrap(),
            async_writer: PesWriter::new(ASYNC_KLV_PID, &asynchronous).unwrap(),
            security: Security::new(cfg.classification, 0x0D, cfg.country, 0x0D, "US").encode().unwrap(),
            frame: Vec::new(),
            frames: 0,
            packets: 0,
            sync_klv: 0,
            async_klv: 0,
        }
    }

    /// Append the next frame slot (exactly `PACKETS_PER_FRAME` packets).
    pub fn frame(&mut self, out: &mut Vec<u8>) {
        let k = self.frames;
        let base = out.len();
        let slot = self.packets;
        if k % 2 == 0 {
            self.tables.write_all(out).unwrap();
        }
        let index = (slot + (out.len() - base) / PACKET_SIZE) as u64;
        let pcr = START_PCR + index * TICKS_PER_PACKET;
        // Presented 200 ms after the frame slot starts.
        let pts = (START_PCR + slot as u64 * TICKS_PER_PACKET) / 300 + 18_000;
        let size = if k % 25 == 0 { 70 * 184 - 40 } else { 50 * 184 - 40 };
        self.frame.clear();
        self.frame.extend_from_slice(&[0, 0, 0, 1, 0x09, 0xF0]);
        self.frame.extend((6..size).map(|i| ((i * 7 + k) as u8) | 1));
        let mut unit = PesUnit::new(&self.frame).with_pts(pts).with_pcr(pcr).with_data_alignment();
        if k % 25 == 0 {
            unit = unit.with_random_access();
        }
        self.video_writer.write(&unit, out).unwrap();
        if self.cfg.sync_klv {
            let klv = st0601(k, &self.security);
            self.sync_writer.write(&PesUnit::new(&klv).with_pts(pts), out).unwrap();
            self.sync_klv += 1;
        }
        if self.cfg.async_klv_every.is_some_and(|n| k % n == 0) {
            let klv = st0601(k, &self.security);
            self.async_writer.write(&PesUnit::new(&klv), out).unwrap();
            self.async_klv += 1;
        }
        let end = base + PACKETS_PER_FRAME * PACKET_SIZE;
        assert!(out.len() <= end, "frame {k} overflows its slot");
        while out.len() < end {
            out.extend_from_slice(&NULL_PACKET);
        }
        self.frames += 1;
        self.packets += PACKETS_PER_FRAME;
    }
}

/// Generate a whole stream in memory.
pub fn generate(cfg: &Synth) -> Stream {
    let mut g = Generator::new(cfg);
    let mut data = Vec::with_capacity(cfg.frames * PACKETS_PER_FRAME * PACKET_SIZE);
    for _ in 0..cfg.frames {
        g.frame(&mut data);
    }
    Stream { packets: g.packets, data, frames: g.frames, sync_klv: g.sync_klv, async_klv: g.async_klv }
}

/// The same packets as 192-byte M2TS records (4-byte arrival time prefix).
pub fn to_m2ts(ts: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(ts.len() / PACKET_SIZE * 192);
    for (i, p) in ts.chunks(PACKET_SIZE).enumerate() {
        out.extend_from_slice(&((i as u32).wrapping_mul(13_500) & 0x3FFF_FFFF).to_be_bytes());
        out.extend_from_slice(p);
    }
    out
}

/// Remove the `n`th packet of `pid` (a continuity error for a receiver).
pub fn drop_packet(ts: &[u8], pid: u16, n: usize) -> Vec<u8> {
    let mut seen = 0;
    let mut out = Vec::with_capacity(ts.len());
    for p in ts.chunks(PACKET_SIZE) {
        let this = (u16::from(p[1] & 0x1F) << 8) | u16::from(p[2]);
        if this == pid {
            seen += 1;
            if seen == n + 1 {
                continue;
            }
        }
        out.extend_from_slice(p);
    }
    out
}

/// A unique path under the integration test temporary directory (the
/// system's for examples).
pub fn temp_path(name: &str) -> std::path::PathBuf {
    let dir = option_env!("CARGO_TARGET_TMPDIR").map_or_else(std::env::temp_dir, std::path::PathBuf::from);
    dir.join(format!("{}-{}", std::process::id(), name))
}

/// Write `data` to a temporary file and return its path.
pub fn temp_file(name: &str, data: &[u8]) -> std::path::PathBuf {
    let path = temp_path(name);
    std::fs::write(&path, data).unwrap();
    path
}
