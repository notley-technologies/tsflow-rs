//! Turning a byte stream into 188-byte TS packets.
//!
//! [`Framer`] finds the packet alignment of any byte stream (a file, a
//! pipe, the payloads of a series of datagrams) and hands out whole
//! 188-byte packets. It recognises three record sizes:
//!
//! - 188 bytes: plain transport stream (ISO/IEC 13818-1 2.4.3.2);
//! - 192 bytes: M2TS / BDAV, a 4-byte TP_extra_header (arrival time stamp)
//!   before each packet, which is stripped;
//! - 204 bytes: 188 bytes followed by 16 bytes of Reed-Solomon parity,
//!   which are stripped.
//!
//! Alignment is trusted once a sync byte (0x47) is followed by
//! [`SYNC_CONFIRMATIONS`] more at the same spacing. A packet whose sync
//! byte is missing counts as a sync loss and the search starts again, one
//! byte further on; skipped bytes are counted. Memory is bounded by the
//! largest single push plus one partial packet.

use crate::PACKET_SIZE;

/// Sync bytes that must follow a candidate at the same spacing before the
/// alignment is trusted.
pub const SYNC_CONFIRMATIONS: usize = 3;
const SYNC_BYTE: u8 = 0x47;
/// The longest record prefix (M2TS).
const MAX_PREFIX: usize = 4;

/// The record format of a stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum PacketFormat {
    /// 188-byte packets.
    Ts,
    /// 192-byte M2TS records (4-byte prefix).
    M2ts,
    /// 204-byte records (16 bytes of parity after the packet).
    Ts204,
}

impl PacketFormat {
    const ALL: [PacketFormat; 3] = [PacketFormat::Ts, PacketFormat::M2ts, PacketFormat::Ts204];

    /// Bytes per record.
    pub fn record_size(self) -> usize {
        match self {
            PacketFormat::Ts => 188,
            PacketFormat::M2ts => 192,
            PacketFormat::Ts204 => 204,
        }
    }

    /// Bytes of each record before the sync byte.
    fn prefix(self) -> usize {
        match self {
            PacketFormat::M2ts => 4,
            _ => 0,
        }
    }

    /// A short name: "188-byte TS", "192-byte M2TS", "204-byte TS".
    pub fn name(self) -> &'static str {
        match self {
            PacketFormat::Ts => "188-byte TS",
            PacketFormat::M2ts => "192-byte M2TS",
            PacketFormat::Ts204 => "204-byte TS",
        }
    }
}

/// Counters kept by a [`Framer`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct FramerStats {
    /// Bytes received.
    pub bytes: u64,
    /// Packets handed out.
    pub packets: u64,
    /// Bytes that were not part of any packet (garbage, damaged records,
    /// a truncated packet at the end), not counting the expected M2TS
    /// prefixes and 204-byte parity.
    pub bytes_skipped: u64,
    /// Times the alignment was lost after it had been found.
    pub sync_losses: u64,
    /// The record format, once found.
    pub format: Option<PacketFormat>,
}

/// What a position holds under the current alignment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum At {
    Packet,
    /// The start of a record whose end has not arrived.
    Partial,
    /// Not a packet: the alignment is lost.
    Lost,
    /// More data is needed to tell.
    Undecided,
}

/// Where the next run of packets is.
#[derive(Debug, Clone)]
pub(crate) enum Run {
    /// Contiguous 188-byte packets in the input buffer.
    InPlace(usize, usize),
    /// Copied into the output buffer (192 and 204-byte records).
    Copied,
}

/// Finds packet alignment in a byte stream and yields 188-byte packets.
///
/// ```
/// use tsflow::framer::{Framer, PacketFormat};
///
/// // Five M2TS records: a 4-byte prefix before each packet.
/// let mut data = Vec::new();
/// for _ in 0..5 {
///     data.extend_from_slice(&[0, 0, 0, 0]);
///     data.push(0x47);
///     data.extend_from_slice(&[0x1F, 0xFF, 0x10]);
///     data.extend_from_slice(&[0xFF; 184]);
/// }
/// let mut framer = Framer::new();
/// let n = framer.push(&data).len() + framer.finish().len();
/// assert_eq!(n, 5 * 188);
/// assert_eq!(framer.stats().format, Some(PacketFormat::M2ts));
/// assert_eq!(framer.stats().bytes_skipped, 0);
/// ```
#[derive(Debug, Clone)]
pub struct Framer {
    buf: Vec<u8>,
    start: usize,
    end: usize,
    format: Option<PacketFormat>,
    out: Vec<u8>,
    stats: FramerStats,
}

impl Default for Framer {
    fn default() -> Self {
        Framer::new()
    }
}

impl Framer {
    /// A framer that has seen nothing.
    pub fn new() -> Framer {
        Framer { buf: Vec::new(), start: 0, end: 0, format: None, out: Vec::new(), stats: FramerStats::default() }
    }

    /// The counters so far.
    pub fn stats(&self) -> &FramerStats {
        &self.stats
    }

    /// Forget buffered bytes and the alignment (for example after seeking).
    /// The counters are kept.
    pub fn reset(&mut self) {
        self.start = 0;
        self.end = 0;
        self.format = None;
        self.out.clear();
    }

    /// Add bytes and return the packets that are now complete (whole
    /// 188-byte packets back to back; empty when more data is needed).
    /// Call [`packets`](Self::packets) until it is empty before pushing
    /// again only if a sync loss could leave more behind; `push` already
    /// returns every packet it can.
    pub fn push(&mut self, data: &[u8]) -> &mut [u8] {
        self.spare(data.len())[..data.len()].copy_from_slice(data);
        self.commit(data.len());
        self.collect(false)
    }

    /// Signal the end of the stream: the last packets are accepted with
    /// fewer confirmations, and a truncated trailing packet is counted as
    /// skipped. Returns the packets that completes.
    pub fn finish(&mut self) -> &mut [u8] {
        self.collect(true)
    }

    /// The next run of complete packets without adding data (empty when
    /// more data is needed). `eof` relaxes the confirmations as in
    /// [`finish`](Self::finish).
    pub fn packets(&mut self, eof: bool) -> &mut [u8] {
        match self.next_run(eof) {
            Some(run) => self.slice(run),
            None => &mut [],
        }
    }

    /// Room for at least `min` more bytes, to read into directly; follow
    /// with [`commit`](Self::commit).
    pub fn spare(&mut self, min: usize) -> &mut [u8] {
        if self.start > 0 && self.buf.len() - self.end < min {
            self.buf.copy_within(self.start..self.end, 0);
            self.end -= self.start;
            self.start = 0;
        }
        if self.buf.len() - self.end < min {
            self.buf.resize(self.end + min, 0);
        }
        &mut self.buf[self.end..]
    }

    /// Mark `n` bytes of the [`spare`](Self::spare) room as filled.
    pub fn commit(&mut self, n: usize) {
        let n = n.min(self.buf.len() - self.end);
        self.end += n;
        self.stats.bytes += n as u64;
    }

    /// Every packet available now, joined (runs interrupted by a sync loss
    /// are merged by copying).
    fn collect(&mut self, eof: bool) -> &mut [u8] {
        let Some(first) = self.next_run(eof) else { return &mut [] };
        // Usually one run covers everything. Keep a copied first run safe
        // while looking for a second one.
        let saved = match first {
            Run::Copied => Some(std::mem::take(&mut self.out)),
            Run::InPlace(..) => None,
        };
        let Some(second) = self.next_run(eof) else {
            if let Some(saved) = saved {
                self.out = saved;
            }
            return self.slice(first);
        };
        let mut joined = saved.unwrap_or_default();
        if let Run::InPlace(a, b) = first {
            joined.extend_from_slice(&self.buf[a..b]);
        }
        let mut next = Some(second);
        while let Some(run) = next {
            match run {
                Run::InPlace(a, b) => joined.extend_from_slice(&self.buf[a..b]),
                Run::Copied => joined.extend_from_slice(&self.out),
            }
            next = self.next_run(eof);
        }
        self.out = joined;
        &mut self.out
    }

    pub(crate) fn slice(&mut self, run: Run) -> &mut [u8] {
        match run {
            Run::InPlace(a, b) => &mut self.buf[a..b],
            Run::Copied => &mut self.out,
        }
    }

    /// Find the next run of packets, consuming it.
    pub(crate) fn next_run(&mut self, eof: bool) -> Option<Run> {
        loop {
            let format = match self.format {
                Some(f) => f,
                None => {
                    if !self.acquire(eof) {
                        return None;
                    }
                    continue;
                }
            };
            let stride = format.record_size();
            let mut p = self.start;
            if format == PacketFormat::Ts {
                while self.classify(p, stride, eof) == At::Packet {
                    p += PACKET_SIZE;
                }
                if p > self.start {
                    let run = Run::InPlace(self.start, p);
                    self.stats.packets += ((p - self.start) / PACKET_SIZE) as u64;
                    self.start = p;
                    return Some(run);
                }
            } else {
                // The cursor sits on a sync byte; the record's prefix (M2TS)
                // was consumed with the previous record.
                self.out.clear();
                while self.classify(p, stride, eof) == At::Packet {
                    self.out.extend_from_slice(&self.buf[p..p + PACKET_SIZE]);
                    p = (p + stride).min(self.end);
                }
                if !self.out.is_empty() {
                    self.stats.packets += (self.out.len() / PACKET_SIZE) as u64;
                    self.start = p;
                    return Some(Run::Copied);
                }
            }
            let avail = self.end - self.start;
            if avail == 0 {
                return None;
            }
            match self.classify(self.start, stride, eof) {
                At::Partial => {
                    // Wait for the rest, or drop it at the end.
                    if eof {
                        self.skip(avail);
                    }
                    return None;
                }
                At::Undecided => return None,
                At::Packet | At::Lost => {
                    self.stats.sync_losses += 1;
                    self.format = None;
                }
            }
        }
    }

    /// What is at `p` under the current alignment.
    ///
    /// A record is complete once its 188 bytes and, except at the end of
    /// the stream, the bytes up to the next record's sync byte are here (so
    /// an M2TS prefix is never split from its packet). It is a packet when
    /// it starts with a sync byte and the next record does too. When the
    /// next one does not (damage follows), the record is still a packet
    /// unless an aligned run of packets starts inside it: then it is the
    /// head of a truncated or stray packet that would swallow the real one
    /// after it, and alignment is lost.
    fn classify(&self, p: usize, stride: usize, eof: bool) -> At {
        let complete = p + stride <= self.end || (eof && p + PACKET_SIZE <= self.end);
        let sync = self.buf[..self.end].get(p) == Some(&SYNC_BYTE);
        if !complete {
            return if sync { At::Partial } else { At::Lost };
        }
        if !sync {
            return At::Lost;
        }
        match self.buf[..self.end].get(p + stride) {
            None | Some(&SYNC_BYTE) => At::Packet,
            Some(_) => match self.overlapped(p, eof) {
                Some(false) => At::Packet,
                Some(true) => At::Lost,
                None => At::Undecided,
            },
        }
    }

    /// Whether a confirmed run of packets starts inside the 188 bytes at
    /// `p`; `None` when more data is needed to tell.
    fn overlapped(&self, p: usize, eof: bool) -> Option<bool> {
        let mut undecided = false;
        for q in p + 1..(p + PACKET_SIZE).min(self.end) {
            if self.buf[q] != SYNC_BYTE {
                continue;
            }
            for format in PacketFormat::ALL {
                match self.confirm(q, format.record_size(), eof) {
                    Some(true) => return Some(true),
                    Some(false) => {}
                    None => undecided = true,
                }
            }
        }
        if undecided {
            None
        } else {
            Some(false)
        }
    }

    fn skip(&mut self, n: usize) {
        self.stats.bytes_skipped += n as u64;
        self.start += n;
    }

    /// Search for alignment from `start`. Returns true once found (with
    /// `start` on the first sync byte), false when more data is needed.
    fn acquire(&mut self, eof: bool) -> bool {
        let mut p = self.start;
        while p < self.end {
            let Some(offset) = self.buf[p..self.end].iter().position(|&b| b == SYNC_BYTE) else {
                p = self.end;
                break;
            };
            p += offset;
            let mut undecided = false;
            for format in PacketFormat::ALL {
                match self.confirm(p, format.record_size(), eof) {
                    Some(true) => {
                        // Bytes before the first sync are skipped, except an
                        // M2TS prefix where one is expected.
                        let gap = p - self.start;
                        let expected = format.prefix().min(gap);
                        self.stats.bytes_skipped += (gap - expected) as u64;
                        self.start = p;
                        self.format = Some(format);
                        self.stats.format = Some(format);
                        return true;
                    }
                    Some(false) => {}
                    None => undecided = true,
                }
            }
            if undecided {
                // Keep the candidate, and what may be its M2TS prefix, and
                // wait for more data.
                self.skip(p.saturating_sub(MAX_PREFIX).max(self.start) - self.start);
                return false;
            }
            p += 1;
        }
        // No candidate: everything so far is garbage, except the last bytes,
        // which may be the prefix of a record whose sync byte is still to
        // come.
        let keep = if eof { 0 } else { MAX_PREFIX };
        self.skip(p.saturating_sub(keep).max(self.start) - self.start);
        false
    }

    /// Whether `p` starts a run of packets `stride` apart: `Some(true)` or
    /// `Some(false)` when decided, `None` when more data is needed.
    fn confirm(&self, p: usize, stride: usize, eof: bool) -> Option<bool> {
        if p + PACKET_SIZE > self.end {
            return if eof { Some(false) } else { None };
        }
        for k in 1..=SYNC_CONFIRMATIONS {
            let q = p + k * stride;
            if q >= self.end {
                // At the end of the stream, what is there must agree.
                return if eof { Some(true) } else { None };
            }
            if self.buf[q] != SYNC_BYTE {
                return Some(false);
            }
        }
        Some(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `n` distinct packets: PID = index, so order and content can be
    /// checked.
    fn packets(n: usize) -> Vec<u8> {
        let mut v = Vec::new();
        for i in 0..n {
            let mut p = [0xFFu8; PACKET_SIZE];
            p[0] = SYNC_BYTE;
            p[1] = (i >> 8) as u8 & 0x1F;
            p[2] = i as u8;
            p[3] = 0x10;
            p[4] = (i * 7) as u8; // no 0x47 at the confirmation offsets
            v.extend_from_slice(&p);
        }
        v
    }

    fn records(ts: &[u8], format: PacketFormat) -> Vec<u8> {
        let mut v = Vec::new();
        for (i, p) in ts.chunks(PACKET_SIZE).enumerate() {
            match format {
                PacketFormat::Ts => v.extend_from_slice(p),
                PacketFormat::M2ts => {
                    v.extend_from_slice(&(i as u32 * 1000).to_be_bytes());
                    v.extend_from_slice(p);
                }
                PacketFormat::Ts204 => {
                    v.extend_from_slice(p);
                    v.extend_from_slice(&[0x5A; 16]);
                }
            }
        }
        v
    }

    fn run_all(framer: &mut Framer, data: &[u8], chunk: usize) -> Vec<u8> {
        let mut out = Vec::new();
        for c in data.chunks(chunk.max(1)) {
            out.extend_from_slice(framer.push(c));
            loop {
                let more = framer.packets(false);
                if more.is_empty() {
                    break;
                }
                out.extend_from_slice(more);
            }
        }
        loop {
            let more = framer.finish();
            if more.is_empty() {
                break;
            }
            out.extend_from_slice(more);
        }
        out
    }

    #[test]
    fn all_formats_any_chunking() {
        let ts = packets(40);
        for format in PacketFormat::ALL {
            let data = records(&ts, format);
            for chunk in [1, 7, 188, 1000, data.len()] {
                let mut f = Framer::new();
                assert_eq!(run_all(&mut f, &data, chunk), ts, "{format:?} chunk {chunk}");
                let s = f.stats();
                assert_eq!(s.format, Some(format));
                assert_eq!((s.packets, s.bytes_skipped, s.sync_losses), (40, 0, 0), "{format:?} chunk {chunk}");
                assert_eq!(s.bytes, data.len() as u64);
            }
        }
    }

    #[test]
    fn garbage_and_damage_are_skipped_and_counted() {
        let ts = packets(30);
        let mut data = vec![0x47, 0x00, 0x12, 0x47, 0x47]; // junk with sync-like bytes
        data.extend_from_slice(&ts[..10 * 188]);
        data.extend_from_slice(&[0xAB; 50]); // a damaged stretch
        data.extend_from_slice(&ts[10 * 188..]);
        data.extend_from_slice(&ts[..100]); // truncated last packet
        let mut f = Framer::new();
        let out = run_all(&mut f, &data, 333);
        assert_eq!(out, ts);
        let s = f.stats();
        assert_eq!(s.sync_losses, 1);
        assert_eq!(s.bytes_skipped, 5 + 50 + 100);
        assert_eq!(s.packets, 30);
    }

    #[test]
    fn a_stray_or_truncated_packet_does_not_swallow_the_next_one() {
        let ts = packets(20);
        // A truncated packet (sync byte and 100 bytes) between two whole ones.
        let mut data = ts[..5 * 188].to_vec();
        data.extend_from_slice(&ts[5 * 188..5 * 188 + 100]);
        data.extend_from_slice(&ts[5 * 188..]);
        let mut f = Framer::new();
        assert_eq!(run_all(&mut f, &data, 4096), ts);
        assert_eq!((f.stats().bytes_skipped, f.stats().sync_losses), (100, 1));
        // A whole packet followed by junk is kept.
        let mut data = ts[..5 * 188].to_vec();
        data.extend_from_slice(&[0x47, 0x47, 0x00]);
        data.extend_from_slice(&ts[5 * 188..]);
        let mut f = Framer::new();
        assert_eq!(run_all(&mut f, &data, 64), ts);
        assert_eq!((f.stats().bytes_skipped, f.stats().sync_losses), (3, 1));
    }

    #[test]
    fn short_streams_and_pure_garbage() {
        // A single packet is accepted at the end of the stream.
        let mut f = Framer::new();
        assert_eq!(run_all(&mut f, &packets(1), 1000).len(), 188);
        // No sync byte at all.
        let mut f = Framer::new();
        assert!(run_all(&mut f, &[0u8; 5000], 64).is_empty());
        assert_eq!(f.stats().bytes_skipped, 5000);
        assert_eq!(f.stats().format, None);
    }

    #[test]
    fn reset_forgets_alignment_but_keeps_counters() {
        let mut f = Framer::new();
        let data = records(&packets(10), PacketFormat::M2ts);
        run_all(&mut f, &data, 500);
        f.reset();
        let out = run_all(&mut f, &records(&packets(10), PacketFormat::Ts), 500);
        assert_eq!(out.len(), 10 * 188);
        assert_eq!(f.stats().packets, 20);
        assert_eq!(f.stats().format, Some(PacketFormat::Ts));
    }

    #[test]
    fn memory_stays_bounded() {
        let ts = packets(50);
        let mut f = Framer::new();
        for _ in 0..200 {
            let _ = f.push(&ts);
        }
        assert!(f.buf.len() <= 2 * ts.len(), "buffer grew to {}", f.buf.len());
    }
}
