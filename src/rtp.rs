//! RTP framing of transport streams (RFC 3550, RFC 2250).
//!
//! RFC 2250 section 2 carries an MPEG-2 transport stream as a whole number
//! of 188-byte packets after a 12-byte RTP header, payload type 33 (MP2T,
//! RFC 3551 Table 5), with a 90 kHz timestamp. [`RtpWriter`] produces those
//! headers; [`parse`] reads any RTP version 2 header, including CSRC lists,
//! a header extension and padding; [`SequenceTracker`] counts lost,
//! late and restarted packets from the sequence numbers.

use std::collections::hash_map::RandomState;
use std::fmt;
use std::hash::{BuildHasher, Hasher};
use std::ops::Range;

/// Length of the fixed RTP header (RFC 3550 5.1).
pub const HEADER_LEN: usize = 12;
/// Static payload type of an MPEG-2 transport stream (RFC 3551 Table 5).
pub const PAYLOAD_TYPE_MP2T: u8 = 33;
/// RTP clock rate of MP2T: 90 kHz (RFC 2250 section 2).
pub const CLOCK_RATE: u32 = 90_000;

/// The fields of an RTP header (RFC 3550 5.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RtpHeader {
    /// Padding bit: the payload ends with padding.
    pub padding: bool,
    /// Extension bit: a header extension follows the CSRC list.
    pub extension: bool,
    /// Number of CSRC identifiers.
    pub csrc_count: u8,
    /// Marker bit.
    pub marker: bool,
    /// Payload type (7 bits).
    pub payload_type: u8,
    /// Sequence number.
    pub sequence: u16,
    /// Timestamp (90 kHz for MP2T).
    pub timestamp: u32,
    /// Synchronisation source identifier.
    pub ssrc: u32,
}

impl RtpHeader {
    /// A plain header: no padding, extension, CSRC or marker.
    pub fn new(payload_type: u8, sequence: u16, timestamp: u32, ssrc: u32) -> RtpHeader {
        RtpHeader {
            padding: false,
            extension: false,
            csrc_count: 0,
            marker: false,
            payload_type: payload_type & 0x7F,
            sequence,
            timestamp,
            ssrc,
        }
    }

    /// The fixed 12-byte header. The CSRC count is written as given, but no
    /// CSRC list or extension is: write those after it yourself.
    pub fn to_bytes(&self) -> [u8; HEADER_LEN] {
        let mut b = [0u8; HEADER_LEN];
        b[0] = 0x80 | (u8::from(self.padding) << 5) | (u8::from(self.extension) << 4) | (self.csrc_count & 0x0F);
        b[1] = (u8::from(self.marker) << 7) | (self.payload_type & 0x7F);
        b[2..4].copy_from_slice(&self.sequence.to_be_bytes());
        b[4..8].copy_from_slice(&self.timestamp.to_be_bytes());
        b[8..12].copy_from_slice(&self.ssrc.to_be_bytes());
        b
    }
}

/// Why a datagram is not a valid RTP packet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum RtpError {
    /// Shorter than the fixed header.
    TooShort,
    /// The version field is not 2.
    Version(u8),
    /// The CSRC list or header extension runs past the end.
    Truncated,
    /// The padding count is zero or longer than the payload.
    Padding,
}

impl fmt::Display for RtpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RtpError::TooShort => f.write_str("shorter than an RTP header"),
            RtpError::Version(v) => write!(f, "RTP version {v}, expected 2"),
            RtpError::Truncated => f.write_str("CSRC list or header extension runs past the end"),
            RtpError::Padding => f.write_str("invalid padding length"),
        }
    }
}

impl std::error::Error for RtpError {}

/// Parse the RTP header of `datagram`. Returns the header and the range of
/// the payload, with the CSRC list, header extension (RFC 3550 5.3.1) and
/// padding (5.1) removed.
///
/// ```
/// use tsflow::rtp::{self, RtpHeader};
///
/// let mut d = RtpHeader::new(33, 7, 9000, 42).to_bytes().to_vec();
/// d.extend_from_slice(&[0x47; 188]);
/// let (h, payload) = rtp::parse(&d).unwrap();
/// assert_eq!((h.sequence, h.timestamp, h.ssrc), (7, 9000, 42));
/// assert_eq!(payload, 12..200);
/// ```
pub fn parse(datagram: &[u8]) -> Result<(RtpHeader, Range<usize>), RtpError> {
    if datagram.len() < HEADER_LEN {
        return Err(RtpError::TooShort);
    }
    let b0 = datagram[0];
    if b0 >> 6 != 2 {
        return Err(RtpError::Version(b0 >> 6));
    }
    let header = RtpHeader {
        padding: b0 & 0x20 != 0,
        extension: b0 & 0x10 != 0,
        csrc_count: b0 & 0x0F,
        marker: datagram[1] & 0x80 != 0,
        payload_type: datagram[1] & 0x7F,
        sequence: u16::from_be_bytes([datagram[2], datagram[3]]),
        timestamp: u32::from_be_bytes([datagram[4], datagram[5], datagram[6], datagram[7]]),
        ssrc: u32::from_be_bytes([datagram[8], datagram[9], datagram[10], datagram[11]]),
    };
    let mut start = HEADER_LEN + 4 * usize::from(header.csrc_count);
    if header.extension {
        // 16-bit profile field, 16-bit length in 32-bit words (5.3.1).
        let ext = datagram.get(start..start + 4).ok_or(RtpError::Truncated)?;
        start += 4 + 4 * usize::from(u16::from_be_bytes([ext[2], ext[3]]));
    }
    if start > datagram.len() {
        return Err(RtpError::Truncated);
    }
    let mut end = datagram.len();
    if header.padding {
        let pad = usize::from(datagram[end - 1]);
        if pad == 0 || pad > end - start {
            return Err(RtpError::Padding);
        }
        end -= pad;
    }
    Ok((header, start..end))
}

/// Writes the RTP headers of an outgoing stream: one sequence number per
/// datagram, and a 90 kHz timestamp derived from the stream position of
/// the datagram's first packet (RFC 2250 section 2: "the timestamp is a 32
/// bit 90K Hz timestamp representing the target transmission time for the
/// first byte of the packet payload").
#[derive(Debug, Clone)]
pub struct RtpWriter {
    payload_type: u8,
    ssrc: u32,
    sequence: u16,
    timestamp_offset: u32,
}

impl RtpWriter {
    /// A writer with random SSRC, initial sequence number and timestamp
    /// offset, as RFC 3550 5.1 recommends, unless `ssrc` is given.
    pub fn new(payload_type: u8, ssrc: Option<u32>) -> RtpWriter {
        RtpWriter::with_state(payload_type, ssrc.unwrap_or_else(random_u32), random_u32() as u16, random_u32())
    }

    /// A writer with chosen initial values (for tests and reproducible
    /// output).
    pub fn with_state(payload_type: u8, ssrc: u32, sequence: u16, timestamp_offset: u32) -> RtpWriter {
        RtpWriter { payload_type: payload_type & 0x7F, ssrc, sequence, timestamp_offset }
    }

    /// The SSRC written.
    pub fn ssrc(&self) -> u32 {
        self.ssrc
    }

    /// The header of the next datagram, whose first packet is at `position`
    /// (27 MHz ticks): timestamp = offset + position / 300, modulo 2^32.
    pub fn next_header(&mut self, position: u64) -> [u8; HEADER_LEN] {
        let timestamp = self.timestamp_offset.wrapping_add((position / 300) as u32);
        let h = RtpHeader::new(self.payload_type, self.sequence, timestamp, self.ssrc);
        self.sequence = self.sequence.wrapping_add(1);
        h.to_bytes()
    }
}

/// A random 32-bit value from the standard library's per-process hash
/// keys, enough for RTP identifiers (they need to be unpredictable across
/// sessions, not cryptographically strong).
fn random_u32() -> u32 {
    let mut h = RandomState::new().build_hasher();
    h.write_u64(std::process::id().into());
    if let Ok(d) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        h.write_u128(d.as_nanos());
    }
    h.finish() as u32
}

/// What one packet's sequence number says, relative to the ones before.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SequenceEvent {
    /// The first packet, or the first of a new SSRC.
    First,
    /// The expected next number.
    InOrder,
    /// This many packets are missing before this one.
    Gap(u16),
    /// Older than expected: a late or duplicated packet.
    Late,
    /// A jump too large to be loss (the sender restarted).
    Restart,
}

/// Counters kept by a [`SequenceTracker`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct SequenceStats {
    /// RTP packets seen.
    pub received: u64,
    /// Packets missing in the sequence (sum of the gaps).
    pub lost: u64,
    /// Packets that arrived after a later one (late or duplicated). A late
    /// packet was also counted in `lost` when its gap was seen.
    pub late: u64,
    /// Sequence restarts: a new SSRC, or a jump of more than
    /// [`MAX_DROPOUT`] packets.
    pub restarts: u64,
}

/// Largest forward jump counted as loss (RFC 3550 A.1 uses 3000).
pub const MAX_DROPOUT: u16 = 3000;
/// Largest backward step counted as a late packet (RFC 3550 A.1 uses 100).
pub const MAX_MISORDER: u16 = 100;

/// Follows the sequence numbers of one RTP stream (RFC 3550 A.1,
/// simplified: no probation, a restart is accepted at once).
///
/// ```
/// use tsflow::rtp::{RtpHeader, SequenceEvent, SequenceTracker};
///
/// let mut t = SequenceTracker::new();
/// let h = |seq| RtpHeader::new(33, seq, 0, 1);
/// assert_eq!(t.push(&h(65534)), SequenceEvent::First);
/// assert_eq!(t.push(&h(65535)), SequenceEvent::InOrder);
/// assert_eq!(t.push(&h(2)), SequenceEvent::Gap(2)); // 0 and 1 missing, across the wrap
/// assert_eq!(t.stats().lost, 2);
/// ```
#[derive(Debug, Clone, Default)]
pub struct SequenceTracker {
    next: Option<(u32, u16)>,
    stats: SequenceStats,
}

impl SequenceTracker {
    /// A tracker that has seen nothing.
    pub fn new() -> SequenceTracker {
        SequenceTracker::default()
    }

    /// Account for one packet.
    pub fn push(&mut self, header: &RtpHeader) -> SequenceEvent {
        self.stats.received += 1;
        let seq = header.sequence;
        let event = match self.next {
            None => SequenceEvent::First,
            Some((ssrc, _)) if ssrc != header.ssrc => {
                self.stats.restarts += 1;
                SequenceEvent::First
            }
            Some((_, expected)) => {
                let delta = seq.wrapping_sub(expected);
                if delta == 0 {
                    SequenceEvent::InOrder
                } else if delta < MAX_DROPOUT {
                    self.stats.lost += u64::from(delta);
                    SequenceEvent::Gap(delta)
                } else if delta > u16::MAX - MAX_MISORDER {
                    self.stats.late += 1;
                    // The expected number stays where it was.
                    return SequenceEvent::Late;
                } else {
                    self.stats.restarts += 1;
                    SequenceEvent::Restart
                }
            }
        };
        self.next = Some((header.ssrc, seq.wrapping_add(1)));
        event
    }

    /// The counters so far.
    pub fn stats(&self) -> SequenceStats {
        self.stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_round_trip() {
        let h = RtpHeader { marker: true, ..RtpHeader::new(PAYLOAD_TYPE_MP2T, 0xBEEF, 0x0102_0304, 0xCAFE_F00D) };
        let b = h.to_bytes();
        assert_eq!(b, [0x80, 0xA1, 0xBE, 0xEF, 1, 2, 3, 4, 0xCA, 0xFE, 0xF0, 0x0D]);
        let (p, range) = parse(&b).unwrap();
        assert_eq!(p, h);
        assert_eq!(range, 12..12);
    }

    #[test]
    fn csrc_extension_and_padding_are_skipped() {
        let mut d = RtpHeader { csrc_count: 2, extension: true, padding: true, ..RtpHeader::new(33, 1, 2, 3) }
            .to_bytes()
            .to_vec();
        d.extend_from_slice(&[0; 8]); // two CSRCs
        d.extend_from_slice(&[0xBE, 0xDE, 0x00, 0x01, 9, 9, 9, 9]); // one-word extension
        d.extend_from_slice(&[0x47; 188]);
        d.extend_from_slice(&[0, 0, 3]); // three bytes of padding
        let (_, range) = parse(&d).unwrap();
        assert_eq!(range, 28..216);
    }

    #[test]
    fn malformed_headers_are_rejected() {
        assert_eq!(parse(&[0x80; 11]), Err(RtpError::TooShort));
        assert_eq!(parse(&[0x47; 188]).unwrap_err(), RtpError::Version(1));
        let mut d = RtpHeader { csrc_count: 15, ..RtpHeader::new(33, 0, 0, 0) }.to_bytes().to_vec();
        assert_eq!(parse(&d), Err(RtpError::Truncated));
        d[0] = 0x90; // extension without its header
        assert_eq!(parse(&d), Err(RtpError::Truncated));
        let mut d = RtpHeader { padding: true, ..RtpHeader::new(33, 0, 0, 0) }.to_bytes().to_vec();
        d.push(0);
        assert_eq!(parse(&d), Err(RtpError::Padding));
        *d.last_mut().unwrap() = 2;
        assert_eq!(parse(&d), Err(RtpError::Padding));
    }

    #[test]
    fn writer_derives_timestamps_from_position() {
        let mut w = RtpWriter::with_state(33, 7, 65535, u32::MAX - 10);
        let a = parse(&w.next_header(0)).unwrap().0;
        let b = parse(&w.next_header(27_000_000)).unwrap().0;
        assert_eq!((a.sequence, b.sequence), (65535, 0));
        assert_eq!(a.timestamp, u32::MAX - 10);
        assert_eq!(b.timestamp, (u32::MAX - 10).wrapping_add(90_000));
        assert_eq!((a.ssrc, a.payload_type), (7, 33));
        // Random identifiers differ between writers.
        let ids: std::collections::HashSet<u32> = (0..8).map(|_| RtpWriter::new(33, None).ssrc()).collect();
        assert!(ids.len() > 1);
    }

    #[test]
    fn sequence_tracking() {
        let mut t = SequenceTracker::new();
        let h = |seq, ssrc| RtpHeader::new(33, seq, 0, ssrc);
        assert_eq!(t.push(&h(10, 1)), SequenceEvent::First);
        assert_eq!(t.push(&h(11, 1)), SequenceEvent::InOrder);
        assert_eq!(t.push(&h(15, 1)), SequenceEvent::Gap(3));
        assert_eq!(t.push(&h(13, 1)), SequenceEvent::Late);
        assert_eq!(t.push(&h(15, 1)), SequenceEvent::Late, "duplicate");
        assert_eq!(t.push(&h(16, 1)), SequenceEvent::InOrder);
        assert_eq!(t.push(&h(30_000, 1)), SequenceEvent::Restart);
        assert_eq!(t.push(&h(30_001, 1)), SequenceEvent::InOrder);
        assert_eq!(t.push(&h(5, 2)), SequenceEvent::First, "new SSRC");
        assert_eq!(t.push(&h(6, 2)), SequenceEvent::InOrder);
        let s = t.stats();
        assert_eq!((s.received, s.lost, s.late, s.restarts), (10, 3, 2, 2));
    }
}
