//! KLV metadata streams: decoding with `misb` and what is worth reporting.

use crate::json::Json;
use misb::{st0102, st0601, Checksum, LeapSeconds, MispTime, Value};
use std::collections::BTreeMap;

/// Distinct security markings remembered per stream; more are counted
/// together, so hostile input cannot grow the map.
const MAX_MARKINGS: usize = 16;

/// The security marking of an ST 0601 packet, from its ST 0102 Security
/// Local Set (tag 48): the classifying country and the classification, as
/// `US//UNCLASSIFIED`. `None` when the packet has no security set or the
/// set has no classification.
///
/// ```
/// use misb::encode::{Classification, Input, Security, St0601Builder};
/// use misb::{st0601::Tag, MispTime};
///
/// let security = Security::new(Classification::Unclassified, 0x0D, "//US", 0x0D, "US");
/// let mut b = St0601Builder::new(MispTime::from_micros(1_700_000_000_000_000));
/// b.set(Tag::SecurityLocalSet, Input::Set(security.encode()?))?;
/// let packet = misb::decode(&b.finish()?).unwrap();
/// assert_eq!(tsflow::monitor::security_marking(&packet).as_deref(), Some("US//UNCLASSIFIED"));
/// # Ok::<(), misb::encode::EncodeError>(())
/// ```
pub fn security_marking(packet: &misb::Packet) -> Option<String> {
    let set = packet.get(st0601::Tag::SecurityLocalSet)?.as_set()?;
    let class = match set.get(st0102::Tag::SecurityClassification)? {
        Value::Enum { label: Some(label), .. } => label.trim_end_matches('/').to_string(),
        Value::Enum { code, .. } => format!("CLASSIFICATION 0x{code:02X}"),
        _ => return None,
    };
    let country = set.get(st0102::Tag::ClassifyingCountry).and_then(Value::as_str).map(|c| c.trim_start_matches('/'));
    Some(match country {
        Some(c) if !c.is_empty() => format!("{c}//{class}"),
        _ => class,
    })
}

/// A MISP time (ST 0603) as a UTC reading, RFC 3339.
pub(crate) fn misp_to_utc(us: u64) -> String {
    match MispTime::from_micros(us).to_utc(LeapSeconds::builtin()) {
        Some(utc) => utc.to_string(),
        None => format!("{} MISP", MispTime::from_micros(us)),
    }
}

/// Minimum and maximum of a measurement.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) struct Range {
    pub min: f64,
    pub max: f64,
    pub n: u64,
}

impl Range {
    pub fn add(&mut self, v: f64) {
        if self.n == 0 || v < self.min {
            self.min = v;
        }
        if self.n == 0 || v > self.max {
            self.max = v;
        }
        self.n += 1;
    }

    pub fn get(&self) -> Option<(f64, f64)> {
        (self.n > 0).then_some((self.min, self.max))
    }
}

/// One KLV elementary stream being decoded.
#[derive(Debug)]
pub(crate) struct KlvTrack {
    pub pid: u16,
    pub sync: bool,
    decoder: misb::Decoder,
    pub pes: u64,
    pub damaged_pes: u64,
    pub packets: u64,
    pub window_packets: u64,
    pub checksum_valid: u64,
    pub checksum_mismatch: u64,
    pub checksum_missing: u64,
    pub malformed: u64,
    pub markings: BTreeMap<String, u64>,
    pub other_markings: u64,
    pub unmarked: u64,
    pub last_marking: Option<String>,
    pub first_time: Option<u64>,
    pub last_time: Option<u64>,
    pub time_backwards: u64,
    pub lead_ms: Range,
    pub lead_outside: u64,
}

impl KlvTrack {
    pub fn new(pid: u16, sync: bool) -> KlvTrack {
        KlvTrack {
            pid,
            sync,
            decoder: misb::Decoder::new(),
            pes: 0,
            damaged_pes: 0,
            packets: 0,
            window_packets: 0,
            checksum_valid: 0,
            checksum_mismatch: 0,
            checksum_missing: 0,
            malformed: 0,
            markings: BTreeMap::new(),
            other_markings: 0,
            unmarked: 0,
            last_marking: None,
            first_time: None,
            last_time: None,
            time_backwards: 0,
            lead_ms: Range::default(),
            lead_outside: 0,
        }
    }

    /// One PES payload (cell headers already removed for sync KLV), with
    /// how far ahead of its PTS it arrived.
    pub fn push_pes(&mut self, payload: &[u8], damaged: bool, lead_ms: Option<f64>) {
        self.pes += 1;
        self.damaged_pes += u64::from(damaged);
        if let Some(lead) = lead_ms {
            self.lead_ms.add(lead);
            // MISB ST 1402 (ISO/IEC 13818-1 2.12.10): a synchronous
            // metadata access unit arrives before its PTS and at most one
            // second before it.
            if lead <= 0.0 || lead > 1000.0 {
                self.lead_outside += 1;
            }
        }
        self.decoder.push(payload);
        while let Some(packet) = self.decoder.next_packet() {
            self.on_packet(&packet);
        }
    }

    fn on_packet(&mut self, packet: &misb::Packet) {
        self.packets += 1;
        self.window_packets += 1;
        match packet.checksum {
            Checksum::Valid => self.checksum_valid += 1,
            Checksum::Mismatch { .. } => self.checksum_mismatch += 1,
            Checksum::Missing => self.checksum_missing += 1,
            _ => {}
        }
        if packet.error.is_some() || !packet.is_ok() {
            self.malformed += 1;
        }
        match security_marking(packet) {
            Some(m) => {
                if let Some(n) = self.markings.get_mut(&m) {
                    *n += 1;
                } else if self.markings.len() < MAX_MARKINGS {
                    self.markings.insert(m.clone(), 1);
                } else {
                    self.other_markings += 1;
                }
                if self.last_marking.as_ref() != Some(&m) {
                    self.last_marking = Some(m);
                }
            }
            None => self.unmarked += 1,
        }
        if let Some(t) = packet.get(st0601::Tag::PrecisionTimeStamp).and_then(Value::as_time) {
            if self.last_time.is_some_and(|last| t <= last) {
                self.time_backwards += 1;
            }
            self.first_time.get_or_insert(t);
            self.last_time = Some(t);
        }
    }

    pub fn summary(&self, duration_s: Option<f64>) -> KlvSummary {
        KlvSummary {
            pid: self.pid,
            sync: self.sync,
            pes: self.pes,
            damaged_pes: self.damaged_pes,
            packets: self.packets,
            rate_hz: duration_s.filter(|&d| d > 0.0).map(|d| self.packets as f64 / d),
            checksum_valid: self.checksum_valid,
            checksum_mismatch: self.checksum_mismatch,
            checksum_missing: self.checksum_missing,
            malformed: self.malformed,
            markings: self.markings.iter().map(|(m, n)| (m.clone(), *n)).collect(),
            other_markings: self.other_markings,
            unmarked: self.unmarked,
            first_time: self.first_time.map(misp_to_utc),
            last_time: self.last_time.map(misp_to_utc),
            time_backwards: self.time_backwards,
            lead_ms: self.lead_ms.get(),
            lead_outside: self.lead_outside,
            bytes_skipped: self.decoder.stats().bytes_skipped,
        }
    }
}

/// What was seen on one KLV stream.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct KlvSummary {
    /// The PID.
    pub pid: u16,
    /// Synchronous (metadata access units with PTS, stream_type 0x15) or
    /// asynchronous (SMPTE RP 217, stream_type 0x06).
    pub sync: bool,
    /// PES packets received.
    pub pes: u64,
    /// PES packets with data missing (continuity gap, truncation).
    pub damaged_pes: u64,
    /// ST 0601 packets decoded.
    pub packets: u64,
    /// ST 0601 packets per second of stream time.
    pub rate_hz: Option<f64>,
    /// Packets whose checksum (tag 1) matched.
    pub checksum_valid: u64,
    /// Packets whose checksum did not match.
    pub checksum_mismatch: u64,
    /// Packets without a checksum.
    pub checksum_missing: u64,
    /// Packets that did not decode cleanly (see `misb::Packet::is_ok`).
    pub malformed: u64,
    /// Security markings and how many packets carried each.
    pub markings: Vec<(String, u64)>,
    /// Packets with markings beyond the first 16 distinct ones.
    pub other_markings: u64,
    /// Packets without an ST 0102 security set.
    pub unmarked: u64,
    /// First Precision Time Stamp (tag 2) as UTC.
    pub first_time: Option<String>,
    /// Last Precision Time Stamp as UTC.
    pub last_time: Option<String>,
    /// Times tag 2 did not increase (expected once per loop of a file).
    pub time_backwards: u64,
    /// Synchronous KLV: how long before its PTS each access unit arrived
    /// (min, max), in milliseconds.
    pub lead_ms: Option<(f64, f64)>,
    /// Access units arriving after their PTS or more than 1 s before it.
    pub lead_outside: u64,
    /// Bytes of the PES payloads that were not ST 0601 packets.
    pub bytes_skipped: u64,
}

impl KlvSummary {
    /// Whether every decoded packet's checksum matched.
    pub fn checksums_ok(&self) -> bool {
        self.checksum_mismatch == 0 && self.checksum_missing == 0
    }

    pub(crate) fn to_json(&self) -> Json {
        let markings: Vec<Json> =
            self.markings.iter().map(|(m, n)| Json::object().with("marking", m.as_str()).with("packets", *n)).collect();
        Json::object()
            .with("pid", self.pid)
            .with("carriage", if self.sync { "sync" } else { "async" })
            .with("pes", self.pes)
            .with("damaged_pes", self.damaged_pes)
            .with("packets", self.packets)
            .with("rate_hz", self.rate_hz)
            .with(
                "checksum",
                Json::object()
                    .with("valid", self.checksum_valid)
                    .with("mismatch", self.checksum_mismatch)
                    .with("missing", self.checksum_missing),
            )
            .with("malformed", self.malformed)
            .with("markings", Json::Array(markings))
            .with("other_markings", self.other_markings)
            .with("unmarked", self.unmarked)
            .with("first_time", self.first_time.clone())
            .with("last_time", self.last_time.clone())
            .with("time_backwards", self.time_backwards)
            .with("lead_min_ms", self.lead_ms.map(|l| l.0))
            .with("lead_max_ms", self.lead_ms.map(|l| l.1))
            .with("lead_outside", self.lead_outside)
            .with("bytes_skipped", self.bytes_skipped)
    }
}
