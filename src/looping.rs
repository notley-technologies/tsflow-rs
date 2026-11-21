//! Playing a file in a loop so that a receiver sees one valid stream.
//!
//! When a file starts again, three things jump back to their values at
//! the start of the file: the program clock (PCR), the timestamps (PTS,
//! DTS) and every PID's continuity counter. [`LoopFixer`] rewrites the
//! packets of every pass after the first so that a receiver can follow:
//!
//! - **PCR.** The first packet carrying a PCR on each PID in the new pass
//!   gets discontinuity_indicator = 1 (ISO/IEC 13818-1 2.4.3.5): a system
//!   time base discontinuity. Receivers restart their clock recovery
//!   instead of seeing the clock run backwards, and treat the PTS/DTS that
//!   follow as belonging to the new time base. The packet already has an
//!   adaptation field (it carries the PCR), so only a flag bit changes.
//! - **Continuity counters.** Each PID's counter is offset so it continues
//!   from the last value sent (2.4.3.3): no continuity error is reported,
//!   and a constant offset per pass keeps the input's own counter pattern,
//!   errors and duplicates included. Marking the jump with a
//!   discontinuity_indicator instead would need an adaptation field in
//!   packets that have none, which cannot be added without repacketizing.
//!
//! What a receiver sees at the loop point: a PCR discontinuity with the
//! flag set; PTS/DTS restarting with the new time base; no CC errors; PAT
//! and PMT unchanged (same versions). The last PES of a pass may be cut
//! short if the file does not end on a PES boundary, and the first packets
//! of the next pass may continue a PES whose start the receiver never saw;
//! decoders drop both and resynchronise at the next payload_unit_start
//! (video shows artefacts until the next random access point unless the
//! file starts with one). KLV and other metadata time stamps (ST 0601 tag
//! 2) are not rewritten and go back to their values at the start of the
//! file. Pacing has no gap or burst at the loop point: the packets after
//! the last PCR of a pass and before the first PCR of the next are timed
//! at the rate of the last PCR interval (see
//! [`PcrTimeline`](mpegts::PcrTimeline)).

use crate::PACKET_SIZE;
use mpegts::NULL_PID;

const PIDS: usize = 8192;
const NO_CC: u8 = 0xFF;

/// Rewrites the packets of repeated passes over a file (see the [module
/// documentation](self)). The first pass goes through unchanged.
///
/// ```
/// use tsflow::looping::LoopFixer;
///
/// let mut packet = [0xFFu8; 188];
/// packet[..4].copy_from_slice(&[0x47, 0x01, 0x00, 0x13]); // PID 0x100, CC 3
/// let mut fixer = LoopFixer::new();
/// let mut first = packet;
/// fixer.fix(&mut first);
/// assert_eq!(first, packet); // pass 1: unchanged
/// fixer.start_pass();
/// let mut again = packet;
/// fixer.fix(&mut again);
/// assert_eq!(again[3] & 0x0F, 4); // continues from CC 3
/// ```
#[derive(Debug, Clone)]
pub struct LoopFixer {
    pass: u32,
    /// Output continuity counter of the last packet per PID.
    last_cc: Box<[u8]>,
    /// The pass each PID's offset was computed for.
    offset_pass: Box<[u32]>,
    offset: Box<[u8]>,
    /// The pass each PID's PCR discontinuity was flagged in.
    pcr_flagged: Box<[u32]>,
    pcr_discontinuities: u64,
}

impl Default for LoopFixer {
    fn default() -> Self {
        LoopFixer::new()
    }
}

impl LoopFixer {
    /// A fixer in pass 1.
    pub fn new() -> LoopFixer {
        LoopFixer {
            pass: 1,
            last_cc: vec![NO_CC; PIDS].into_boxed_slice(),
            offset_pass: vec![0; PIDS].into_boxed_slice(),
            offset: vec![0; PIDS].into_boxed_slice(),
            pcr_flagged: vec![0; PIDS].into_boxed_slice(),
            pcr_discontinuities: 0,
        }
    }

    /// The current pass, from 1.
    pub fn pass(&self) -> u32 {
        self.pass
    }

    /// The file starts again.
    pub fn start_pass(&mut self) {
        self.pass = self.pass.saturating_add(1);
    }

    /// PCR discontinuity indicators set so far.
    pub fn pcr_discontinuities(&self) -> u64 {
        self.pcr_discontinuities
    }

    /// Rewrite one packet of the current pass in place.
    pub fn fix(&mut self, p: &mut [u8; PACKET_SIZE]) {
        if p[0] != 0x47 {
            return;
        }
        let pid = (u16::from(p[1] & 0x1F) << 8) | u16::from(p[2]);
        if pid == NULL_PID {
            return;
        }
        let i = usize::from(pid);
        let afc = (p[3] >> 4) & 0x03;
        let has_payload = afc & 0x01 != 0;
        if self.pass > 1 {
            if self.offset_pass[i] != self.pass {
                self.offset_pass[i] = self.pass;
                let cc = p[3] & 0x0F;
                self.offset[i] = match self.last_cc[i] {
                    NO_CC => 0,
                    // A packet without payload repeats the counter of the
                    // one before it (2.4.3.3).
                    last => (last.wrapping_add(u8::from(has_payload)).wrapping_sub(cc)) & 0x0F,
                };
            }
            p[3] = (p[3] & 0xF0) | ((p[3].wrapping_add(self.offset[i])) & 0x0F);
            // PCR_flag in an adaptation field long enough to hold one.
            if afc & 0x02 != 0 && p[4] >= 7 && p[5] & 0x10 != 0 && self.pcr_flagged[i] != self.pass {
                self.pcr_flagged[i] = self.pass;
                if p[5] & 0x80 == 0 {
                    p[5] |= 0x80;
                    self.pcr_discontinuities += 1;
                }
            }
        }
        self.last_cc[i] = p[3] & 0x0F;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn packet(pid: u16, cc: u8, payload: bool, pcr: bool) -> [u8; PACKET_SIZE] {
        let mut p = [0xFF; PACKET_SIZE];
        p[0] = 0x47;
        p[1] = (pid >> 8) as u8;
        p[2] = pid as u8;
        let afc = if pcr { 0x20 } else { 0 } | if payload { 0x10 } else { 0 };
        p[3] = afc | cc;
        if pcr {
            p[4] = 7;
            p[5] = 0x10;
        }
        p
    }

    #[test]
    fn counters_continue_and_pcr_is_flagged_once_per_pass() {
        let mut f = LoopFixer::new();
        // Pass 1 ends with CC 9 on 0x100 and 5 on 0x101.
        for cc in 0..=9 {
            let mut p = packet(0x100, cc, true, cc == 0);
            f.fix(&mut p);
            assert_eq!(p, packet(0x100, cc, true, cc == 0));
        }
        let mut p = packet(0x101, 5, true, false);
        f.fix(&mut p);
        for pass in 2..=3 {
            f.start_pass();
            assert_eq!(f.pass(), pass);
            // The file starts again with CC 0, a PCR packet first.
            let mut p = packet(0x100, 0, true, true);
            f.fix(&mut p);
            let expect_cc = if pass == 2 { 10 } else { 4 };
            assert_eq!(p[3] & 0x0F, expect_cc);
            assert_eq!(p[5], 0x90, "discontinuity_indicator set");
            // A second PCR in the same pass is left alone.
            let mut q = packet(0x100, 1, true, true);
            f.fix(&mut q);
            assert_eq!((q[3] & 0x0F, q[5]), ((expect_cc + 1) & 0x0F, 0x10));
            for cc in 2..=9 {
                let mut r = packet(0x100, cc, true, false);
                f.fix(&mut r);
            }
            // An adaptation-only packet first on 0x101 repeats the counter.
            let mut a = packet(0x101, 0, false, false);
            f.fix(&mut a);
            let last_101 = if pass == 2 { 5 } else { 6 };
            assert_eq!(a[3] & 0x0F, last_101);
            let mut b = packet(0x101, 1, true, false);
            f.fix(&mut b);
            assert_eq!(b[3] & 0x0F, last_101 + 1, "offset stays constant within a pass");
        }
        assert_eq!(f.pcr_discontinuities(), 2);
        // Null packets are never touched.
        let mut n = packet(NULL_PID, 7, true, false);
        f.fix(&mut n);
        assert_eq!(n, packet(NULL_PID, 7, true, false));
    }
}
