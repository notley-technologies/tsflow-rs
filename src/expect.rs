//! Assertions on a [`Summary`], for scripts and CI.
//!
//! `tsflow recv` and `tsflow analyze` take `--expect NAME[=VALUE]`, any
//! number of times, and exit with status 1 when one is not met:
//!
//! | Expectation | Passes when |
//! |---|---|
//! | `no-cc-errors` | no continuity errors on any PID |
//! | `max-cc-errors=N` | at most N continuity errors |
//! | `min-packets=N` | at least N TS packets arrived |
//! | `klv-streams=N` | exactly N KLV streams were seen |
//! | `klv-marking=MARKING` | KLV arrived and every ST 0601 packet carries this ST 0102 marking, e.g. `US//UNCLASSIFIED` |
//! | `min-klv-rate=HZ` | every KLV stream (and at least one) delivered at least HZ ST 0601 packets per second |
//! | `klv-checksums` | KLV arrived and every ST 0601 checksum matched |
//! | `max-pcr-interval=MS` | every PCR PID's longest interval is at most MS (ISO/IEC 13818-1 allows 100) |
//! | `max-pcr-jitter=MS` | every PCR PID's arrival jitter is at most MS peak to peak (live inputs) |
//! | `max-psi-interval=MS` | the PAT and every PMT repeated at least every MS (DVB practice: 100) |
//! | `no-rtp-loss` | no RTP sequence gaps (passes for non-RTP input) |
//!
//! ```
//! use tsflow::expect::Expectation;
//!
//! let e: Expectation = "klv-marking=US//UNCLASSIFIED".parse()?;
//! assert_eq!(e.to_string(), "klv-marking=US//UNCLASSIFIED");
//! assert!("min-klv-rate=fast".parse::<Expectation>().is_err());
//! # Ok::<(), tsflow::Error>(())
//! ```

use crate::json::Json;
use crate::monitor::Summary;
use crate::Error;
use std::fmt;
use std::str::FromStr;

/// One assertion. See the [module documentation](self).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Expectation {
    /// `no-cc-errors`
    NoCcErrors,
    /// `max-cc-errors=N`
    MaxCcErrors(u64),
    /// `min-packets=N`
    MinPackets(u64),
    /// `klv-streams=N`
    KlvStreams(usize),
    /// `klv-marking=MARKING`
    KlvMarking(String),
    /// `min-klv-rate=HZ`
    MinKlvRate(f64),
    /// `klv-checksums`
    KlvChecksums,
    /// `max-pcr-interval=MS`
    MaxPcrInterval(f64),
    /// `max-pcr-jitter=MS`
    MaxPcrJitter(f64),
    /// `max-psi-interval=MS`
    MaxPsiInterval(f64),
    /// `no-rtp-loss`
    NoRtpLoss,
}

const NAMES: &str = "no-cc-errors, max-cc-errors=N, min-packets=N, klv-streams=N, klv-marking=MARKING, \
                     min-klv-rate=HZ, klv-checksums, max-pcr-interval=MS, max-pcr-jitter=MS, max-psi-interval=MS, \
                     no-rtp-loss";

impl FromStr for Expectation {
    type Err = Error;

    fn from_str(s: &str) -> Result<Expectation, Error> {
        let (name, value) = match s.split_once('=') {
            Some((n, v)) => (n.trim(), Some(v.trim())),
            None => (s.trim(), None),
        };
        let bad = |why: &str| Error::config(format!("--expect {s}: {why}"));
        let need = || value.filter(|v| !v.is_empty()).ok_or_else(|| bad("needs a value (NAME=VALUE)"));
        let int = || need()?.parse::<u64>().map_err(|_| bad("the value must be a whole number"));
        let num = || {
            need()?
                .parse::<f64>()
                .ok()
                .filter(|v| v.is_finite() && *v >= 0.0)
                .ok_or_else(|| bad("the value must be a number"))
        };
        let flag = |e: Expectation| match value {
            None => Ok(e),
            Some(_) => Err(bad("takes no value")),
        };
        match name {
            "no-cc-errors" => flag(Expectation::NoCcErrors),
            "max-cc-errors" => Ok(Expectation::MaxCcErrors(int()?)),
            "min-packets" => Ok(Expectation::MinPackets(int()?)),
            "klv-streams" => Ok(Expectation::KlvStreams(int()? as usize)),
            "klv-marking" => Ok(Expectation::KlvMarking(need()?.to_string())),
            "min-klv-rate" => Ok(Expectation::MinKlvRate(num()?)),
            "klv-checksums" => flag(Expectation::KlvChecksums),
            "max-pcr-interval" => Ok(Expectation::MaxPcrInterval(num()?)),
            "max-pcr-jitter" => Ok(Expectation::MaxPcrJitter(num()?)),
            "max-psi-interval" => Ok(Expectation::MaxPsiInterval(num()?)),
            "no-rtp-loss" => flag(Expectation::NoRtpLoss),
            _ => Err(bad(&format!("unknown expectation (known: {NAMES})"))),
        }
    }
}

impl fmt::Display for Expectation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expectation::NoCcErrors => f.write_str("no-cc-errors"),
            Expectation::MaxCcErrors(n) => write!(f, "max-cc-errors={n}"),
            Expectation::MinPackets(n) => write!(f, "min-packets={n}"),
            Expectation::KlvStreams(n) => write!(f, "klv-streams={n}"),
            Expectation::KlvMarking(m) => write!(f, "klv-marking={m}"),
            Expectation::MinKlvRate(r) => write!(f, "min-klv-rate={r}"),
            Expectation::KlvChecksums => f.write_str("klv-checksums"),
            Expectation::MaxPcrInterval(ms) => write!(f, "max-pcr-interval={ms}"),
            Expectation::MaxPcrJitter(ms) => write!(f, "max-pcr-jitter={ms}"),
            Expectation::MaxPsiInterval(ms) => write!(f, "max-psi-interval={ms}"),
            Expectation::NoRtpLoss => f.write_str("no-rtp-loss"),
        }
    }
}

/// The outcome of one expectation.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Check {
    /// The expectation.
    pub expectation: Expectation,
    /// Whether it was met.
    pub passed: bool,
    /// What was measured.
    pub detail: String,
}

impl fmt::Display for Check {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}: {}", if self.passed { "PASS" } else { "FAIL" }, self.expectation, self.detail)
    }
}

impl Check {
    pub(crate) fn json(&self) -> Json {
        Json::object()
            .with("expectation", self.expectation.to_string())
            .with("passed", self.passed)
            .with("detail", self.detail.as_str())
    }
}

/// A whole run as one JSON object: `{"stopped": .., "passed": ..,
/// "checks": [..], "summary": {..}}`. `stopped` says why the run ended (a
/// live receive), `None` for a file analysis.
pub fn report_json(summary: &Summary, checks: &[Check], stopped: Option<&str>) -> String {
    Json::object()
        .with("tsflow", crate::VERSION)
        .with("stopped", stopped)
        .with("passed", checks.iter().all(|c| c.passed))
        .with("checks", Json::Array(checks.iter().map(Check::json).collect()))
        .with("summary", summary.json())
        .to_string()
}

impl Expectation {
    /// Check the expectation against a summary.
    pub fn check(&self, s: &Summary) -> Check {
        let (passed, detail) = self.evaluate(s);
        Check { expectation: self.clone(), passed, detail }
    }

    fn evaluate(&self, s: &Summary) -> (bool, String) {
        let klv_packets = s.klv_packets();
        match self {
            Expectation::NoCcErrors => (s.cc_errors == 0, format!("{} CC errors", s.cc_errors)),
            Expectation::MaxCcErrors(n) => (s.cc_errors <= *n, format!("{} CC errors", s.cc_errors)),
            Expectation::MinPackets(n) => (s.packets >= *n, format!("{} packets", s.packets)),
            Expectation::KlvStreams(n) => (s.klv.len() == *n, format!("{} KLV streams", s.klv.len())),
            Expectation::KlvMarking(want) => {
                let mut seen: Vec<String> = Vec::new();
                let mut ok = klv_packets > 0;
                for k in &s.klv {
                    let mut stream: Vec<String> = Vec::new();
                    for (m, n) in &k.markings {
                        ok &= m == want;
                        stream.push(format!("{m} x{n}"));
                    }
                    if k.unmarked > 0 {
                        ok = false;
                        stream.push(format!("no security set x{}", k.unmarked));
                    }
                    if k.other_markings > 0 {
                        ok = false;
                        stream.push(format!("other markings x{}", k.other_markings));
                    }
                    if !stream.is_empty() {
                        seen.push(format!("0x{:04X} {}", k.pid, stream.join(", ")));
                    }
                }
                let detail = if seen.is_empty() { "no ST 0601 packets".into() } else { seen.join("; ") };
                (ok, detail)
            }
            Expectation::MinKlvRate(min) => {
                if s.klv.is_empty() {
                    return (false, "no KLV stream".into());
                }
                let mut ok = true;
                let rates: Vec<String> = s
                    .klv
                    .iter()
                    .map(|k| {
                        let rate = k.rate_hz.unwrap_or(0.0);
                        ok &= rate >= *min;
                        format!("0x{:04X} {rate:.2}/s", k.pid)
                    })
                    .collect();
                (ok, rates.join(", "))
            }
            Expectation::KlvChecksums => {
                let bad: u64 = s.klv.iter().map(|k| k.checksum_mismatch + k.checksum_missing).sum();
                (klv_packets > 0 && bad == 0, format!("{klv_packets} ST 0601 packets, {bad} bad or missing checksums"))
            }
            Expectation::MaxPcrInterval(max) => {
                let worst = s.pcr.iter().filter_map(|p| p.interval_ms.map(|i| i.2)).fold(None, max_of);
                match worst {
                    None => (false, "fewer than two PCRs on every PID".into()),
                    Some(w) => (w <= *max, format!("longest PCR interval {w:.1} ms")),
                }
            }
            Expectation::MaxPcrJitter(max) => {
                let worst = s.pcr.iter().filter_map(|p| p.jitter_pp_ms).fold(None, max_of);
                match worst {
                    None => (false, "no PCR arrival times (a live input is needed)".into()),
                    Some(w) => (w <= *max, format!("PCR arrival jitter {w:.2} ms peak to peak")),
                }
            }
            Expectation::MaxPsiInterval(max) => {
                let has_pat = s.psi.iter().any(|p| p.pid == 0 && p.interval_ms.is_some());
                let has_pmt = s
                    .report
                    .programs
                    .iter()
                    .all(|prog| s.psi.iter().any(|p| p.pid == prog.pmt_pid && p.interval_ms.is_some()));
                let worst = s.psi.iter().filter_map(|p| p.interval_ms.map(|i| i.1)).fold(None, max_of);
                match worst {
                    Some(w) if has_pat && has_pmt => (w <= *max, format!("longest PAT/PMT interval {w:.1} ms")),
                    _ => (false, "PAT or a PMT was not repeated".into()),
                }
            }
            Expectation::NoRtpLoss => match s.input.and_then(|i| i.rtp) {
                Some(r) => (r.lost == 0, format!("{} RTP packets missing, {} late", r.lost, r.late)),
                None => (true, "no RTP input".into()),
            },
        }
    }
}

fn max_of(acc: Option<f64>, v: f64) -> Option<f64> {
    Some(acc.map_or(v, |a| a.max(v)))
}

/// Check every expectation; returns the checks and whether all passed.
pub fn evaluate(expectations: &[Expectation], summary: &Summary) -> (Vec<Check>, bool) {
    let checks: Vec<Check> = expectations.iter().map(|e| e.check(summary)).collect();
    let ok = checks.iter().all(|c| c.passed);
    (checks, ok)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parsing() {
        let p = |s: &str| s.parse::<Expectation>();
        assert_eq!(p("no-cc-errors").unwrap(), Expectation::NoCcErrors);
        assert_eq!(p("max-cc-errors=3").unwrap(), Expectation::MaxCcErrors(3));
        assert_eq!(p("min-klv-rate=29.97").unwrap(), Expectation::MinKlvRate(29.97));
        assert_eq!(
            p("klv-marking=US//SECRET//REL TO USA, GBR").unwrap().to_string(),
            "klv-marking=US//SECRET//REL TO USA, GBR"
        );
        for bad in ["bogus", "no-cc-errors=1", "max-cc-errors", "max-cc-errors=-1", "min-klv-rate=NaN", "klv-marking="]
        {
            assert!(p(bad).is_err(), "{bad}");
        }
        assert!(p("bogus").unwrap_err().to_string().contains("known: no-cc-errors"));
    }
}
