//! KLV injection: a metadata file put into a transport stream.
//!
//! The libraries do the stream work: `misb::mux::FileMux` schedules the
//! metadata and the `mpegts` injector places it (MISB ST 1402.2 sync or
//! async carriage, every other packet unchanged). This module adds what
//! needs I/O: reading the metadata file incrementally ([`KlvFile`] for a
//! recorded `.klv` file, [`NdjsonFile`] for authoring records) and the
//! [`InjectStage`] that runs FileMux inside [`crate::play::play_with`], so
//! injection works from a file or socket to a file or socket, paced or
//! not.

use crate::play::Stage;
use crate::{Error, Result, PACKET_SIZE};
use misb::mux::{FileMux, FileMuxConfig, FileMuxStats, KlvSamples, Sample};
use misb::LeapSeconds;
use std::cell::RefCell;
use std::fmt;
use std::io::{BufRead, Read};
use std::rc::Rc;

/// Where a source records the error that ended it early: an iterator
/// cannot return one, so the reader keeps it for the caller to check.
#[derive(Clone, Default)]
pub struct SourceError(Rc<RefCell<Option<String>>>);

impl SourceError {
    /// The error, if the source stopped because of one.
    #[must_use]
    pub fn get(&self) -> Option<String> {
        self.0.borrow().clone()
    }

    fn set(&self, message: String) {
        self.0.borrow_mut().get_or_insert(message);
    }
}

impl fmt::Debug for SourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("SourceError").field(&self.get()).finish()
    }
}

/// Size of each read from a metadata file.
const READ_CHUNK: usize = 64 * 1024;

/// A recorded `.klv` file (concatenated ST 0601 packets), read as the
/// samples are needed: memory stays at one read buffer whatever the file
/// size. Packets are passed on byte for byte; damaged packets (bad
/// checksum) and packets without tag 2 are skipped and counted.
pub struct KlvFile {
    reader: Box<dyn Read>,
    samples: KlvSamples,
    buf: Vec<u8>,
    done: bool,
    name: String,
    error: SourceError,
}

impl KlvFile {
    /// Read `reader`; `name` appears in error messages.
    pub fn new(reader: Box<dyn Read>, name: impl Into<String>) -> KlvFile {
        KlvFile {
            reader,
            samples: KlvSamples::new(),
            buf: vec![0; READ_CHUNK],
            done: false,
            name: name.into(),
            error: SourceError::default(),
        }
    }

    /// Where a read error is recorded.
    #[must_use]
    pub fn error(&self) -> SourceError {
        self.error.clone()
    }

    /// Packets skipped so far: (damaged, without tag 2).
    #[must_use]
    pub fn skipped(&self) -> (u64, u64) {
        let s = self.samples.stats();
        (s.damaged, s.untimed)
    }
}

impl Iterator for KlvFile {
    type Item = Sample;

    fn next(&mut self) -> Option<Sample> {
        loop {
            if let Some(s) = self.samples.next_sample() {
                return Some(s);
            }
            if self.done {
                return None;
            }
            match self.reader.read(&mut self.buf) {
                Ok(0) => {
                    self.samples.finish();
                    self.done = true;
                }
                Ok(n) => self.samples.push(&self.buf[..n]),
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => {
                    self.error.set(format!("read {}: {e}", self.name));
                    self.done = true;
                    return None;
                }
            }
        }
    }
}

/// NDJSON authoring records (one JSON object per line, the format of
/// `misb::json`), read line by line. Blank lines are skipped; a line that
/// does not parse stops the source with an error naming the line.
pub struct NdjsonFile {
    lines: std::io::Lines<Box<dyn BufRead>>,
    line: u64,
    name: String,
    error: SourceError,
}

impl NdjsonFile {
    /// Read `reader`; `name` appears in error messages.
    pub fn new(reader: Box<dyn BufRead>, name: impl Into<String>) -> NdjsonFile {
        NdjsonFile { lines: reader.lines(), line: 0, name: name.into(), error: SourceError::default() }
    }

    /// Where a read or parse error is recorded.
    #[must_use]
    pub fn error(&self) -> SourceError {
        self.error.clone()
    }
}

impl Iterator for NdjsonFile {
    type Item = Sample;

    fn next(&mut self) -> Option<Sample> {
        loop {
            self.line += 1;
            let line = match self.lines.next()? {
                Ok(l) => l,
                Err(e) => {
                    self.error.set(format!("read {}: {e}", self.name));
                    return None;
                }
            };
            if line.trim().is_empty() {
                continue;
            }
            return match misb::json::parse_record(&line, LeapSeconds::builtin()) {
                Ok(r) => Some(Sample::from(r)),
                Err(e) => {
                    self.error.set(format!("{} line {}: {e}", self.name, self.line));
                    None
                }
            };
        }
    }
}

/// Runs a [`FileMux`] as a [`Stage`] of [`crate::play::play_with`].
pub struct InjectStage<S: Iterator<Item = Sample>> {
    mux: FileMux<S>,
    out: Vec<u8>,
    source_error: SourceError,
    stats: Option<FileMuxStats>,
}

impl<S: Iterator<Item = Sample>> InjectStage<S> {
    /// A stage injecting `source` with `cfg`; `source_error` is checked
    /// at the end so a metadata file that failed half way is an error, not
    /// a short output.
    pub fn new(cfg: FileMuxConfig, source: S, source_error: SourceError) -> Result<Self> {
        let mux = FileMux::new(cfg, source).map_err(|e| Error::Inject(e.to_string()))?;
        Ok(InjectStage { mux, out: Vec::with_capacity(64 * PACKET_SIZE), source_error, stats: None })
    }

    /// The muxer: anchor, target, events.
    pub fn mux(&mut self) -> &mut FileMux<S> {
        &mut self.mux
    }

    /// The counters, final once the input ended.
    #[must_use]
    pub fn stats(&self) -> FileMuxStats {
        self.stats.clone().unwrap_or_else(|| self.mux.stats())
    }

    fn drain(&mut self, out: &mut dyn FnMut(&[u8; PACKET_SIZE])) {
        for p in self.out.chunks_exact(PACKET_SIZE) {
            if let Ok(p) = <&[u8; PACKET_SIZE]>::try_from(p) {
                out(p);
            }
        }
        self.out.clear();
    }

    fn check_source(&self) -> Result<()> {
        match self.source_error.get() {
            Some(e) => Err(Error::Inject(e)),
            None => Ok(()),
        }
    }
}

impl<S: Iterator<Item = Sample>> Stage for InjectStage<S> {
    fn packet(
        &mut self,
        packet: &[u8; PACKET_SIZE],
        clock: Option<u64>,
        out: &mut dyn FnMut(&[u8; PACKET_SIZE]),
    ) -> Result<()> {
        let r = self.mux.push_packet(packet, clock, &mut self.out);
        self.drain(out);
        r.map_err(|e| Error::Inject(e.to_string()))?;
        self.check_source()
    }

    fn finish(&mut self, out: &mut dyn FnMut(&[u8; PACKET_SIZE])) -> Result<()> {
        let r = self.mux.finish(&mut self.out);
        self.drain(out);
        self.stats = Some(r.map_err(|e| Error::Inject(e.to_string()))?);
        self.check_source()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use misb::encode::St0601Builder;
    use misb::MispTime;

    #[test]
    fn klv_file_reads_incrementally_and_skips_damage() {
        let mut bytes = Vec::new();
        for t in 0..300u64 {
            let mut p = St0601Builder::new(MispTime::from_micros(1_000_000 + t * 1000)).finish().unwrap();
            if t == 7 {
                let n = p.len();
                p[n - 1] ^= 1;
            }
            bytes.extend(p);
        }
        let file = KlvFile::new(Box::new(std::io::Cursor::new(bytes)), "test.klv");
        let error = file.error();
        let times: Vec<u64> = file.map(|s| s.time.as_micros()).collect();
        assert_eq!(times.len(), 299);
        assert!(!times.contains(&1_007_000));
        assert_eq!(error.get(), None);
    }

    #[test]
    fn ndjson_errors_name_the_line() {
        let text =
            "{\"time_us\": 5, \"items\": {\"SensorLatitude\": 1.5}}\n\n{\"time_us\": 6, \"items\": {\"Bogus\": 1}}\n";
        let file = NdjsonFile::new(Box::new(std::io::Cursor::new(text.as_bytes().to_vec())), "in.ndjson");
        let error = file.error();
        let samples: Vec<Sample> = file.collect();
        assert_eq!(samples.len(), 1);
        let e = error.get().unwrap();
        assert!(e.starts_with("in.ndjson line 3:") && e.contains("Bogus"), "{e}");
    }
}
