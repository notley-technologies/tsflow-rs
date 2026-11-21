//! tsflow: an MPEG transport stream toolkit in the spirit of TSDuck.
//!
//! The libraries it builds on never do I/O or read clocks: `mpegts`
//! demultiplexes, analyses and writes transport streams, and `misb` decodes
//! and encodes MISB KLV metadata. tsflow is where the I/O lives: files,
//! pipes, UDP and RTP sockets, the wall clock, and the tools built from
//! them.
//!
//! # Pieces
//!
//! | Module | What it does |
//! |---|---|
//! | [`endpoint`] | Parses `file.ts`, `-`, `udp://host:port`, `udp://@239.1.1.1:port?iface=...`, `rtp://...` |
//! | [`framer`] | Finds packet alignment in a byte stream: 188-byte TS, 192-byte M2TS, 204-byte TS; resynchronises |
//! | [`input`] | Reads packets from files, stdin and sockets (multicast, SSM, RTP auto-detection, sequence gap counting) |
//! | [`output`] | Writes packets to files, stdout and sockets (datagram grouping, RTP headers, TTL, interface) |
//! | [`rtp`] | RFC 3550 headers, RFC 2250 MP2T framing, sequence tracking |
//! | [`clock`] | The [`Clock`](clock::Clock) trait: the system clock, or a simulated one for tests |
//! | [`pacer`] | Maps the 27 MHz stream position onto the wall clock with a latency, re-anchoring; groups datagrams |
//! | [`looping`] | Rewrites repeated passes over a file (PCR discontinuity, continuity counters) |
//! | [`play`] | Paced real-time playback, with an optional processing [`Stage`](play::Stage) |
//! | [`inject`] | KLV injection: `.klv` and NDJSON metadata files scheduled into a stream by `misb::mux::FileMux` |
//! | [`monitor`] | Live and offline analysis: CC, PCR, PSI, bitrate, programs, KLV (ST 0601, ST 0102 marking) |
//! | [`expect`] | Assertions on a monitor summary for scripts |
//! | [`recv`] | The receive loop: monitor, forward, record |
//!
//! # Example
//!
//! Monitor a file offline:
//!
//! ```no_run
//! use std::time::Duration;
//! use tsflow::input::{Input, ReadEvent};
//! use tsflow::monitor::Monitor;
//!
//! let mut input = Input::open(&"input.ts".parse()?)?;
//! let mut monitor = Monitor::new()?;
//! while let ReadEvent::Data(chunk) = input.read(Duration::ZERO)? {
//!     monitor.push(chunk.packets, None);
//! }
//! monitor.finish();
//! monitor.set_input_stats(input.stats());
//! println!("{}", monitor.summary());
//! # Ok::<(), tsflow::Error>(())
//! ```
//!
//! # Guarantees
//!
//! No `unsafe` code. No panics on any input: damaged data is skipped and
//! counted. Memory is bounded: every buffer has a fixed limit, documented
//! where it is kept. The packet path does not allocate per packet. Errors
//! say what was being done and with which endpoint.
//!
//! # Features
//!
//! - `cli` (default): the `tsflow` binary and its dependencies (`clap`,
//!   `ctrlc`). Library users can disable default features.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod clock;
pub mod endpoint;
mod error;
pub mod expect;
pub mod framer;
pub mod inject;
pub mod input;
mod json;
pub mod looping;
pub mod monitor;
mod net;
pub mod output;
pub mod pacer;
pub mod play;
pub mod recv;
pub mod rtp;

pub use error::{Error, Result};
/// Bytes in a transport stream packet (ISO/IEC 13818-1 2.4.3.2).
pub use mpegts::PACKET_SIZE;

/// The version of this crate.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
