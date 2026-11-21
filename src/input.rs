//! Reading transport streams from files, standard input and UDP/RTP
//! sockets.
//!
//! [`Input::read`] returns runs of whole 188-byte packets, whatever the
//! source: 192-byte M2TS and 204-byte records are converted, lost
//! alignment is recovered, and an RTP header is removed. Every datagram is
//! classified on its own (RTP version 2 starts with 0x80 to 0xBF, a raw TS
//! datagram with 0x47, so the two cannot be confused), and RTP sequence
//! gaps are counted.

use crate::endpoint::{Direction, Endpoint, NetEndpoint};
use crate::framer::{Framer, FramerStats};
use crate::rtp::{self, RtpHeader, SequenceStats, SequenceTracker};
use crate::{net, Error, Result};
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::net::{SocketAddr, UdpSocket};
use std::time::Duration;

/// Bytes read from a file or pipe at a time.
pub const READ_CHUNK: usize = 256 << 10;
/// Largest datagram accepted unless `pkt_size` says otherwise.
pub const MAX_DATAGRAM: usize = 65_536;
/// Shortest read timeout passed to the system (a zero timeout means
/// "block forever" to some systems).
const MIN_TIMEOUT: Duration = Duration::from_micros(100);

/// What one [`Input::read`] produced.
#[derive(Debug)]
pub enum ReadEvent<'a> {
    /// Packets (possibly none, when a datagram held only part of one).
    Data(Chunk<'a>),
    /// Nothing arrived before the timeout (sockets only).
    Idle,
    /// The end of a file or pipe. Sockets never end.
    End,
}

/// A run of packets and, for a socket, the datagram they came in.
#[derive(Debug)]
#[non_exhaustive]
pub struct Chunk<'a> {
    /// Whole 188-byte packets, back to back. Mutable so a pass-through
    /// tool can rewrite headers in place.
    pub packets: &'a mut [u8],
    /// The datagram exactly as received (RTP header included).
    pub datagram: Option<&'a [u8]>,
    /// The datagram's RTP header, if it had one.
    pub rtp: Option<RtpHeader>,
}

/// Counters of an input.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct InputStats {
    /// Alignment and record format.
    pub framer: FramerStats,
    /// Datagrams received (sockets).
    pub datagrams: u64,
    /// Datagrams with an RTP header.
    pub rtp_datagrams: u64,
    /// Datagrams that were neither RTP nor started with a sync byte; their
    /// bytes still go through the framer, which skips what is not TS.
    pub unrecognized_datagrams: u64,
    /// RTP sequence counters, once an RTP datagram has arrived.
    pub rtp: Option<SequenceStats>,
    /// Receive buffer the system granted (sockets).
    pub recv_buffer: Option<usize>,
}

/// A source of transport stream packets.
#[derive(Debug)]
pub struct Input {
    kind: Kind,
    framer: Framer,
    stats: InputStats,
    description: String,
}

#[derive(Debug)]
enum Kind {
    File { file: File, eof: bool },
    Stdin { stdin: io::Stdin, eof: bool },
    Udp(Box<Udp>),
}

#[derive(Debug)]
struct Udp {
    socket: UdpSocket,
    local: SocketAddr,
    buf: Vec<u8>,
    sequence: Option<SequenceTracker>,
    timeout: Option<Duration>,
}

impl Input {
    /// Open an endpoint for reading. A network endpoint is bound (and
    /// joined, for a multicast group) at once.
    pub fn open(endpoint: &Endpoint) -> Result<Input> {
        endpoint.validate(Direction::Input)?;
        let description = endpoint.to_string();
        let kind = match endpoint {
            Endpoint::File(path) => {
                let file = File::open(path).map_err(|e| Error::io(format!("open {}", path.display()), e))?;
                Kind::File { file, eof: false }
            }
            Endpoint::Stdio => Kind::Stdin { stdin: io::stdin(), eof: false },
            Endpoint::Net(net) => return Input::open_net(net, description),
        };
        Ok(Input { kind, framer: Framer::new(), stats: InputStats::default(), description })
    }

    fn open_net(ep: &NetEndpoint, description: String) -> Result<Input> {
        let r = net::open_receiver(ep).map_err(|e| Error::io(format!("listen on {description}"), e))?;
        let size = ep.options.packet_size.unwrap_or(MAX_DATAGRAM);
        let udp = Udp { socket: r.socket, local: r.local, buf: vec![0; size], sequence: None, timeout: None };
        let stats = InputStats { recv_buffer: r.buffer_size, ..InputStats::default() };
        Ok(Input { kind: Kind::Udp(Box::new(udp)), framer: Framer::new(), stats, description })
    }

    /// The endpoint as given.
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Whether this is a socket (live: data arrives at the sender's pace,
    /// and [`read`](Self::read) can time out).
    pub fn is_live(&self) -> bool {
        matches!(self.kind, Kind::Udp(_))
    }

    /// Whether [`rewind`](Self::rewind) works (a regular file).
    pub fn is_seekable(&self) -> bool {
        matches!(self.kind, Kind::File { .. })
    }

    /// The local address of a socket (useful after binding port 0).
    pub fn local_addr(&self) -> Option<SocketAddr> {
        match &self.kind {
            Kind::Udp(u) => Some(u.local),
            _ => None,
        }
    }

    /// The counters so far.
    pub fn stats(&self) -> InputStats {
        let mut s = self.stats;
        s.framer = *self.framer.stats();
        if let Kind::Udp(u) = &self.kind {
            s.rtp = u.sequence.as_ref().map(SequenceTracker::stats);
        }
        s
    }

    /// Start again at the beginning of a file.
    pub fn rewind(&mut self) -> Result<()> {
        match &mut self.kind {
            Kind::File { file, eof } => {
                file.seek(SeekFrom::Start(0)).map_err(|e| Error::io(format!("rewind {}", self.description), e))?;
                *eof = false;
                self.framer.reset();
                Ok(())
            }
            _ => Err(Error::config(format!("{} cannot be rewound (only a file can loop)", self.description))),
        }
    }

    /// The next packets. A file or pipe blocks until data is read and
    /// returns [`ReadEvent::End`] at its end; a socket waits at most
    /// `timeout` and then returns [`ReadEvent::Idle`].
    pub fn read(&mut self, timeout: Duration) -> Result<ReadEvent<'_>> {
        match &mut self.kind {
            Kind::File { file, eof } => read_stream(file, eof, &mut self.framer, &self.description),
            Kind::Stdin { stdin, eof } => read_stream(&mut stdin.lock(), eof, &mut self.framer, &self.description),
            Kind::Udp(udp) => read_datagram(udp, timeout, &mut self.framer, &mut self.stats, &self.description),
        }
    }
}

fn read_stream<'a>(
    source: &mut impl Read,
    eof: &mut bool,
    framer: &'a mut Framer,
    description: &str,
) -> Result<ReadEvent<'a>> {
    loop {
        if let Some(run) = framer.next_run(*eof) {
            return Ok(ReadEvent::Data(Chunk { packets: framer.slice(run), datagram: None, rtp: None }));
        }
        if *eof {
            return Ok(ReadEvent::End);
        }
        let spare = framer.spare(READ_CHUNK);
        let got = loop {
            match source.read(&mut spare[..READ_CHUNK]) {
                Ok(n) => break n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(Error::io(format!("read {description}"), e)),
            }
        };
        if got == 0 {
            *eof = true;
        } else {
            framer.commit(got);
        }
    }
}

fn read_datagram<'a>(
    udp: &'a mut Udp,
    timeout: Duration,
    framer: &'a mut Framer,
    stats: &mut InputStats,
    description: &str,
) -> Result<ReadEvent<'a>> {
    let timeout = timeout.max(MIN_TIMEOUT);
    if udp.timeout != Some(timeout) {
        udp.socket.set_read_timeout(Some(timeout)).map_err(|e| Error::io(format!("configure {description}"), e))?;
        udp.timeout = Some(timeout);
    }
    let n = match udp.socket.recv(&mut udp.buf) {
        Ok(n) => n,
        Err(e) if net::is_retry(&e) || net::is_refused(&e) => return Ok(ReadEvent::Idle),
        Err(e) => return Err(Error::io(format!("receive on {description}"), e)),
    };
    stats.datagrams += 1;
    let datagram = &udp.buf[..n];
    let mut header = None;
    let payload = match datagram.first() {
        Some(0x47) => datagram,
        _ => match rtp::parse(datagram) {
            Ok((h, range)) if datagram.get(range.start) == Some(&0x47) || range.is_empty() => {
                stats.rtp_datagrams += 1;
                udp.sequence.get_or_insert_with(SequenceTracker::new).push(&h);
                header = Some(h);
                &datagram[range]
            }
            _ => {
                stats.unrecognized_datagrams += 1;
                datagram
            }
        },
    };
    let packets = framer.push(payload);
    Ok(ReadEvent::Data(Chunk { packets, datagram: Some(datagram), rtp: header }))
}
