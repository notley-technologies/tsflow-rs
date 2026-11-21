//! Writing transport streams to files, standard output and UDP/RTP
//! sockets.
//!
//! Every output takes whole 188-byte packets through the [`Sink`] trait.
//! A socket output sends each call as one datagram (split into several if
//! it holds more than the datagram size), prefixed with an RTP header for
//! `rtp://`; a file output writes the bytes through a large buffer.

use crate::endpoint::{
    Direction, Endpoint, NetEndpoint, Protocol, DEFAULT_PACKETS_PER_DATAGRAM, MAX_PACKETS_PER_DATAGRAM,
};
use crate::rtp::{self, RtpWriter, PAYLOAD_TYPE_MP2T};
use crate::{net, Error, Result, PACKET_SIZE};
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::net::{SocketAddr, UdpSocket};

/// Buffer of a file output.
const FILE_BUFFER: usize = 1 << 20;

/// Where packets go. Implemented by [`Output`]; tests implement it to
/// record what a pipeline sends and when.
pub trait Sink {
    /// Take `packets` (whole 188-byte packets) as one datagram. `position`
    /// is the 27 MHz stream position of the first packet, from which an
    /// RTP output derives its timestamp.
    fn send(&mut self, packets: &[u8], position: u64) -> io::Result<()>;

    /// Push buffered data out.
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Counters of an output.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct OutputStats {
    /// TS packets written or sent.
    pub packets: u64,
    /// Datagrams sent (sockets).
    pub datagrams: u64,
    /// Datagrams the system reported as refused (nobody listening at a
    /// unicast destination). Sending continues.
    pub refused: u64,
}

/// A destination for transport stream packets.
#[derive(Debug)]
pub struct Output {
    kind: Kind,
    stats: OutputStats,
    description: String,
}

#[derive(Debug)]
enum Kind {
    Writer(BufWriter<Box<dyn WriteSend>>),
    Udp(Box<UdpSender>),
}

/// `Write + Send` with `Debug`, for the boxed file writer.
trait WriteSend: Write + Send + std::fmt::Debug {}
impl<T: Write + Send + std::fmt::Debug> WriteSend for T {}

#[derive(Debug)]
struct UdpSender {
    socket: UdpSocket,
    dest: SocketAddr,
    rtp: Option<RtpWriter>,
    burst: usize,
    buf: Vec<u8>,
}

impl Output {
    /// Open an endpoint for writing. `burst` (TS packets per datagram)
    /// overrides the endpoint's `pkt_size`; the default is 7.
    pub fn open(endpoint: &Endpoint, burst: Option<usize>) -> Result<Output> {
        endpoint.validate(Direction::Output)?;
        let description = endpoint.to_string();
        let kind = match endpoint {
            Endpoint::File(path) => {
                let file = File::create(path).map_err(|e| Error::io(format!("create {}", path.display()), e))?;
                Kind::Writer(BufWriter::with_capacity(FILE_BUFFER, Box::new(file)))
            }
            Endpoint::Stdio => Kind::Writer(BufWriter::with_capacity(FILE_BUFFER, Box::new(io::stdout()))),
            Endpoint::Net(net) => Kind::Udp(Box::new(open_udp(net, burst, &description)?)),
        };
        Ok(Output { kind, stats: OutputStats::default(), description })
    }

    /// The endpoint as given.
    pub fn description(&self) -> &str {
        &self.description
    }

    /// TS packets per datagram (sockets), `None` for files.
    pub fn packets_per_datagram(&self) -> Option<usize> {
        match &self.kind {
            Kind::Udp(u) => Some(u.burst),
            Kind::Writer(_) => None,
        }
    }

    /// Whether datagrams carry an RTP header.
    pub fn is_rtp(&self) -> bool {
        matches!(&self.kind, Kind::Udp(u) if u.rtp.is_some())
    }

    /// Whether this is a socket.
    pub fn is_network(&self) -> bool {
        matches!(self.kind, Kind::Udp(_))
    }

    /// The destination address of a socket.
    pub fn destination(&self) -> Option<SocketAddr> {
        match &self.kind {
            Kind::Udp(u) => Some(u.dest),
            Kind::Writer(_) => None,
        }
    }

    /// The counters so far.
    pub fn stats(&self) -> OutputStats {
        self.stats
    }

    /// Send a received datagram on, as unchanged as the two framings allow:
    /// a datagram whose framing matches this output (RTP to `rtp://`, raw
    /// to `udp://`) is sent byte for byte, header included; otherwise the
    /// packets are re-framed (`position` gives the RTP timestamp). A file
    /// output writes the packets.
    pub fn relay(&mut self, datagram: Option<&[u8]>, was_rtp: bool, packets: &[u8], position: u64) -> io::Result<()> {
        match (&mut self.kind, datagram) {
            (Kind::Udp(u), Some(d)) if u.rtp.is_some() == was_rtp => {
                let n = (packets.len() / PACKET_SIZE) as u64;
                match u.socket.send(d) {
                    Ok(_) => {}
                    Err(e) if net::is_refused(&e) => self.stats.refused += 1,
                    Err(e) => return Err(e),
                }
                self.stats.datagrams += 1;
                self.stats.packets += n;
                Ok(())
            }
            _ if packets.is_empty() => Ok(()),
            _ => self.send(packets, position),
        }
    }
}

fn open_udp(ep: &NetEndpoint, burst: Option<usize>, description: &str) -> Result<UdpSender> {
    let burst = burst.or_else(|| ep.packets_per_datagram()).unwrap_or(DEFAULT_PACKETS_PER_DATAGRAM);
    if !(1..=MAX_PACKETS_PER_DATAGRAM).contains(&burst) {
        return Err(Error::config(format!(
            "packets per datagram must be 1 to {MAX_PACKETS_PER_DATAGRAM}, not {burst}"
        )));
    }
    let (socket, dest) = net::open_sender(ep).map_err(|e| Error::io(format!("open {description}"), e))?;
    let rtp = (ep.protocol == Protocol::Rtp)
        .then(|| RtpWriter::new(ep.options.payload_type.unwrap_or(PAYLOAD_TYPE_MP2T), ep.options.ssrc));
    Ok(UdpSender { socket, dest, rtp, burst, buf: Vec::with_capacity(rtp::HEADER_LEN + burst * PACKET_SIZE) })
}

impl Sink for Output {
    fn send(&mut self, packets: &[u8], position: u64) -> io::Result<()> {
        let n = (packets.len() / PACKET_SIZE) as u64;
        match &mut self.kind {
            Kind::Writer(w) => {
                w.write_all(packets)?;
                self.stats.packets += n;
            }
            Kind::Udp(u) => {
                for chunk in packets.chunks(u.burst * PACKET_SIZE) {
                    let data: &[u8] = match &mut u.rtp {
                        Some(rtp) => {
                            u.buf.clear();
                            u.buf.extend_from_slice(&rtp.next_header(position));
                            u.buf.extend_from_slice(chunk);
                            &u.buf
                        }
                        None => chunk,
                    };
                    match u.socket.send(data) {
                        Ok(_) => {}
                        Err(e) if net::is_refused(&e) => self.stats.refused += 1,
                        Err(e) => return Err(e),
                    }
                    self.stats.datagrams += 1;
                }
                self.stats.packets += n;
            }
        }
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        match &mut self.kind {
            Kind::Writer(w) => w.flush(),
            Kind::Udp(_) => Ok(()),
        }
    }
}

impl Drop for Output {
    fn drop(&mut self) {
        let _ = Sink::flush(self);
    }
}
