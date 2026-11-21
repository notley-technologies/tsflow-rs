//! Where a stream comes from or goes to.
//!
//! An endpoint is written the way FFmpeg, VLC and TSDuck users expect:
//!
//! | Form | Meaning |
//! |---|---|
//! | `input.ts`, `file://input.ts` | a file |
//! | `-` | standard input or standard output |
//! | `udp://host:port` | send to `host:port`; as an input, receive on that local address |
//! | `udp://@:port` | receive on `port` on every interface |
//! | `udp://@239.1.1.1:port` | receive multicast: join the group on `port` |
//! | `rtp://...` | the same, with an RTP header (RFC 3550, payload type 33 MP2T per RFC 2250) |
//!
//! IPv6 addresses go in brackets: `udp://[ff15::1]:5000`. Options follow a
//! `?` and are joined by `&`:
//!
//! | Option | Direction | Meaning |
//! |---|---|---|
//! | `ttl=N` | output | IP time to live (multicast default 1, so a stream stays on the local network unless asked) |
//! | `iface=A`, `localaddr=A` | both | IPv4 address of the local interface: the multicast interface to join on or send from, and the source address of what is sent |
//! | `pkt_size=N` | both | output: bytes of TS per datagram, a multiple of 188 (default 1316, 7 packets); input: largest datagram accepted |
//! | `buffer_size=N` | both | socket buffer in bytes (input default 8 MiB) |
//! | `source=A` | input | source-specific multicast (IGMPv3): only accept the group from sender `A` |
//! | `reuse=0/1` | input | SO_REUSEADDR (default on for multicast, off for unicast) |
//! | `loop=0/1` | output | multicast loopback to this host (default on) |
//! | `ssrc=N`, `pt=N` | RTP output | RTP synchronisation source and payload type (defaults random, 33) |
//!
//! ```
//! use tsflow::endpoint::{Direction, Endpoint};
//!
//! let ep: Endpoint = "udp://@239.1.1.1:5000?iface=10.0.0.2".parse()?;
//! ep.validate(Direction::Input)?;
//! let net = ep.net().unwrap();
//! assert!(net.listen && net.port == 5000);
//! assert_eq!(ep.to_string(), "udp://@239.1.1.1:5000?iface=10.0.0.2");
//! # Ok::<(), tsflow::endpoint::EndpointError>(())
//! ```

use crate::PACKET_SIZE;
use std::fmt;
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::str::FromStr;

/// Most TS packets in one datagram. 128 is TSDuck's limit; anything above 7
/// needs IP fragmentation on a 1500-byte MTU.
pub const MAX_PACKETS_PER_DATAGRAM: usize = 128;
/// TS packets per datagram unless configured: 7 packets, 1316 bytes, the
/// most that fit a 1500-byte Ethernet MTU with IP, UDP and RTP headers.
pub const DEFAULT_PACKETS_PER_DATAGRAM: usize = 7;

/// A parsed endpoint. See the [module documentation](self) for the syntax.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Endpoint {
    /// A file.
    File(PathBuf),
    /// Standard input (as an input) or standard output (as an output).
    Stdio,
    /// A UDP or RTP socket.
    Net(NetEndpoint),
}

/// The datagram framing of a network endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Protocol {
    /// TS packets directly in UDP datagrams.
    Udp,
    /// TS packets after an RTP header (RFC 3550, RFC 2250).
    Rtp,
}

impl Protocol {
    /// The URL scheme: `udp` or `rtp`.
    pub fn scheme(self) -> &'static str {
        match self {
            Protocol::Udp => "udp",
            Protocol::Rtp => "rtp",
        }
    }
}

/// A `udp://` or `rtp://` endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct NetEndpoint {
    /// UDP or RTP.
    pub protocol: Protocol,
    /// Written with `@`: a local address to listen on (inputs only).
    pub listen: bool,
    /// Host as written: an IP literal (without brackets) or a name. `None`
    /// for `@:port`, every local interface.
    pub host: Option<String>,
    /// UDP port. 0 asks the system for a free port (inputs only).
    pub port: u16,
    /// The `?` options.
    pub options: NetOptions,
}

/// Options of a network endpoint. `None` means the default.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct NetOptions {
    /// `ttl`: IP time to live of sent datagrams.
    pub ttl: Option<u32>,
    /// `iface` or `localaddr`: IPv4 address of the local interface.
    pub interface: Option<Ipv4Addr>,
    /// `pkt_size`: output TS bytes per datagram, or the largest input
    /// datagram.
    pub packet_size: Option<usize>,
    /// `buffer_size`: socket buffer size in bytes.
    pub buffer_size: Option<usize>,
    /// `source`: source-specific multicast sender.
    pub source: Option<Ipv4Addr>,
    /// `reuse`: SO_REUSEADDR on a receiving socket.
    pub reuse: Option<bool>,
    /// `loop`: multicast loopback on a sending socket.
    pub multicast_loop: Option<bool>,
    /// `ssrc`: RTP synchronisation source identifier.
    pub ssrc: Option<u32>,
    /// `pt`: RTP payload type.
    pub payload_type: Option<u8>,
}

/// Which way an endpoint is used.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Direction {
    /// The stream is read from it.
    Input,
    /// The stream is written to it.
    Output,
}

/// An endpoint that cannot be parsed or used.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndpointError {
    endpoint: String,
    reason: String,
}

impl EndpointError {
    fn new(endpoint: &str, reason: impl Into<String>) -> EndpointError {
        EndpointError { endpoint: endpoint.to_string(), reason: reason.into() }
    }

    /// The endpoint as given.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// What is wrong with it.
    pub fn reason(&self) -> &str {
        &self.reason
    }
}

impl fmt::Display for EndpointError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.endpoint, self.reason)
    }
}

impl std::error::Error for EndpointError {}

const OPTIONS: &str = "ttl, iface, localaddr, pkt_size, buffer_size, source, reuse, loop, ssrc, pt";

impl FromStr for Endpoint {
    type Err = EndpointError;

    fn from_str(s: &str) -> Result<Endpoint, EndpointError> {
        Endpoint::parse(s)
    }
}

impl Endpoint {
    /// Parse an endpoint. Checks the syntax only; use
    /// [`validate`](Self::validate) for what depends on the direction.
    pub fn parse(s: &str) -> Result<Endpoint, EndpointError> {
        if s.is_empty() {
            return Err(EndpointError::new(s, "empty endpoint"));
        }
        if s == "-" {
            return Ok(Endpoint::Stdio);
        }
        let Some((scheme, rest)) = s.split_once("://") else {
            return Ok(Endpoint::File(PathBuf::from(s)));
        };
        let protocol = match scheme.to_ascii_lowercase().as_str() {
            "file" if rest.is_empty() => return Err(EndpointError::new(s, "file:// needs a path")),
            "file" => return Ok(Endpoint::File(PathBuf::from(rest))),
            "udp" => Protocol::Udp,
            "rtp" => Protocol::Rtp,
            other => {
                return Err(EndpointError::new(
                    s,
                    format!("unsupported scheme \"{other}\" (supported: udp, rtp, file)"),
                ))
            }
        };
        parse_net(s, protocol, rest).map(Endpoint::Net)
    }

    /// The network endpoint, if this is one.
    pub fn net(&self) -> Option<&NetEndpoint> {
        match self {
            Endpoint::Net(n) => Some(n),
            _ => None,
        }
    }

    /// Check that the endpoint can be used as an input or an output: an
    /// output needs a destination host and port, `@` is for inputs, and
    /// each option applies to one direction.
    pub fn validate(&self, direction: Direction) -> Result<(), EndpointError> {
        match self {
            Endpoint::Net(n) => n.validate(direction),
            _ => Ok(()),
        }
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Endpoint::File(p) => write!(f, "{}", p.display()),
            Endpoint::Stdio => f.write_str("-"),
            Endpoint::Net(n) => n.fmt(f),
        }
    }
}

fn parse_net(s: &str, protocol: Protocol, rest: &str) -> Result<NetEndpoint, EndpointError> {
    let err = |reason: String| EndpointError::new(s, reason);
    let (authority, query) = match rest.split_once('?') {
        Some((a, q)) => (a, Some(q)),
        None => (rest, None),
    };
    let authority = authority.strip_suffix('/').unwrap_or(authority);
    let (listen, authority) = match authority.strip_prefix('@') {
        Some(a) => (true, a),
        None => (false, authority),
    };
    let (host, port) = if let Some(v6) = authority.strip_prefix('[') {
        let (host, after) = v6.split_once(']').ok_or_else(|| err("missing \"]\" after an IPv6 address".into()))?;
        let port = after.strip_prefix(':').ok_or_else(|| err("missing \":port\"".into()))?;
        host.parse::<Ipv6Addr>().map_err(|_| err(format!("\"{host}\" is not an IPv6 address")))?;
        (Some(host), port)
    } else {
        let (host, port) = authority.rsplit_once(':').ok_or_else(|| err("missing \":port\"".into()))?;
        if host.contains(':') {
            return Err(err("IPv6 addresses go in brackets, for example udp://[ff15::1]:5000".into()));
        }
        (if host.is_empty() { None } else { Some(host) }, port)
    };
    if let Some(h) = host {
        if h.contains(|c: char| c.is_whitespace() || c == '/' || c == '@') {
            return Err(err(format!("\"{h}\" is not a host name or address")));
        }
    }
    let port: u16 = port.parse().map_err(|_| err(format!("\"{port}\" is not a port number (0 to 65535)")))?;
    let mut options = NetOptions::default();
    let mut seen: Vec<&str> = Vec::new();
    for pair in query.into_iter().flat_map(|q| q.split('&')).filter(|p| !p.is_empty()) {
        let (key, value) =
            pair.split_once('=').ok_or_else(|| err(format!("option \"{pair}\" needs a value (key=value)")))?;
        let canonical = if key == "localaddr" { "iface" } else { key };
        if seen.contains(&canonical) {
            return Err(err(format!("option \"{key}\" given twice")));
        }
        seen.push(canonical);
        let bad = |what: &str| err(format!("{key}={value}: {what}"));
        match key {
            "ttl" => {
                options.ttl =
                    Some(parse_int(value).filter(|&v| v <= 255).ok_or_else(|| bad("must be 0 to 255"))? as u32)
            }
            "iface" | "localaddr" => {
                options.interface =
                    Some(value.parse().map_err(|_| bad("must be the IPv4 address of a local interface"))?)
            }
            "pkt_size" => {
                options.packet_size =
                    Some(parse_int(value).filter(|&v| v > 0).ok_or_else(|| bad("must be a positive number of bytes"))?
                        as usize)
            }
            "buffer_size" => {
                options.buffer_size = Some(
                    parse_int(value).filter(|&v| v > 0 && v <= 1 << 30).ok_or_else(|| bad("must be 1 byte to 1 GiB"))?
                        as usize,
                )
            }
            "source" => options.source = Some(value.parse().map_err(|_| bad("must be an IPv4 address"))?),
            "reuse" => options.reuse = Some(parse_bool(value).ok_or_else(|| bad("must be 0 or 1"))?),
            "loop" => options.multicast_loop = Some(parse_bool(value).ok_or_else(|| bad("must be 0 or 1"))?),
            "ssrc" => {
                options.ssrc = Some(
                    parse_int(value).filter(|&v| v <= u64::from(u32::MAX)).ok_or_else(|| bad("must be 0 to 2^32-1"))?
                        as u32,
                )
            }
            "pt" => {
                options.payload_type =
                    Some(parse_int(value).filter(|&v| v <= 127).ok_or_else(|| bad("must be 0 to 127"))? as u8)
            }
            _ => return Err(err(format!("unknown option \"{key}\" (known: {OPTIONS})"))),
        }
    }
    Ok(NetEndpoint { protocol, listen, host: host.map(str::to_string), port, options })
}

/// Decimal or `0x` hexadecimal.
fn parse_int(s: &str) -> Option<u64> {
    match s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        Some(hex) => u64::from_str_radix(hex, 16).ok(),
        None => s.parse().ok(),
    }
}

fn parse_bool(s: &str) -> Option<bool> {
    match s {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

impl NetEndpoint {
    fn err(&self, reason: impl Into<String>) -> EndpointError {
        EndpointError::new(&self.to_string(), reason)
    }

    /// The host as an IP address, when it is written as one.
    pub fn ip(&self) -> Option<IpAddr> {
        self.host.as_deref().and_then(|h| h.parse().ok())
    }

    /// Whether the host is a multicast group address.
    pub fn is_multicast(&self) -> bool {
        self.ip().is_some_and(|ip| ip.is_multicast())
    }

    /// See [`Endpoint::validate`].
    pub fn validate(&self, direction: Direction) -> Result<(), EndpointError> {
        let o = &self.options;
        match direction {
            Direction::Output => {
                if self.listen {
                    return Err(
                        self.err("\"@\" marks an address to listen on; an output needs a destination host:port")
                    );
                }
                if self.host.is_none() {
                    return Err(self.err("an output needs a destination host"));
                }
                if self.port == 0 {
                    return Err(self.err("an output needs a destination port other than 0"));
                }
                if o.source.is_some() || o.reuse.is_some() {
                    return Err(self.err("source and reuse apply to inputs"));
                }
                if self.protocol == Protocol::Udp && (o.ssrc.is_some() || o.payload_type.is_some()) {
                    return Err(self.err("ssrc and pt apply to rtp:// outputs"));
                }
                if let Some(size) = o.packet_size {
                    let max = MAX_PACKETS_PER_DATAGRAM * PACKET_SIZE;
                    if size % PACKET_SIZE != 0 || size > max {
                        return Err(self.err(format!("pkt_size must be a multiple of 188 up to {max}")));
                    }
                }
            }
            Direction::Input => {
                if o.ttl.is_some() || o.multicast_loop.is_some() || o.ssrc.is_some() || o.payload_type.is_some() {
                    return Err(self.err("ttl, loop, ssrc and pt apply to outputs"));
                }
                if let Some(size) = o.packet_size {
                    if !(PACKET_SIZE..=65_536).contains(&size) {
                        return Err(self.err("pkt_size of an input must be 188 to 65536"));
                    }
                }
                if o.source.is_some() && !self.ip().is_some_and(|ip| ip.is_multicast() && ip.is_ipv4()) {
                    return Err(self.err(
                        "source needs an IPv4 multicast group, for example udp://@232.1.1.1:5000?source=10.0.0.1",
                    ));
                }
            }
        }
        Ok(())
    }

    /// TS packets per datagram from `pkt_size`, if given.
    pub fn packets_per_datagram(&self) -> Option<usize> {
        self.options.packet_size.map(|s| s / PACKET_SIZE)
    }

    /// The socket address: the host resolved (names through the system
    /// resolver), or the IPv4 wildcard address for `@:port`.
    pub fn socket_addr(&self) -> io::Result<SocketAddr> {
        let Some(host) = &self.host else {
            return Ok(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), self.port));
        };
        if let Ok(ip) = host.parse::<IpAddr>() {
            return Ok(SocketAddr::new(ip, self.port));
        }
        (host.as_str(), self.port)
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, format!("{host}: no address")))
    }
}

impl fmt::Display for NetEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}://", self.protocol.scheme())?;
        if self.listen {
            f.write_str("@")?;
        }
        match &self.host {
            Some(h) if h.contains(':') => write!(f, "[{h}]")?,
            Some(h) => f.write_str(h)?,
            None => {}
        }
        write!(f, ":{}", self.port)?;
        let o = &self.options;
        let mut sep = '?';
        let mut opt = |f: &mut fmt::Formatter<'_>, key: &str, value: &dyn fmt::Display| {
            let r = write!(f, "{sep}{key}={value}");
            sep = '&';
            r
        };
        if let Some(v) = o.ttl {
            opt(f, "ttl", &v)?;
        }
        if let Some(v) = o.interface {
            opt(f, "iface", &v)?;
        }
        if let Some(v) = o.packet_size {
            opt(f, "pkt_size", &v)?;
        }
        if let Some(v) = o.buffer_size {
            opt(f, "buffer_size", &v)?;
        }
        if let Some(v) = o.source {
            opt(f, "source", &v)?;
        }
        if let Some(v) = o.reuse {
            opt(f, "reuse", &u8::from(v))?;
        }
        if let Some(v) = o.multicast_loop {
            opt(f, "loop", &u8::from(v))?;
        }
        if let Some(v) = o.ssrc {
            opt(f, "ssrc", &v)?;
        }
        if let Some(v) = o.payload_type {
            opt(f, "pt", &v)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn net(s: &str) -> NetEndpoint {
        match Endpoint::parse(s).unwrap() {
            Endpoint::Net(n) => n,
            other => panic!("{s}: {other:?}"),
        }
    }

    fn reason(s: &str) -> String {
        Endpoint::parse(s).unwrap_err().reason().to_string()
    }

    #[test]
    fn files_and_stdio() {
        assert_eq!(Endpoint::parse("-").unwrap(), Endpoint::Stdio);
        assert_eq!(Endpoint::parse("in.ts").unwrap(), Endpoint::File("in.ts".into()));
        assert_eq!(Endpoint::parse("file:///tmp/a.ts").unwrap(), Endpoint::File("/tmp/a.ts".into()));
        assert_eq!(Endpoint::parse("C:\\streams\\a.ts").unwrap(), Endpoint::File("C:\\streams\\a.ts".into()));
        assert!(reason("").contains("empty"));
        assert!(reason("file://").contains("path"));
        assert!(reason("srt://host:9000").contains("unsupported scheme \"srt\""));
    }

    #[test]
    fn unicast_and_listen_forms() {
        let n = net("udp://127.0.0.1:5000");
        assert_eq!((n.protocol, n.listen, n.host.as_deref(), n.port), (Protocol::Udp, false, Some("127.0.0.1"), 5000));
        let n = net("udp://@:5000");
        assert!(n.listen && n.host.is_none());
        assert_eq!(n.socket_addr().unwrap(), "0.0.0.0:5000".parse().unwrap());
        let n = net("rtp://@239.1.2.3:1234/");
        assert!(n.listen && n.is_multicast() && n.protocol == Protocol::Rtp);
        let n = net("UDP://[ff15::1]:5000");
        assert_eq!(n.host.as_deref(), Some("ff15::1"));
        assert!(n.is_multicast());
        assert_eq!(n.to_string(), "udp://[ff15::1]:5000");
        let n = net("udp://localhost:7000");
        assert_eq!(n.socket_addr().unwrap().port(), 7000);
    }

    #[test]
    fn options() {
        let n = net("rtp://239.0.0.1:5000?ttl=4&localaddr=10.1.2.3&pkt_size=1316&buffer_size=0x100000&loop=0&ssrc=0xDEADBEEF&pt=96");
        let o = &n.options;
        assert_eq!(o.ttl, Some(4));
        assert_eq!(o.interface, Some(Ipv4Addr::new(10, 1, 2, 3)));
        assert_eq!(n.packets_per_datagram(), Some(7));
        assert_eq!(o.buffer_size, Some(1 << 20));
        assert_eq!(o.multicast_loop, Some(false));
        assert_eq!(o.ssrc, Some(0xDEAD_BEEF));
        assert_eq!(o.payload_type, Some(96));
        Endpoint::Net(n.clone()).validate(Direction::Output).unwrap();
        // Display gives a canonical form that parses back to the same thing.
        assert_eq!(net(&n.to_string()), n);
        let n = net("udp://@232.1.1.1:5000?source=10.0.0.1&reuse=1&iface=10.0.0.2");
        Endpoint::Net(n).validate(Direction::Input).unwrap();
    }

    #[test]
    fn syntax_errors_say_what_is_wrong() {
        assert!(reason("udp://host").contains("missing \":port\""));
        assert!(reason("udp://host:99999").contains("not a port number"));
        assert!(reason("udp://ff15::1:5000").contains("brackets"));
        assert!(reason("udp://[ff15::1:5000").contains("missing \"]\""));
        assert!(reason("udp://[nonsense]:5000").contains("not an IPv6 address"));
        assert!(reason("udp://h:1?ttl=300").contains("0 to 255"));
        assert!(reason("udp://h:1?ttl").contains("needs a value"));
        assert!(reason("udp://h:1?bogus=1").contains("unknown option \"bogus\""));
        assert!(reason("udp://h:1?iface=eth0").contains("IPv4 address of a local interface"));
        assert!(reason("udp://h:1?iface=1.2.3.4&localaddr=1.2.3.4").contains("given twice"));
        assert!(reason("udp://h:1?pt=200").contains("0 to 127"));
        assert!(reason("udp://bad host:1").contains("not a host name"));
    }

    #[test]
    fn direction_rules() {
        let check = |s: &str, d| Endpoint::parse(s).unwrap().validate(d).map_err(|e| e.reason().to_string());
        assert!(check("udp://@:5000", Direction::Output).unwrap_err().contains("listen"));
        assert!(check("udp://:5000", Direction::Output).unwrap_err().contains("destination host"));
        assert!(check("udp://1.2.3.4:0", Direction::Output).unwrap_err().contains("port other than 0"));
        assert!(check("udp://1.2.3.4:5?ssrc=1", Direction::Output).unwrap_err().contains("rtp://"));
        assert!(check("udp://1.2.3.4:5?pkt_size=1000", Direction::Output).unwrap_err().contains("multiple of 188"));
        assert!(check("udp://1.2.3.4:5?pkt_size=37600", Direction::Output).unwrap_err().contains("multiple of 188"));
        assert!(check("udp://1.2.3.4:5?source=1.1.1.1", Direction::Output).unwrap_err().contains("inputs"));
        assert!(check("udp://@:5000?ttl=2", Direction::Input).unwrap_err().contains("outputs"));
        assert!(check("udp://@:5000?source=1.1.1.1", Direction::Input).unwrap_err().contains("multicast group"));
        assert!(check("udp://@:5000?pkt_size=100", Direction::Input).unwrap_err().contains("188 to 65536"));
        check("udp://127.0.0.1:0", Direction::Input).unwrap();
        check("-", Direction::Output).unwrap();
    }
}
