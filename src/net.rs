//! Socket creation for network endpoints.

use crate::endpoint::NetEndpoint;
use socket2::{Domain, Protocol as SockProtocol, SockAddr, Socket, Type};
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket};

/// Receive buffer asked for unless `buffer_size` says otherwise: about 2 s
/// of a 30 Mbit/s stream, so a short stall of the reading thread loses
/// nothing. The kernel may grant less (Linux caps it at
/// `net.core.rmem_max`); the granted size is reported.
pub const DEFAULT_RECV_BUFFER: usize = 8 << 20;
/// Multicast TTL unless `ttl` says otherwise: 1, the local network only.
pub const DEFAULT_MULTICAST_TTL: u32 = 1;

/// A bound receiving socket and what the system granted.
pub(crate) struct Receiver {
    pub socket: UdpSocket,
    pub local: SocketAddr,
    pub buffer_size: Option<usize>,
}

/// A socket bound and, for a multicast group, joined.
///
/// A group is bound by its own address where the system allows it (Linux,
/// macOS), so the socket does not also receive other traffic sent to the
/// port, and by the wildcard address on Windows, which refuses that.
pub(crate) fn open_receiver(ep: &NetEndpoint) -> io::Result<Receiver> {
    let addr = ep.socket_addr()?;
    let domain = if addr.is_ipv4() { Domain::IPV4 } else { Domain::IPV6 };
    let socket = Socket::new(domain, Type::DGRAM, Some(SockProtocol::UDP))?;
    let multicast = addr.ip().is_multicast();
    if ep.options.reuse.unwrap_or(multicast) {
        socket.set_reuse_address(true)?;
    }
    let requested = ep.options.buffer_size.unwrap_or(DEFAULT_RECV_BUFFER);
    // A refused size is not fatal: the default is kept and reported.
    let _ = socket.set_recv_buffer_size(requested);
    let bind_ip = match addr.ip() {
        ip if multicast && cfg!(windows) => match ip {
            IpAddr::V4(_) => IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            IpAddr::V6(_) => IpAddr::V6(Ipv6Addr::UNSPECIFIED),
        },
        ip => ip,
    };
    socket.bind(&SockAddr::from(SocketAddr::new(bind_ip, addr.port())))?;
    match addr.ip() {
        IpAddr::V4(group) if multicast => {
            let iface = ep.options.interface.unwrap_or(Ipv4Addr::UNSPECIFIED);
            match ep.options.source {
                Some(source) => join_ssm(&socket, source, group, iface)?,
                None => socket.join_multicast_v4(&group, &iface)?,
            }
        }
        IpAddr::V6(group) if multicast => socket.join_multicast_v6(&group, 0)?,
        _ => {}
    }
    let buffer_size = socket.recv_buffer_size().ok();
    let socket: UdpSocket = socket.into();
    let local = socket.local_addr()?;
    Ok(Receiver { socket, local, buffer_size })
}

#[cfg(any(
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    windows
))]
fn join_ssm(socket: &Socket, source: Ipv4Addr, group: Ipv4Addr, iface: Ipv4Addr) -> io::Result<()> {
    socket.join_ssm_v4(&source, &group, &iface)
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    windows
)))]
fn join_ssm(_: &Socket, _: Ipv4Addr, _: Ipv4Addr, _: Ipv4Addr) -> io::Result<()> {
    Err(io::Error::new(io::ErrorKind::Unsupported, "source-specific multicast is not supported on this system"))
}

/// A sending socket connected to the destination.
///
/// `iface` binds the socket to that local address, which on Linux also
/// selects the interface multicast leaves from; `IP_MULTICAST_IF` is set as
/// well for the other systems. Connecting lets the system report an
/// unreachable unicast destination; [`is_refused`] errors are expected
/// while nobody listens.
pub(crate) fn open_sender(ep: &NetEndpoint) -> io::Result<(UdpSocket, SocketAddr)> {
    let dest = ep.socket_addr()?;
    let domain = if dest.is_ipv4() { Domain::IPV4 } else { Domain::IPV6 };
    let socket = Socket::new(domain, Type::DGRAM, Some(SockProtocol::UDP))?;
    let local: SocketAddr = match (dest, ep.options.interface) {
        (SocketAddr::V4(_), Some(iface)) => (iface, 0).into(),
        (SocketAddr::V4(_), None) => (Ipv4Addr::UNSPECIFIED, 0).into(),
        (SocketAddr::V6(_), _) => (Ipv6Addr::UNSPECIFIED, 0).into(),
    };
    socket.bind(&SockAddr::from(local))?;
    if let Some(size) = ep.options.buffer_size {
        socket.set_send_buffer_size(size)?;
    }
    match dest.ip() {
        IpAddr::V4(_) if dest.ip().is_multicast() => {
            socket.set_multicast_ttl_v4(ep.options.ttl.unwrap_or(DEFAULT_MULTICAST_TTL))?;
            socket.set_multicast_loop_v4(ep.options.multicast_loop.unwrap_or(true))?;
            if let Some(iface) = ep.options.interface {
                socket.set_multicast_if_v4(&iface)?;
            }
        }
        IpAddr::V6(_) if dest.ip().is_multicast() => {
            socket.set_multicast_hops_v6(ep.options.ttl.unwrap_or(DEFAULT_MULTICAST_TTL))?;
            socket.set_multicast_loop_v6(ep.options.multicast_loop.unwrap_or(true))?;
        }
        _ => {
            if let Some(ttl) = ep.options.ttl {
                socket.set_ttl(ttl)?;
            }
        }
    }
    socket.connect(&SockAddr::from(dest))?;
    Ok((socket.into(), dest))
}

/// Nobody is listening at a unicast destination (ICMP port unreachable;
/// Windows reports it as a reset). Senders keep going: a receiver may
/// start later.
pub(crate) fn is_refused(e: &io::Error) -> bool {
    matches!(e.kind(), io::ErrorKind::ConnectionRefused | io::ErrorKind::ConnectionReset)
}

/// A read timed out or was interrupted: try again.
pub(crate) fn is_retry(e: &io::Error) -> bool {
    matches!(e.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut | io::ErrorKind::Interrupted)
}
