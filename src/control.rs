//! Control input for a live injector: lines of text (JSON commands) read
//! on background threads from standard input, a file, a UDP port or a Unix
//! socket, and handed to the packet loop through a channel.
//!
//! The packet loop never waits for control input: it takes whatever lines
//! have arrived between two reads of the stream ([`Control::try_lines`]),
//! so a command takes effect before the next packets are processed and a
//! quiet control channel costs nothing.
//!
//! Each line is one unit: everything it says is applied together, between
//! two packets. A UDP datagram may hold several lines. Lines longer than
//! [`MAX_LINE`] are rejected (reported, not applied) so a broken sender
//! cannot exhaust memory.

use crate::{Error, Result};
use std::fmt;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read};
use std::net::{SocketAddr, UdpSocket};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::mpsc::{self, Receiver, SyncSender, TryRecvError};
use std::thread;

/// The longest control line accepted, in bytes.
pub const MAX_LINE: usize = 1 << 20;
/// Lines waiting for the packet loop at most; a sender that outruns the
/// loop by more than this waits (stdin, files, Unix sockets) or loses
/// datagrams (UDP, like any UDP receiver that falls behind).
const QUEUE: usize = 4096;

/// Where control lines come from.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ControlSource {
    /// Standard input (`-`).
    Stdin,
    /// A file read once from start to end (`file:PATH` or a plain path).
    File(PathBuf),
    /// Datagrams to this local address (`udp://@:PORT`,
    /// `udp://127.0.0.1:PORT`); each holds one or more lines.
    Udp(SocketAddr),
    /// A Unix stream socket created at this path (`unix:PATH`); any number
    /// of clients may connect and write lines.
    #[cfg(unix)]
    Unix(PathBuf),
}

impl FromStr for ControlSource {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, String> {
        if s == "-" {
            return Ok(ControlSource::Stdin);
        }
        if let Some(rest) = s.strip_prefix("udp://") {
            let rest = rest.strip_prefix('@').unwrap_or(rest);
            let rest = if rest.starts_with(':') { format!("0.0.0.0{rest}") } else { rest.to_string() };
            return rest
                .parse()
                .map(ControlSource::Udp)
                .map_err(|_| format!("\"{s}\": expected udp://@:PORT or udp://ADDRESS:PORT"));
        }
        if let Some(path) = s.strip_prefix("unix:") {
            #[cfg(unix)]
            {
                if path.is_empty() {
                    return Err("unix: needs a socket path".into());
                }
                return Ok(ControlSource::Unix(PathBuf::from(path)));
            }
            #[cfg(not(unix))]
            {
                let _ = path;
                return Err("Unix sockets are not available on this system; use udp://@:PORT".into());
            }
        }
        let path = s.strip_prefix("file:").unwrap_or(s);
        if path.is_empty() {
            return Err("an empty control path".into());
        }
        Ok(ControlSource::File(PathBuf::from(path)))
    }
}

impl fmt::Display for ControlSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ControlSource::Stdin => f.write_str("standard input"),
            ControlSource::File(p) => write!(f, "{}", p.display()),
            ControlSource::Udp(a) => write!(f, "udp://{a}"),
            #[cfg(unix)]
            ControlSource::Unix(p) => write!(f, "unix:{}", p.display()),
        }
    }
}

/// One control line and where it came from, for error messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlLine {
    /// "standard input line 3", "udp 127.0.0.1:50000", ...
    pub origin: String,
    /// The line without its end-of-line characters; `Err` when it could
    /// not be read as a line (too long, not UTF-8).
    pub text: std::result::Result<String, String>,
}

/// The receiving end of the control threads.
#[derive(Debug)]
pub struct Control {
    rx: Receiver<ControlLine>,
    local_addr: Option<SocketAddr>,
    description: String,
}

impl Control {
    /// Start reading `source` on a background thread. The thread ends when
    /// the source ends (standard input or a file) or with the program.
    pub fn open(source: &ControlSource) -> Result<Control> {
        let (tx, rx) = mpsc::sync_channel(QUEUE);
        let description = source.to_string();
        let mut local_addr = None;
        match source {
            ControlSource::Stdin => spawn_reader(Box::new(io::stdin()), "standard input".into(), tx),
            ControlSource::File(path) => {
                let f = File::open(path).map_err(|e| Error::io(format!("open control file {}", path.display()), e))?;
                spawn_reader(Box::new(f), path.display().to_string(), tx);
            }
            ControlSource::Udp(addr) => {
                let socket = UdpSocket::bind(addr).map_err(|e| Error::io(format!("bind control socket {addr}"), e))?;
                local_addr = socket.local_addr().ok();
                thread::spawn(move || udp_loop(socket, tx));
            }
            #[cfg(unix)]
            ControlSource::Unix(path) => {
                // A socket file left by an earlier run would make bind fail.
                if std::fs::symlink_metadata(path).map(|m| is_socket(&m)).unwrap_or(false) {
                    let _ = std::fs::remove_file(path);
                }
                let listener = std::os::unix::net::UnixListener::bind(path)
                    .map_err(|e| Error::io(format!("create control socket {}", path.display()), e))?;
                let name = path.display().to_string();
                thread::spawn(move || {
                    let mut client = 0u64;
                    for stream in listener.incoming() {
                        let Ok(stream) = stream else { continue };
                        client += 1;
                        spawn_reader(Box::new(stream), format!("{name} client {client}"), tx.clone());
                    }
                });
            }
        }
        Ok(Control { rx, local_addr, description })
    }

    /// The bound address of a UDP control socket (useful with port 0).
    #[must_use]
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.local_addr
    }

    /// What is being read, for messages.
    #[must_use]
    pub fn description(&self) -> &str {
        &self.description
    }

    /// The lines that have arrived, without waiting.
    pub fn try_lines(&self) -> Vec<ControlLine> {
        let mut lines = Vec::new();
        loop {
            match self.rx.try_recv() {
                Ok(line) => lines.push(line),
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => return lines,
            }
        }
    }
}

#[cfg(unix)]
fn is_socket(m: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::FileTypeExt;
    m.file_type().is_socket()
}

/// Read lines from `reader` until it ends, sending each.
fn spawn_reader(reader: Box<dyn Read + Send>, name: String, tx: SyncSender<ControlLine>) {
    thread::spawn(move || {
        let mut reader = BufReader::new(reader);
        let mut n = 0u64;
        loop {
            n += 1;
            let text = match read_line(&mut reader) {
                Ok(Some(t)) => t,
                Ok(None) => return,
                Err(e) if e.kind() == io::ErrorKind::InvalidData => Err(e.to_string()),
                Err(_) => return,
            };
            if matches!(&text, Ok(t) if t.trim().is_empty()) {
                continue;
            }
            if tx.send(ControlLine { origin: format!("{name} line {n}"), text }).is_err() {
                return;
            }
        }
    });
}

/// One line, bounded by [`MAX_LINE`]: `Ok(None)` at the end, `Ok(Err)`
/// for a line too long or not UTF-8 (the rest of it is skipped).
fn read_line(r: &mut impl BufRead) -> io::Result<Option<std::result::Result<String, String>>> {
    let mut buf = Vec::new();
    let mut too_long = false;
    loop {
        let chunk = r.fill_buf()?;
        if chunk.is_empty() {
            if buf.is_empty() && !too_long {
                return Ok(None);
            }
            break;
        }
        let (take, done) = match chunk.iter().position(|&b| b == b'\n') {
            Some(i) => (i + 1, true),
            None => (chunk.len(), false),
        };
        if !too_long {
            if buf.len() + take > MAX_LINE + 2 {
                too_long = true;
                buf.clear();
            } else {
                buf.extend_from_slice(&chunk[..take]);
            }
        }
        r.consume(take);
        if done {
            break;
        }
    }
    if too_long {
        return Ok(Some(Err(format!("line longer than {MAX_LINE} bytes"))));
    }
    while matches!(buf.last(), Some(b'\n' | b'\r')) {
        buf.pop();
    }
    Ok(Some(String::from_utf8(buf).map_err(|_| "the line is not UTF-8".to_string())))
}

fn udp_loop(socket: UdpSocket, tx: SyncSender<ControlLine>) {
    let mut buf = vec![0u8; 65_536];
    loop {
        let Ok((n, from)) = socket.recv_from(&mut buf) else { continue };
        let origin = format!("udp {from}");
        let text = match std::str::from_utf8(&buf[..n]) {
            Ok(t) => t,
            Err(_) => {
                let _ = tx.try_send(ControlLine { origin, text: Err("the datagram is not UTF-8".into()) });
                continue;
            }
        };
        for line in text.lines().filter(|l| !l.trim().is_empty()) {
            // A full queue drops the datagram's remaining lines, as a full
            // socket buffer would.
            if tx.try_send(ControlLine { origin: origin.clone(), text: Ok(line.to_string()) }).is_err() {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::time::{Duration, Instant};

    #[test]
    fn sources_parse() {
        assert_eq!("-".parse(), Ok(ControlSource::Stdin));
        assert_eq!("udp://@:5000".parse(), Ok(ControlSource::Udp("0.0.0.0:5000".parse().unwrap())));
        assert_eq!("udp://127.0.0.1:0".parse(), Ok(ControlSource::Udp("127.0.0.1:0".parse().unwrap())));
        assert_eq!("cmds.ndjson".parse(), Ok(ControlSource::File("cmds.ndjson".into())));
        assert_eq!("file:-x".parse(), Ok(ControlSource::File("-x".into())));
        assert!("udp://nowhere".parse::<ControlSource>().is_err());
        #[cfg(unix)]
        assert_eq!("unix:/tmp/k.sock".parse(), Ok(ControlSource::Unix("/tmp/k.sock".into())));
    }

    #[test]
    fn lines_are_bounded_and_trimmed() {
        let mut long = vec![b'x'; MAX_LINE + 10];
        long.extend_from_slice(b"\n{\"emit\": true}\r\n\xff\n");
        let mut r = BufReader::with_capacity(1000, Cursor::new(long));
        assert_eq!(read_line(&mut r).unwrap(), Some(Err(format!("line longer than {MAX_LINE} bytes"))));
        assert_eq!(read_line(&mut r).unwrap(), Some(Ok("{\"emit\": true}".to_string())));
        assert_eq!(read_line(&mut r).unwrap(), Some(Err("the line is not UTF-8".to_string())));
        assert_eq!(read_line(&mut r).unwrap(), None);
    }

    /// Wait for `n` lines; real time only as a hang guard.
    fn collect(c: &Control, n: usize) -> Vec<ControlLine> {
        let give_up = Instant::now() + Duration::from_secs(60);
        let mut got = Vec::new();
        while got.len() < n && Instant::now() < give_up {
            got.extend(c.try_lines());
            thread::yield_now();
        }
        got
    }

    #[test]
    fn udp_datagrams_split_into_lines() {
        let c = Control::open(&"udp://127.0.0.1:0".parse().unwrap()).unwrap();
        let to = c.local_addr().unwrap();
        let s = UdpSocket::bind("127.0.0.1:0").unwrap();
        s.send_to(b"{\"set\": {\"13\": 1}}\n\n{\"emit\": true}", to).unwrap();
        let lines = collect(&c, 2);
        let texts: Vec<_> = lines.iter().map(|l| l.text.clone().unwrap()).collect();
        assert_eq!(texts, ["{\"set\": {\"13\": 1}}", "{\"emit\": true}"]);
        assert!(lines[0].origin.starts_with("udp 127.0.0.1:"));
    }

    #[test]
    fn a_file_is_read_to_its_end() {
        let dir = std::env::temp_dir().join(format!("tsflow-control-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("c.ndjson");
        std::fs::write(&path, "{\"emit\": true}\n\n{\"clear_all\": true}\n").unwrap();
        let c = Control::open(&ControlSource::File(path.clone())).unwrap();
        let lines = collect(&c, 2);
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[1].origin, format!("{} line 3", path.display()));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn unix_clients_send_lines() {
        use std::io::Write;
        let dir = std::env::temp_dir().join(format!("tsflow-control-unix-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("k.sock");
        let c = Control::open(&ControlSource::Unix(path.clone())).unwrap();
        let mut a = std::os::unix::net::UnixStream::connect(&path).unwrap();
        a.write_all(b"{\"emit\": true}\n").unwrap();
        drop(a);
        let mut b = std::os::unix::net::UnixStream::connect(&path).unwrap();
        b.write_all(b"{\"clear_all\": true}\n").unwrap();
        let lines = collect(&c, 2);
        assert_eq!(lines.len(), 2);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
