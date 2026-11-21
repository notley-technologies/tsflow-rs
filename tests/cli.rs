//! The `tsflow` binary: exit statuses, `--expect`, JSON, usage errors,
//! play to recv over a real socket, and Ctrl-C.

mod common;

use common::*;
use std::io::{BufRead, BufReader, Read};
use std::process::{Child, Command, Output, Stdio};
use std::thread;

fn tsflow() -> Command {
    Command::new(env!("CARGO_BIN_EXE_tsflow"))
}

fn run(args: &[&str]) -> Output {
    tsflow().args(args).output().unwrap()
}

fn text(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

#[test]
fn analyze_passes_and_fails_expectations() {
    let path = temp_file("cli-analyze.ts", &generate(&Synth::frames(25)).data);
    let file = path.to_str().unwrap();
    let ok = run(&["analyze", file, "--expect", "klv-marking=US//UNCLASSIFIED", "--expect", "no-cc-errors"]);
    assert_eq!(ok.status.code(), Some(0), "{}", text(&ok.stderr));
    let out = text(&ok.stdout);
    assert!(out.contains("PASS klv-marking=US//UNCLASSIFIED: 0x0101 US//UNCLASSIFIED x25; 0x0102"), "{out}");
    assert!(out.contains("PCR 0x0100: 25 PCRs"), "{out}");
    let bad = run(&["analyze", file, "--expect", "klv-marking=US//SECRET", "--expect", "min-klv-rate=30"]);
    assert_eq!(bad.status.code(), Some(1));
    let out = text(&bad.stdout);
    assert!(out.contains("FAIL klv-marking=US//SECRET"), "{out}");
    assert!(out.contains("FAIL min-klv-rate=30"), "{out}");
    let json = run(&["analyze", file, "--json", "--expect", "no-cc-errors"]);
    assert_eq!(json.status.code(), Some(0));
    let out = text(&json.stdout);
    assert!(out.starts_with("{\"tsflow\":\"") && out.trim_end().ends_with('}'), "{out}");
    assert!(out.contains("\"passed\":true") && out.contains("\"packets\":2000"), "{out}");
    std::fs::remove_file(path).unwrap();
}

#[test]
fn usage_and_runtime_errors_exit_with_status_2() {
    let cases: &[&[&str]] = &[
        &["play"],
        &["recv", "udp://host"],
        &["recv", "udp://@:5000", "--expect", "bogus"],
        &["play", "in.ts", "udp://@:5000"],
        &["play", "-", "out.ts", "--loop"],
        &["analyze", "/nonexistent/input.ts"],
    ];
    for args in cases {
        let o = run(args);
        assert_eq!(o.status.code(), Some(2), "{args:?}: {}", text(&o.stderr));
        assert!(!o.stderr.is_empty());
    }
    let o = run(&["recv", "udp://host"]);
    assert!(text(&o.stderr).contains("missing \":port\""), "{}", text(&o.stderr));
    let o = run(&["--help"]);
    assert_eq!(o.status.code(), Some(0));
    assert!(text(&o.stdout).contains("udp://@239.1.1.1:port"));
}

/// Start `tsflow recv` and wait until it is listening.
fn start_recv(args: &[&str]) -> (Child, thread::JoinHandle<String>) {
    let mut child = tsflow().arg("recv").args(args).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
    let mut stderr = BufReader::new(child.stderr.take().unwrap());
    let mut line = String::new();
    stderr.read_line(&mut line).unwrap();
    assert!(line.contains("listening on"), "{line}");
    let drain = thread::spawn(move || {
        let mut rest = String::new();
        let _ = stderr.read_to_string(&mut rest);
        line + &rest
    });
    (child, drain)
}

fn free_port() -> u16 {
    std::net::UdpSocket::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

#[test]
fn play_to_recv_with_expectations() {
    let path = temp_file("cli-play.ts", &generate(&Synth::frames(40)).data);
    for (marking, status) in [("US//UNCLASSIFIED", 0), ("US//SECRET", 1)] {
        let port = free_port();
        let input = format!("udp://127.0.0.1:{port}");
        let expect = format!("klv-marking={marking}");
        let (child, stderr) = start_recv(&[
            &input,
            "--idle-exit",
            "0.7",
            "--duration",
            "30",
            "--expect",
            &expect,
            "--expect",
            "no-cc-errors",
            "--expect",
            "min-klv-rate=4",
            "--expect",
            "max-psi-interval=100",
            "--json",
        ]);
        let play = run(&["play", path.to_str().unwrap(), &input, "--latency", "50", "-q"]);
        assert_eq!(play.status.code(), Some(0), "{}", text(&play.stderr));
        assert!(text(&play.stderr).contains("sent 3200 packets"), "{}", text(&play.stderr));
        let out = child.wait_with_output().unwrap();
        let json = text(&out.stdout);
        assert_eq!(out.status.code(), Some(status), "{json}\n{}", stderr.join().unwrap());
        assert!(json.contains("\"stopped\":\"input went quiet\""), "{json}");
        assert!(json.contains("\"packets\":3200"), "{json}");
    }
    std::fs::remove_file(path).unwrap();
}

#[cfg(unix)]
#[test]
fn ctrl_c_stops_recv_with_a_summary() {
    let port = free_port();
    let (child, stderr) = start_recv(&[&format!("udp://127.0.0.1:{port}"), "--duration", "30"]);
    let killed = Command::new("kill").args(["-s", "INT", &child.id().to_string()]).status().unwrap();
    assert!(killed.success());
    let out = child.wait_with_output().unwrap();
    let stdout = text(&out.stdout);
    assert_eq!(out.status.code(), Some(0), "{stdout}\n{}", stderr.join().unwrap());
    assert!(stdout.contains("(interrupted)"), "{stdout}");
    assert!(stdout.contains("packets    0"), "{stdout}");
}
