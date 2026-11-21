# tsflow

An MPEG transport stream toolkit in the spirit of TSDuck, in Rust: real-time playback paced by the PCR, UDP and RTP send and receive (unicast, multicast, source-specific multicast), live monitoring with scriptable checks, offline analysis, and MISB KLV injection from files or a live control channel, with first-class awareness of MISB KLV metadata.

It is built for workloads where a stream must be right and a tool must not fall over: integer timing that does not drift over days, bounded memory whatever arrives, no panics on hostile input, no `unsafe`, and errors that say what was being done to which endpoint.

tsflow is the I/O half of a family of crates. The libraries never touch a file, a socket or a clock; tsflow does:

| Crate | Role |
|---|---|
| [`mpegts`](https://github.com/notley-technologies/mpegts-rs) | Transport stream demultiplexer and analyser (`Demuxer`, `Report`), packet and PES header parsers, the `PcrTimeline` that gives every packet its program clock time, and the `mux` write primitives. |
| [`misb`](https://github.com/notley-technologies/misb-rs) | MISB ST 0601 decoder (with ST 0102 security, ST 0903 VMTI and the other embedded sets) and encoder. |
| **tsflow** | Files, pipes, sockets, the wall clock, and the command line tools built on them. |

## Commands

```text
tsflow play     <INPUT> <OUTPUT> [--loop | --passes N] [--latency MS] [--burst N] [--rate-fallback BPS] [--pcr-pid PID] [--no-pace]
tsflow recv     <INPUT> [--forward OUTPUT] [--out FILE] [--idle-exit S] [--duration S] [--count N] [--expect CHECK]... [--json]
tsflow analyze  <FILE> [--expect CHECK]... [--json]
tsflow inject   <INPUT> <OUTPUT> (--klv FILE | --ndjson FILE) [--mode sync|async] [--rate RATE] [--anchor ANCHOR] [--snap]
                [--interpolate] [--video-anchor] [--video-timestamps off|add|rewrite]
                [--existing keep|replace|fail] [--program N] [--pid PID] [--cbr [--drop-on-overflow]] [--pace | --no-pace] [--strict] [--json]
tsflow live     <INPUT> <OUTPUT> [--state FILE] [--control SOURCE]... [--mode sync|async] [--rate RATE] [--anchor ANCHOR]
                [--video-anchor] [--video-timestamps off|add|rewrite] [--max-age MS] [--cbr] [--duration S] [--json]
```

`-q` silences the once-a-second status lines; `tsflow <command> --help` lists everything.

### play

Plays a stream in real time, each packet at its time on the program clock:

```bash
tsflow play input.ts udp://239.1.1.1:5000?ttl=4 --loop
tsflow play input.ts rtp://10.0.0.5:5004
tsflow play input.m2ts out.ts                    # 192-byte M2TS to 188-byte TS, at wire pace
tsflow play input.ts out.ts --no-pace            # as fast as possible
tsflow play udp://@:5000 udp://10.0.0.5:5000 --latency 300   # re-pace (de-jitter) a live stream
```

```text
tsflow play: input.ts (188-byte TS, 3.05 Mb/s, PCR PID 0x0100) -> rtp://127.0.0.1:15600 (7 packets per RTP datagram), latency 100 ms, 2 passes
[    1s] play   2.71 Mb/s | pass 1 | 1799 packets | stream 0.9 s | queue 459 ms | late 0.3 ms
[    2s] play   3.01 Mb/s | pass 1 | 3801 packets | stream 1.9 s | queue 179 ms | late 0.2 ms
...
tsflow play: sent 12000 packets (2.26 MB) in 1715 datagrams, 2 passes, 5.998 s of stream in 6.099 s, worst lateness 1.0 ms
```

How it paces:

- Every packet gets a position on a continuous 27 MHz time line from `mpegts::PcrTimeline`: interpolated between PCRs as the T-STD model defines (ISO/IEC 13818-1 2.4.2.2), continuous across clock wraps, discontinuities and loops.
- The position maps onto the wall clock with a fixed delay, `--latency`: `due = anchor + (position - anchor_position) / 27 MHz`, in integer nanoseconds from the anchor, so a PCR packet leaves at its PCR time and 24 hours of playback do not drift by a nanosecond.
- Packets go 7 to a datagram (1316 bytes, `--burst` or `?pkt_size=` to change). A datagram leaves when its last packet is due, the way a network encapsulator sends once a datagram is full, so no packet leaves early; a packet waits at most 10 ms for the rest of its datagram, which flushes partial datagrams at low rates and pauses.
- If the input stalls (a packet would already be due by more than half the latency) or runs ahead (it would wait more than the latency plus one second), the schedule re-anchors, and the status line says so.
- A file is read only as fast as the output needs it. A socket input is scheduled from each datagram's arrival: the latency is then the de-jitter buffer.
- Without PCR (or before the second one) packets are timed at `--rate-fallback`, by default the file's average rate.

**What a receiver sees at the loop point.** When the file starts again, the program clock, the timestamps and every continuity counter jump back. tsflow rewrites the repeated passes so a receiver can follow:

- the first PCR on each PID in the new pass carries `discontinuity_indicator` = 1 (ISO/IEC 13818-1 2.4.3.5), so receivers restart clock recovery instead of seeing time run backwards, and treat the PTS/DTS that follow as the new time base;
- continuity counters are offset per PID so they continue from the last value sent (2.4.3.3): no CC errors are reported, and the input's own counter pattern (errors included) is kept;
- PAT and PMT are unchanged (same versions);
- the last PES of a pass may be cut short if the file does not end on a PES boundary, and the next pass may start inside a PES; decoders drop both and resynchronise at the next payload unit start (video shows artefacts until the next random access point unless the file starts with one);
- KLV time stamps (ST 0601 tag 2) are not rewritten and go back to the start of the file; `recv` reports one "step back" per loop;
- pacing has no gap and no burst at the loop point.

### recv

Receives and monitors a stream, live:

```bash
tsflow recv udp://@:5000 --forward udp://127.0.0.1:5001         # watch it in VLC at udp://@:5001 meanwhile
tsflow recv rtp://@239.1.1.1:5004?iface=10.0.0.2 --out recording.ts
tsflow recv udp://@:5000 --idle-exit 2 --expect klv-marking=US//UNCLASSIFIED --expect no-cc-errors --json
```

```text
tsflow recv: listening on rtp://@:15600 (bound 0.0.0.0:15600, receive buffer 416 KiB)
tsflow recv: program 1 (PMT 0x1000, PCR 0x0100): 0x0100 video H.264/AVC, 0x0101 metadata KLV (sync), 0x0102 metadata KLV (async)
[    1s]   3.03 Mb/s | CC errors 0 | PCR jitter 3.0 ms | KLV 0x0101 25/s US//UNCLASSIFIED | KLV 0x0102 5/s US//UNCLASSIFIED
...
tsflow recv: rtp://@:15600 (input went quiet)
packets    12000 (2.26 MB), 3.01 Mb/s
duration   5.999 s stream time, 5.996 s wall clock
input      188-byte TS, 0 bytes skipped, 0 sync losses; 1715 datagrams (1715 RTP: 0 lost, 0 late, 0 restarts); receive buffer 416 KiB
errors     0 CC errors, 0 transport errors
...
PCR 0x0100: 150 PCRs, interval 39.0 / 40.0 / 41.0 ms (min/avg/max), 0 over 100 ms, 1 discontinuities, 0 jumps, arrival jitter 3.36 ms p-p
PAT 0x0000: 76 sections, every 40.0 to 80.0 ms
KLV 0x0101 (sync): 150 ST 0601 packets in 150 PES, 25.00/s; checksums 150 ok, 0 bad, 0 missing
  marking  US//UNCLASSIFIED x150
  tag 2    2023-11-14T22:13:20.000000Z to 2023-11-14T22:13:22.960000Z (1 steps back)
  lead     164.0 to 175.0 ms before PTS, 0 outside 0 to 1000 ms
PASS klv-marking=US//UNCLASSIFIED: 0x0101 US//UNCLASSIFIED x150; 0x0102 US//UNCLASSIFIED x30
```

What it measures:

| Area | Figures |
|---|---|
| Input | datagrams, RTP or raw TS (detected per datagram), RTP loss, late and restarts from the sequence numbers (RFC 3550 A.1), framing (188/192/204), bytes skipped, sync losses, the receive buffer the kernel granted |
| Transport | packets, bitrate (wall clock per status line, stream time in the summary), continuity errors per PID, transport errors |
| Programs | programs and streams from the `mpegts` report, printed whenever they change |
| PCR | per PCR PID: count, interval min/avg/max, intervals over 100 ms (ISO/IEC 13818-1 2.7.2), flagged discontinuities, unflagged jumps, and the PCR-versus-arrival jitter (peak to peak within each 1 s window) |
| PSI | PAT and every PMT: repetition interval in stream time |
| KLV | per KLV stream, sync or async: ST 0601 packets decoded with `misb`, rate, checksum status (tag 1), ST 0102 security marking (tag 48, as `US//UNCLASSIFIED`), tag 2 time span and steps back, and for sync KLV how long before its PTS each access unit arrived (MISB ST 1402: before the PTS, at most 1 s before) |

`--forward` passes every datagram on as it arrived, before analysis: byte for byte when the framing matches (RTP to `rtp://`, raw to `udp://`), re-framed otherwise. A UDP port has one reader, so a player cannot bind the port the monitor holds; forwarding solves that. `--out` records the TS packets (RTP removed).

The run ends at the end of a file or pipe, when nothing has arrived for `--idle-exit` seconds (counted from the first datagram, so the receiver can start before the sender), after `--duration` seconds, once `--count` TS packets have arrived (exact, independent of timing, useful in scripts that know what they sent), or on Ctrl-C. The summary goes to standard output, the status lines to standard error.

### analyze

The same report for a file, from stream time only, with bounded memory whatever its size:

```bash
tsflow analyze input.ts
tsflow analyze input.m2ts --json | jq .summary.klv
tsflow analyze out.ts --expect no-cc-errors --expect max-psi-interval=100
```

### Checks for scripts

`recv` and `analyze` take `--expect`, any number of times:

| Check | Passes when |
|---|---|
| `no-cc-errors`, `max-cc-errors=N` | continuity errors on all PIDs are 0, or at most N |
| `min-packets=N` | at least N TS packets arrived |
| `klv-streams=N` | exactly N KLV streams were seen |
| `klv-marking=MARKING` | KLV arrived and every ST 0601 packet carries this ST 0102 marking |
| `min-klv-rate=HZ` | every KLV stream delivered at least HZ packets per second |
| `klv-checksums` | KLV arrived and every checksum matched |
| `max-pcr-interval=MS` | the longest PCR interval is at most MS |
| `max-pcr-jitter=MS` | the PCR arrival jitter is at most MS peak to peak (live input) |
| `max-psi-interval=MS` | the PAT and every PMT repeated at least every MS |
| `no-rtp-loss` | no RTP sequence gaps (passes for raw UDP) |

Exit status: **0** every check passed, **1** a check failed, **2** a usage or runtime error. `--json` prints one object: `{"tsflow": version, "stopped": reason, "passed": bool, "checks": [...], "summary": {...}}`.

### inject

Puts a metadata file into a stream as MISB ST 1402.2 KLV, synchronous (stream_type 0x15, a PTS on every packet) or asynchronous (0x06, SMPTE RP 217):

```bash
tsflow inject video.ts out.ts --klv flight.klv --anchor first-frame=2024-05-01T12:00:00Z
tsflow inject video.ts out.ts --ndjson track.ndjson --rate 30hz
tsflow inject video.ts udp://239.1.1.1:5000 --klv flight.klv           # paced, to the network
tsflow inject video.ts out.ts --klv flight.klv --cbr --strict --json    # same size, fail if anything is left out
```

Replacing the KLV of a synthetic stream (`examples/synth`) with a recorded file (here its own synchronous KLV, taken out with `ffmpeg -map 0:d:0 -c copy -f data`), `tsflow inject video.ts out.ts --klv flight.klv --existing replace` prints:

```text
tsflow inject: video.ts + flight.klv -> out.ts (sync KLV, no pacing)
tsflow inject: program 1 (PMT 0x1000), KLV on PID 0x0101, service 0
tsflow inject: anchor PTS 918000 = 2023-11-14T22:13:20.000000Z
tsflow inject: 100 KLV units inserted from 100 samples; 8000 packets in, 7980 out (+100 KLV, 100 PSI); skipped: 0 late, 0 out of order, 0 stale, 0 unused; dropped by the injector: 0 late, 0 overflow, 0 discontinuity, 0 unplaced
```

- **What changes.** Every input packet goes out unchanged except the PAT (resent as it is), the target program's PMT (rewritten with the KLV stream, version + 1, new CRC) and the KLV PID. `--existing keep` (default) joins an existing synchronous KLV stream as a new metadata service (ST 1402-13); `replace` removes the program's KLV; `fail` stops.
- **Metadata.** `--klv`: recorded ST 0601 packets, written byte for byte at their own tag 2 time; damaged packets and packets without tag 2 are skipped and counted. `--ndjson`: authoring records (the `misb::json` format), each at its own time, or sampled at video frames with `--rate frame`, `every=N` or a rate such as `30hz`, tag 2 being the frame's own time. Both are read incrementally, so the file size does not matter.
- **Anchor.** Where MISP time meets the program clock: `source-start` (the first video frame is at the first sample's time, the default), `first-frame=TIME`, or `pts=PTS@TIME`, with TIME in UTC (converted with the leap second table) or `misp:MICROSECONDS`. `--snap` moves recorded packets' PTS to the nearest frame without touching tag 2.
- **Timing.** Synchronous KLV arrives 990 ms to 1 ms before its PTS, right before its video frame (ISO/IEC 13818-1, MISB ST 1402.2). Samples that would be late are counted, never sent late; `--strict` turns any sample left out into exit status 1.
- **Rate.** Inserted packets add to the bitrate by default. `--cbr` keeps the packet count and every PCR where it was by putting the KLV and table packets in place of null packets, and stops (or with `--drop-on-overflow` drops and counts) when there is no room.
- **Pacing.** A socket output is paced by the PCR like `play` (`--latency`, `--burst`); a file output is written as fast as possible unless `--pace`. A socket input works too (live injection with a file's metadata and an explicit anchor).
- **Interpolation.** With a frame rate, `--interpolate` moves each item between the records that carry it (linear, the short way around for angles, along the great circle for positions, held for text and codes; a record's `"interpolate"` object overrides an item), so a flight log at 1 Hz gives smooth values at every frame.
- **Video timestamps.** `--video-anchor` takes MISP time from the input video's MISB ST 0604 timestamps (H.264, H.265, MPEG-2) when it has them, ahead of `--anchor`. `--video-timestamps add` writes an ST 0604 timestamp into every H.264 or H.265 access unit that lacks one (`rewrite`: every access unit, replacing existing ones), with the anchor's time, so the video and the metadata share one clock; only the video PES that change are re-packetized (`--time-locked` asserts lock in the Time Status, `--nanoseconds` writes the H.265 nanosecond form).

- **M2TS.** A 192-byte M2TS file written to a file stays M2TS: each record keeps its arrival time stamp, and inserted packets get stamps between their neighbours'. Paced or sent to a socket, it goes out as 188-byte TS.

The work is done by the libraries: `misb::mux::FileMux` schedules the samples and the `mpegts` injector places them; tsflow reads the files and sockets.

### live

Puts KLV into a live stream as it passes, with the values coming from a control channel: each video frame the rate admits gets one ST 0601 packet built from the current values, tag 2 being the frame's MISP time and (synchronous) its PTS the frame's PTS, sent right before the frame. A change shows on the next frame.

```bash
tsflow live udp://@:5000 udp://10.0.0.5:5000 --state start.ndjson --control udp://127.0.0.1:6000
ffmpeg -re -i camera.ts -c copy -f mpegts - | tsflow live - udp://239.1.1.1:5000 --control unix:/run/klv.sock --video-timestamps add
echo '{"set": {"SensorLatitude": 38.8977, "PlatformHeadingAngle": 271.5}, "emit": true}' | nc -u -w0 127.0.0.1 6000
```

A test pattern encoded with libx264 (B-frames, 15 fps), with initial values from `--state` and ST 0604 timestamps added, `tsflow live x264.ts out.ts --state state.ndjson --video-timestamps add -q` prints:

```text
tsflow live: listening on x264.ts -> out.ts (sync KLV, ST 0604 in the video)
tsflow live: program 1 (PMT 0x1000), KLV on PID 0x0101, service 0
tsflow live: anchor PTS 138000 = 2026-11-21T21:22:10.412841Z
tsflow live: end of input after 0.0 s; 765 packets in, 894 out; 60 frames, 60 KLV units (0 on request, 0 with nothing set, 0 refused); 0 control lines, 0 refused; ST 0604 timestamps: 0 found, 60 written
```

- **Control lines.** One JSON object per line (the `misb::json` commands): `{"set": {...}}` with keys and values as in `inject --ndjson` records (`null` sends an item as unknown), `{"unknown": [...]}`, `{"clear": [...]}`, `{"clear_all": true}`, `{"version": 19}`, `{"emit": true}` for a packet now. A line is applied whole or not at all, between two packets; a refused line is reported on standard error and changes nothing. They come from `--control -` (standard input), `udp://@:PORT` (one or more lines per datagram), `unix:PATH` (a socket any number of clients write to) or a file; `--state FILE` applies lines before the stream starts.
- **Latency.** The output leaves as the input arrives: nothing is paced or buffered, and the injector extrapolates the program clock instead of waiting for the next PCR. Only `--video-timestamps` holds each frame until its first slice has been read. A file input is read as fast as possible; play it through `tsflow play FILE -` for real time.
- **Anchor.** `wall` (default): the first frame is at the wall-clock time it arrived, again after a clock discontinuity; `first-frame=TIME` or `pts=PTS@TIME` as in `inject`; `--video-anchor` puts the video's ST 0604 timestamps first.
- **Staleness.** `--max-age MS` leaves out (or with `--stale-unknown` marks unknown) any value older than that, so a dead sensor feed is not reported forever.
- **The rest** (`--existing`, `--program`, `--pid`, `--service-id`, `--cbr`, `--psi-interval`) works as in `inject`. `--duration`, `--idle-exit` and `--count` end a run; `--json` prints the summary as JSON.

The work is done by `misb::mux::LiveMux`; tsflow reads the sockets and the control channel and keeps the wall clock.

### Planned

- `tsflow replace`: strip a stream's KLV and write new KLV (for example with a different ST 0102 marking), live, at the stream's pace.

The command line is laid out so each lands as one module in `src/cli/` (see [CONTRIBUTING.md](CONTRIBUTING.md)).

## Endpoints

| Form | Meaning |
|---|---|
| `input.ts`, `file://input.ts` | a file; reading detects 188-byte TS, 192-byte M2TS (prefix removed) and 204-byte records, and resynchronises after damage |
| `-` | standard input or output |
| `udp://host:port` | send to `host:port` (as an input: receive on that local address) |
| `udp://@:port` | receive on `port`, every interface |
| `udp://@239.1.1.1:port` | receive multicast: join the group |
| `rtp://...` | the same with an RTP header (RFC 3550; payload type 33, MP2T, RFC 2250); RTP inputs and raw inputs are told apart per datagram |
| `udp://[ff15::1]:5000` | IPv6, in brackets |

Options follow `?`, joined by `&`:

| Option | Applies to | Meaning |
|---|---|---|
| `ttl=N` | outputs | IP time to live. Multicast defaults to 1, so a stream stays on the local network unless asked |
| `iface=A`, `localaddr=A` | both | IPv4 address of the local interface: multicast join interface, or send interface and source address |
| `pkt_size=N` | both | output: TS bytes per datagram, a multiple of 188 (default 1316); input: largest datagram |
| `buffer_size=N` | both | socket buffer in bytes (input default 8 MiB, as granted by the kernel; raise `net.core.rmem_max` on Linux for more) |
| `source=A` | inputs | source-specific multicast (IGMPv3): accept the group only from sender `A` |
| `reuse=0/1` | inputs | `SO_REUSEADDR`; on by default for multicast so several receivers can share a group |
| `loop=0/1` | outputs | multicast loopback to this host (default on) |
| `ssrc=N`, `pt=N` | RTP outputs | RTP SSRC and payload type (default random SSRC, 33) |

Output RTP headers carry one sequence number per datagram and a 90 kHz timestamp derived from the stream position of the datagram's first packet.

## The library

Everything the commands do is a library function, so other tools can embed it:

| Module | Contents |
|---|---|
| `endpoint` | `Endpoint` parsing and validation per direction |
| `framer` | `Framer`: packet alignment of any byte stream, 188/192/204, resynchronisation |
| `input`, `output` | `Input` (files, stdin, sockets) and `Output` (files, stdout, sockets), and the `Sink` trait |
| `rtp` | RFC 3550 header parsing and writing, `SequenceTracker` |
| `clock` | the `Clock` trait, `SystemClock`, and `ManualClock` for deterministic tests |
| `pacer` | `Schedule` (position to wall clock, re-anchoring) and `Pacer` (queue and datagram grouping) |
| `looping` | `LoopFixer`: PCR discontinuity and continuity counters for repeated passes |
| `play` | `play()`: the paced playback loop, and `probe()` |
| `monitor` | `Monitor`: the analysis behind `recv` and `analyze`, with `Summary` and per-window `WindowStats` |
| `expect` | `Expectation` checks |
| `recv` | `run()`: receive, monitor, forward and record |

```rust
use std::time::Duration;
use tsflow::input::{Input, ReadEvent};
use tsflow::monitor::Monitor;

let mut input = Input::open(&"input.ts".parse()?)?;
let mut monitor = Monitor::new()?;
while let ReadEvent::Data(chunk) = input.read(Duration::ZERO)? {
    monitor.push(chunk.packets, None);
}
monitor.finish();
println!("{}", monitor.summary());
```

The `cli` feature (on by default) adds the binary and its two dependencies, `clap` and `ctrlc`; library users can turn it off.

## Guarantees and limits

- **Memory is bounded.** The framer holds one read (256 KiB) plus a partial packet; the PCR timeline at most 20,000 packets (one PCR interval at over 300 Mbit/s); the pacer queue at most 200,000 packets; per-PID tables at most 8192 entries; distinct KLV markings at most 16 per stream; the `mpegts` demuxer and `misb` decoder buffers are capped by their own configurations. `analyze` of a 677 MB file peaks at 4.5 MB resident.
- **No per-packet allocation** on the packet path: buffers are reused, 188-byte runs are handed out in place.
- **No panics on any input**, checked by mutation tests over the framer, monitor, loop fixer, RTP parser and the endpoint and check parsers; no `unsafe` code (`#![forbid(unsafe_code)]`).
- **Single-threaded loops.** `play` and `recv` each run on one thread with no queues between threads; socket reads time out every 100 ms at most, so Ctrl-C and status lines are never late.
- The monitor measures PAT/PMT repetition in stream time: tables before the first PCR are timed with it.
- `iface=` takes an IPv4 address, not an interface name; IPv6 multicast joins use the default interface.
- A socket input's last packet of each datagram is accepted on its sync byte alone (there is no next packet to look ahead to), so the framer's look-ahead check for stray sync bytes applies within a datagram and within file reads.

## Performance

Release build, synthetic 3 Mbit/s stream (H.264-like video, sync and async KLV), file in the page cache, one core of an Intel Core i9-12900K under WSL2:

| Path | Throughput |
|---|---|
| `tsflow analyze` (framer, PCR timeline, demuxer, KLV decoding) | about 2.6 GB/s, 13.8 M packets/s (677 MB in 0.26 s) |
| `tsflow play --no-pace`, file to file (framer, timeline, loop fixer, buffered writes) | about 1.4 GB/s, 7.5 M packets/s (677 MB in 0.45 to 0.52 s) |
| `tsflow inject --existing replace`, file to file, a recorded `.klv` (framer, timeline, KLV split, scheduler, injector) | about 870 MB/s, 4.6 M packets/s (226 MB, 10 minutes of stream, in 0.26 s; 5 MB resident) |

Reproduce with:

```bash
cargo run --release --example throughput -- 600       # in-process, 226 MB
cargo run --release --example synth -- test.ts 1800    # a 677 MB file, then time the commands
```

## Building

Rust 1.75 or later. `mpegts` and `misb` are pinned by git revision in `Cargo.toml` and move together; `Cargo.lock` pins every crates.io dependency to a version that builds on 1.75.

```bash
cargo build --release          # target/release/tsflow
cargo test
```

## Tests

```bash
cargo test
TSFLOW_FUZZ_ITERATIONS=20000 cargo test --release --test robustness
```

Every test stream is synthetic, generated in code with the `mpegts::mux` writers and `misb::encode` (`tests/common/mod.rs`): PAT/PMT, H.264-like video PES with PTS and PCR, synchronous KLV in metadata access units, asynchronous KLV, null packet stuffing. Unit tests cover endpoint parsing, RTP, the framer (all record sizes, any chunking, damage), the schedule (exact due times, re-anchoring, no drift over a simulated 24 hours), datagram grouping, the loop fixer and the checks. Integration tests play streams on a simulated clock (every datagram leaves exactly when its last packet is due, and the monitor measures that pacing exactly), play and receive over real UDP and RTP sockets on 127.0.0.1 (every byte in order, no CC errors, RTP numbering, looping, forwarding byte for byte), drive the binary (exit statuses, JSON, Ctrl-C), and feed mutated input to everything that parses.

No test measures the real clock. Timing is tested on `ManualClock`; socket and binary tests check data only, send in lockstep with the receiver rather than at real-time pace, and stop on exact packet counts or a process's "listening" line, with real time used only as a hang guard. They pass the same on a loaded laptop and on a slow CI virtual machine.

`cargo run --release --example synth -- test.ts 60` writes such a stream for manual tests, for example `tsflow play test.ts udp://127.0.0.1:5000 --loop` with VLC on `udp://@:5000`.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Copyright 2026 Notley Technologies and Noah Davis.

Licensed under the [Apache License, Version 2.0](LICENSE).
