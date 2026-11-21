# Changelog

All notable changes to this project are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses [semantic versioning](https://semver.org/).

## Unreleased (0.1.0)

### Added

- `tsflow play`: real-time playback paced by the PCR, from a file, standard input or a socket to UDP, RTP, a file or standard output. Configurable latency, packets per datagram (`--burst`, `?pkt_size=`), fallback rate for streams without PCR, PCR PID, `--loop` and `--passes` with loop points a receiver can follow (PCR discontinuity_indicator, continuous continuity counters), and `--no-pace` for copying and format conversion.
- `tsflow recv`: live reception and monitoring with once-a-second status lines and a final summary: continuity errors per PID, PCR intervals and PCR-versus-arrival jitter, PAT and PMT repetition, bitrates, programs and streams, KLV streams decoded with `misb` (packet rate, ST 0601 checksums, ST 0102 security marking, tag 2 time span, synchronous KLV arrival ahead of PTS), RTP loss. `--forward` relays every datagram unchanged to another endpoint; `--out` records; `--idle-exit`, `--duration` and `--count` (an exact number of packets, `RecvConfig::max_packets`) end the run.
- `tsflow analyze`: the same report for a file, from stream time, with bounded memory.
- `tsflow inject`: a recorded `.klv` file or NDJSON authoring records into a stream as synchronous or asynchronous KLV (MISB ST 1402.2), with `misb::mux::FileMux` and the `mpegts` injector: anchors (`source-start`, `first-frame=TIME`, `pts=PTS@TIME`), rates (explicit, every frame, every Nth frame, Hz), `--snap`, existing KLV kept, replaced or refused, `--cbr` with null packet replacement, paced output to sockets, `--strict` and `--json`. Library: `inject` (`KlvFile`, `NdjsonFile`, `InjectStage`) and `play::play_with` with a `Stage` between timing and output.
- `--expect` checks for scripts (`no-cc-errors`, `max-cc-errors`, `min-packets`, `klv-streams`, `klv-marking`, `min-klv-rate`, `klv-checksums`, `max-pcr-interval`, `max-pcr-jitter`, `max-psi-interval`, `no-rtp-loss`), exit status 1 when one fails, and `--json` output.
- Endpoints: files, `-`, `udp://` and `rtp://` with `@` listen forms, multicast join, source-specific multicast, IPv6, and the options `ttl`, `iface`/`localaddr`, `pkt_size`, `buffer_size`, `source`, `reuse`, `loop`, `ssrc`, `pt`.
- Clean shutdown on Ctrl-C with a summary; a second Ctrl-C exits at once.
- Library modules behind every command: `endpoint`, `framer` (188/192/204-byte records, resynchronisation with a look-ahead check), `input`, `output`, `rtp`, `clock` (injectable, with a simulated clock for tests), `pacer`, `looping`, `play`, `monitor`, `expect`, `recv`. The `cli` feature (default) holds the binary's dependencies.
- Examples `synth` (synthetic test streams) and `throughput`.
- Tests on synthetic streams only: unit tests, exact pacing on a simulated clock (including 24 hours without drift), UDP and RTP loopback, the binary's exit statuses and Ctrl-C, and mutation tests for hostile input.
