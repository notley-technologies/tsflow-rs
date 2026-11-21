# Contributing

Thanks for taking the time to help. Bug reports, fixes, new checks, new subcommands and documentation improvements are all welcome.

## Reporting problems

Open an issue. The most useful reports include:

- the version or commit you are using, and the operating system,
- the exact command line, and what it printed,
- what you expected instead.

Do not attach recordings of real streams. Describe the stream (packet size, programs, codecs, bitrate, KLV carriage) or, better, reproduce the problem with a synthetic one: `cargo run --example synth -- test.ts 10` writes one, and `tests/common/mod.rs` builds variations in code.

## Development

You need Rust 1.75 or later (the minimum is in `Cargo.toml`; `Cargo.lock` keeps every dependency on versions that build with it).

```bash
cargo test
```

This runs the unit and integration tests, including the loopback tests on real UDP and RTP sockets (127.0.0.1 only) and the command line tests. CI runs the same on Linux, macOS and Windows, plus formatting, Clippy, documentation with warnings as errors, and a check on Rust 1.75.

Before opening a pull request:

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo clippy --all-targets --no-default-features -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
TSFLOW_FUZZ_ITERATIONS=20000 cargo test --release --test robustness
```

For changes to the packet path, run the throughput example before and after and include the numbers in the pull request:

```bash
cargo run --release --example throughput -- 600
```

## Adding a subcommand

1. Put the work in the library: a module with a function that takes an `Input`, a `Sink` or `Output`, a `&dyn Clock` and a stop flag, and returns its statistics. Keep it on one thread unless there is a reason not to.
2. Add `src/cli/<name>.rs` with an `Args` struct deriving `clap::Args` (with examples in `after_help`) and `run(args, &Context) -> Result<ExitCode, Error>`: parse, print, pick the exit status, nothing else.
3. Add a variant to `Command` in `src/cli/mod.rs` and a line to its match.
4. Test the library function with `ManualClock` and synthetic streams, and the command through the binary in `tests/cli.rs`.
5. Document it in the README and the changelog.

## Guidelines

- **Tests.** Fixes come with a test that fails without the fix. Test data is synthetic: build it with `tests/common/mod.rs`, never from captured streams.
- **No wall clock in tests.** Anything timed is tested through `Clock` with `ManualClock` (like .NET's `TimeProvider`), never by sleeping, waiting for "long enough", or asserting on measured durations, jitter or speed. Tests on real sockets or the binary check data, synchronise on events (a "listening" line, an exact `--count`, a receiver's progress) and use real time only as a hang guard that fails the test. Speed belongs in benchmarks and examples, not assertions.
- **I/O lives here.** `mpegts` and `misb` never do I/O or read clocks; when something can be done without I/O, it belongs in one of them. Everything here that reads the time goes through `Clock`, so it can be tested without waiting.
- **Bounded memory.** Anything that buffers data has a fixed limit, documented where it is kept. Hostile input must not be able to grow memory.
- **No panics, no `unsafe`.** Damaged input is skipped and counted, never unwrapped. Arithmetic on values from the stream saturates or is checked.
- **Hot path.** No allocation per packet; reuse buffers, hand out slices.
- **Errors.** Say what was being done and to which endpoint, so an operator can act on the message alone.
- **Standards.** Cite the document and section in a comment for every rule implemented (for example ISO/IEC 13818-1 2.4.3.5, RFC 3550 A.1, MISB ST 1402).
- **Style.** `cargo fmt` (see `rustfmt.toml`). Comments explain why, not what. Use hyphens, not em or en dashes.

## Pull requests

Keep pull requests focused on one change and describe what it does and why. Add an entry under "Unreleased" in [CHANGELOG.md](CHANGELOG.md) for anything user-visible.

## License

By contributing, you agree that your contributions are licensed under the [Apache License 2.0](LICENSE), as described in section 5 of the license.
