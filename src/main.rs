//! The `tsflow` command line. Each subcommand is a module of `cli`; see
//! `cli/mod.rs` for how to add one.

mod cli;

fn main() -> std::process::ExitCode {
    cli::main()
}
