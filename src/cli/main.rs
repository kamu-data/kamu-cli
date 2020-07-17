#![feature(backtrace)]

mod cli_parser;
mod commands;

use commands::*;
use kamu::domain::*;
use kamu::infra::*;

use clap::value_t_or_exit;
use console::style;
use std::backtrace::BacktraceStatus;
use std::error::Error;
use std::path::PathBuf;

fn main() {
    let kamu_root = PathBuf::from(".kamu");

    let workspace_layout = WorkspaceLayout {
        metadata_dir: kamu_root.join("datasets"),
        remotes_dir: kamu_root.join("remotes"),
        local_volume_dir: PathBuf::from(".kamu.local"),
    };

    let metadata_repo = MetadataRepositoryFs::new(workspace_layout.clone());

    let matches = cli_parser::cli().get_matches();

    // Verboseness
    match matches.occurrences_of("v") {
        0 => (),
        _ => std::env::set_var("RUST_BACKTRACE", "1"),
    };

    let mut command: Box<dyn Command> = match matches.subcommand() {
        ("list", Some(_)) => Box::new(ListCommand::new(&metadata_repo)),
        ("log", Some(submatches)) => Box::new(LogCommand::new(
            &metadata_repo,
            value_t_or_exit!(submatches.value_of("dataset"), DatasetIDBuf),
        )),
        ("pull", Some(_)) => Box::new(PullCommand::new()),
        ("sql", Some(submatches)) => match submatches.subcommand() {
            ("", None) => Box::new(SqlShellCommand::new()),
            ("server", Some(server_matches)) => Box::new(SqlServerCommand::new(
                server_matches.value_of("address").unwrap(),
                value_t_or_exit!(server_matches.value_of("port"), u16),
            )),
            _ => panic!("Unrecognized command"),
        },
        ("completions", Some(submatches)) => {
            let shell = value_t_or_exit!(submatches.value_of("shell"), clap::Shell);
            cli_parser::cli().gen_completions_to("kamu-rs", shell, &mut std::io::stdout());
            Box::new(NoOpCommand)
        }
        _ => panic!("Unrecognized command"),
    };

    let ok = if command.needs_workspace() && !workspace_layout.metadata_dir.exists() {
        // TODO: Make into error
        eprintln!("Error: Not a kamu workspace");
        false
    } else {
        match command.run() {
            Ok(_) => true,
            Err(err) => {
                // TODO: Missing colors
                eprintln!("[{}] {}", style("ERROR").red(), err);
                if let Some(bt) = err.backtrace() {
                    if bt.status() == BacktraceStatus::Captured {
                        eprintln!("\nBacktrace:\n{}", style(bt).dim());
                    }
                }
                false
            }
        }
    };

    if !ok {
        std::process::exit(1);
    }
}
