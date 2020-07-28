#![feature(backtrace)]

use kamu::domain::*;
use kamu::infra::*;
use kamu_cli::cli_parser;
use kamu_cli::commands::*;
use kamu_cli::error::*;

use clap::value_t_or_exit;
use console::style;
use std::backtrace::BacktraceStatus;
use std::cell::RefCell;
use std::error::Error as StdError;
use std::path::PathBuf;
use std::rc::Rc;

const BINARY_NAME: &str = "kamu-rs";
const VERSION: &str = "0.0.1";

fn main() {
    let workspace_layout = find_workspace();
    let metadata_repo = Rc::new(RefCell::new(MetadataRepositoryImpl::new(&workspace_layout)));
    let resource_loader = Rc::new(RefCell::new(ResourceLoaderImpl::new()));
    let ingest_svc = Rc::new(RefCell::new(IngestServiceImpl::new(metadata_repo.clone())));
    let transform_svc = Rc::new(RefCell::new(TransformServiceImpl::new(
        metadata_repo.clone(),
    )));
    let pull_svc = Rc::new(RefCell::new(PullServiceImpl::new(
        metadata_repo.clone(),
        ingest_svc.clone(),
        transform_svc.clone(),
    )));

    let matches = cli_parser::cli(BINARY_NAME, VERSION).get_matches();

    // Verboseness
    match matches.occurrences_of("v") {
        0 => (),
        _ => std::env::set_var("RUST_BACKTRACE", "1"),
    };

    let mut command: Box<dyn Command> = match matches.subcommand() {
        ("add", Some(submatches)) => Box::new(AddCommand::new(
            resource_loader.clone(),
            metadata_repo.clone(),
            submatches.values_of("snapshot").unwrap(),
            submatches.is_present("recursive"),
        )),
        ("complete", Some(submatches)) => Box::new(CompleteCommand::new(
            metadata_repo.clone(),
            cli_parser::cli(BINARY_NAME, VERSION),
            submatches.value_of("input").unwrap().into(),
            submatches.value_of("current").unwrap().parse().unwrap(),
        )),
        ("completions", Some(submatches)) => Box::new(CompletionsCommand::new(
            cli_parser::cli(BINARY_NAME, VERSION),
            value_t_or_exit!(submatches.value_of("shell"), clap::Shell),
        )),
        ("init", Some(_)) => Box::new(InitCommand::new(&workspace_layout)),
        ("list", Some(_)) => Box::new(ListCommand::new(metadata_repo.clone())),
        ("log", Some(submatches)) => Box::new(LogCommand::new(
            metadata_repo.clone(),
            value_t_or_exit!(submatches.value_of("dataset"), DatasetIDBuf),
        )),
        ("pull", Some(submatches)) => Box::new(PullCommand::new(
            pull_svc.clone(),
            submatches.values_of("dataset").unwrap_or_default(),
            submatches.is_present("all"),
            submatches.is_present("recursive"),
        )),
        ("sql", Some(submatches)) => match submatches.subcommand() {
            ("", None) => Box::new(SqlShellCommand::new()),
            ("server", Some(server_matches)) => Box::new(SqlServerCommand::new(
                server_matches.value_of("address").unwrap(),
                value_t_or_exit!(server_matches.value_of("port"), u16),
            )),
            _ => panic!("Unrecognized command"),
        },
        _ => panic!("Unrecognized command"),
    };

    let result = if command.needs_workspace() && !workspace_layout.kamu_root_dir.is_dir() {
        Err(Error::NotInWorkspace)
    } else {
        command.run()
    };

    match result {
        Ok(_) => (),
        Err(err) => {
            display_error(err);
            std::process::exit(1);
        }
    }
}

fn find_workspace() -> WorkspaceLayout {
    let kamu_root_dir = PathBuf::from(".kamu");
    WorkspaceLayout {
        kamu_root_dir: kamu_root_dir.clone(),
        datasets_dir: kamu_root_dir.join("datasets"),
        remotes_dir: kamu_root_dir.join("remotes"),
        local_volume_dir: PathBuf::from(".kamu.local"),
    }
}

fn display_error(err: Error) {
    eprintln!("{}: {}", style("Error").red().bold(), err);
    if let Some(bt) = err.backtrace() {
        if bt.status() == BacktraceStatus::Captured {
            eprintln!("\nBacktrace:\n{}", style(bt).dim().bold());
        }
    }
}
