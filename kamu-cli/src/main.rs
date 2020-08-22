#![feature(backtrace)]

use kamu::domain::*;
use kamu::infra::*;
use kamu_cli::cli_parser;
use kamu_cli::commands::*;
use kamu_cli::output::*;

use clap::value_t_or_exit;
use console::style;
use slog::{info, o, Drain, FnValue, Logger};
use std::backtrace::BacktraceStatus;
use std::cell::RefCell;
use std::error::Error as StdError;
use std::path::Path;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

const BINARY_NAME: &str = "kamu";
const VERSION: &str = env!("CARGO_PKG_VERSION");

fn main() {
    let matches = cli_parser::cli(BINARY_NAME, VERSION).get_matches();
    let output_format = configure_output_format(&matches);

    let workspace_layout = find_workspace();
    let local_volume_layout = VolumeLayout::new(&workspace_layout.local_volume_dir);

    // Cleanup run info dir
    if workspace_layout.run_info_dir.exists() {
        std::fs::remove_dir_all(&workspace_layout.run_info_dir).unwrap();
        std::fs::create_dir(&workspace_layout.run_info_dir).unwrap();
    }

    let logger = configure_logging(&output_format, &workspace_layout);

    let metadata_repo = Rc::new(RefCell::new(MetadataRepositoryImpl::new(&workspace_layout)));
    let resource_loader = Rc::new(RefCell::new(ResourceLoaderImpl::new()));
    let engine_factory = Arc::new(Mutex::new(EngineFactory::new(&workspace_layout)));
    let ingest_svc = Rc::new(RefCell::new(IngestServiceImpl::new(
        metadata_repo.clone(),
        engine_factory.clone(),
        &local_volume_layout,
        logger.new(o!()),
    )));
    let transform_svc = Rc::new(RefCell::new(TransformServiceImpl::new(
        metadata_repo.clone(),
        engine_factory.clone(),
        &local_volume_layout,
        logger.new(o!()),
    )));
    let pull_svc = Rc::new(RefCell::new(PullServiceImpl::new(
        metadata_repo.clone(),
        ingest_svc.clone(),
        transform_svc.clone(),
        logger.new(o!()),
    )));

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
        ("delete", Some(submatches)) => Box::new(DeleteCommand::new(
            metadata_repo.clone(),
            submatches.values_of("dataset").unwrap_or_default(),
            submatches.is_present("all"),
            submatches.is_present("recursive"),
            submatches.is_present("yes"),
        )),
        ("init", Some(_)) => Box::new(InitCommand::new(&workspace_layout)),
        ("list", Some(submatches)) => match submatches.subcommand() {
            ("", None) => Box::new(ListCommand::new(metadata_repo.clone(), &output_format)),
            ("depgraph", _) => {
                Box::new(DepgraphCommand::new(metadata_repo.clone(), &output_format))
            }
            _ => unimplemented!(),
        },
        ("log", Some(submatches)) => Box::new(LogCommand::new(
            metadata_repo.clone(),
            value_t_or_exit!(submatches.value_of("dataset"), DatasetIDBuf),
        )),
        ("notebook", Some(submatches)) => Box::new(NotebookCommand::new(
            &workspace_layout,
            &local_volume_layout,
            &output_format,
            submatches.values_of("env").unwrap_or_default(),
        )),
        ("pull", Some(submatches)) => Box::new(PullCommand::new(
            pull_svc.clone(),
            submatches.values_of("dataset").unwrap_or_default(),
            submatches.is_present("all"),
            submatches.is_present("recursive"),
            &output_format,
        )),
        ("sql", Some(submatches)) => match submatches.subcommand() {
            ("", None) => Box::new(SqlShellCommand::new(
                &workspace_layout,
                &local_volume_layout,
                &output_format,
                logger.new(o!()),
            )),
            ("server", Some(server_matches)) => Box::new(SqlServerCommand::new(
                server_matches.value_of("address").unwrap(),
                value_t_or_exit!(server_matches.value_of("port"), u16),
            )),
            _ => unimplemented!(),
        },
        _ => unimplemented!(),
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
    let cwd = Path::new(".").canonicalize().unwrap();
    if let Some(ws) = find_workspace_rec(&cwd) {
        ws
    } else {
        WorkspaceLayout::new(&cwd)
    }
}

fn find_workspace_rec(p: &Path) -> Option<WorkspaceLayout> {
    let root_dir = p.join(".kamu");
    if root_dir.exists() {
        Some(WorkspaceLayout::new(p))
    } else if let Some(parent) = p.parent() {
        find_workspace_rec(parent)
    } else {
        None
    }
}

fn configure_logging(output_format: &OutputFormat, workspace_layout: &WorkspaceLayout) -> Logger {
    let raw_logger = if output_format.verbosity_level > 0 {
        // Log into stderr for verbose output
        let decorator = slog_term::PlainSyncDecorator::new(std::io::stderr());
        let drain = slog_term::CompactFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        Logger::root(drain, o!())
    } else if workspace_layout.run_info_dir.exists() {
        // Log to file if workspace exists
        let log_path = workspace_layout.run_info_dir.join("kamu.log");
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&log_path)
            .unwrap_or_else(|e| {
                panic!("Failed to create log file at {}: {}", log_path.display(), e)
            });

        let decorator = slog_term::PlainSyncDecorator::new(file);
        let drain = slog_term::CompactFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        Logger::root(drain, o!())
    } else {
        // Discard otherwise
        let drain = slog::Discard.fuse();
        Logger::root(drain, o!())
    };

    let logger = raw_logger.new(o!("version" => VERSION));

    info!(logger, "Running with arguments"; "args" => FnValue(|_| {
         let v: Vec<_> = std::env::args().collect();
         format!("{:?}", v)
    }));

    logger
}

fn configure_output_format(matches: &clap::ArgMatches<'_>) -> OutputFormat {
    let verbosity_level = matches.occurrences_of("v") as u8;

    if verbosity_level > 0 {
        std::env::set_var("RUST_BACKTRACE", "1");
    }

    OutputFormat {
        verbosity_level: verbosity_level,
        is_tty: console::Term::stdout().is_term(),
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
