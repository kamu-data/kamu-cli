#![feature(backtrace)]

use kamu::infra::*;
use kamu_cli::cli_parser;
use kamu_cli::commands::*;
use kamu_cli::output::*;
use opendatafabric::DatasetIDBuf;

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
    let engine_factory = Arc::new(Mutex::new(EngineFactory::new(
        &workspace_layout,
        logger.new(o!()),
    )));
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
    let remote_factory = Arc::new(Mutex::new(RemoteFactory::new(logger.new(o!()))));
    let sync_svc = Rc::new(RefCell::new(SyncServiceImpl::new(
        workspace_layout.clone(),
        metadata_repo.clone(),
        remote_factory.clone(),
        logger.new(o!()),
    )));

    let mut command: Box<dyn Command> = match matches.subcommand() {
        ("add", Some(submatches)) => Box::new(AddCommand::new(
            resource_loader.clone(),
            metadata_repo.clone(),
            submatches.values_of("manifest").unwrap(),
            submatches.is_present("recursive"),
        )),
        ("complete", Some(submatches)) => Box::new(CompleteCommand::new(
            if in_workspace(&workspace_layout) {
                Some(metadata_repo.clone())
            } else {
                None
            },
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
        ("init", Some(submatches)) => {
            if !submatches.is_present("pull-images") {
                Box::new(InitCommand::new(&workspace_layout))
            } else {
                Box::new(PullImagesCommand::new())
            }
        }
        ("list", Some(submatches)) => match submatches.subcommand() {
            ("", _) => Box::new(ListCommand::new(metadata_repo.clone(), &output_format)),
            ("depgraph", _) => Box::new(DepgraphCommand::new(metadata_repo.clone())),
            _ => unimplemented!(),
        },
        ("log", Some(submatches)) => Box::new(LogCommand::new(
            metadata_repo.clone(),
            value_t_or_exit!(submatches.value_of("dataset"), DatasetIDBuf),
        )),
        ("new", Some(submatches)) => Box::new(NewDatasetCommand::new(
            submatches.value_of("id").unwrap(),
            submatches.is_present("root"),
            submatches.is_present("derivative"),
            None::<&str>,
        )),
        ("notebook", Some(submatches)) => Box::new(NotebookCommand::new(
            &workspace_layout,
            &local_volume_layout,
            &output_format,
            submatches.values_of("env").unwrap_or_default(),
            logger.new(o!()),
        )),
        ("pull", Some(submatches)) => {
            if submatches.is_present("set-watermark") {
                let datasets = submatches.values_of("dataset").unwrap_or_default();
                if datasets.len() != 1 {}
                Box::new(SetWatermarkCommand::new(
                    pull_svc.clone(),
                    submatches.values_of("dataset").unwrap_or_default(),
                    submatches.is_present("all"),
                    submatches.is_present("recursive"),
                    submatches.value_of("set-watermark").unwrap(),
                ))
            } else if submatches.is_present("remote") {
                Box::new(SyncFromCommand::new(
                    sync_svc.clone(),
                    submatches.values_of("dataset").unwrap_or_default(),
                    submatches.value_of("remote"),
                    &output_format,
                ))
            } else {
                Box::new(PullCommand::new(
                    pull_svc.clone(),
                    submatches.values_of("dataset").unwrap_or_default(),
                    submatches.is_present("all"),
                    submatches.is_present("recursive"),
                    submatches.is_present("force-uncacheable"),
                    &output_format,
                ))
            }
        }
        ("push", Some(push_matches)) => Box::new(PushCommand::new(
            sync_svc.clone(),
            push_matches.values_of("dataset").unwrap_or_default(),
            push_matches.value_of("remote"),
            &output_format,
        )),
        ("remote", Some(remote_matches)) => match remote_matches.subcommand() {
            ("add", Some(add_matches)) => Box::new(RemoteAddCommand::new(
                metadata_repo.clone(),
                add_matches.value_of("name").unwrap(),
                add_matches.value_of("url").unwrap(),
            )),
            ("delete", Some(delete_matches)) => Box::new(RemoteDeleteCommand::new(
                metadata_repo.clone(),
                delete_matches.values_of("remote").unwrap_or_default(),
                delete_matches.is_present("all"),
                delete_matches.is_present("yes"),
            )),
            ("list", _) => Box::new(RemoteListCommand::new(
                metadata_repo.clone(),
                &output_format,
            )),
            _ => unimplemented!(),
        },
        ("sql", Some(submatches)) => match submatches.subcommand() {
            ("", None) => Box::new(SqlShellCommand::new(
                &workspace_layout,
                &local_volume_layout,
                &output_format,
                submatches.value_of("command"),
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

    let result = if command.needs_workspace() && !in_workspace(&workspace_layout) {
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

fn in_workspace(workspace_layout: &WorkspaceLayout) -> bool {
    workspace_layout.kamu_root_dir.is_dir()
}

fn configure_logging(output_config: &OutputConfig, workspace_layout: &WorkspaceLayout) -> Logger {
    let raw_logger = if output_config.verbosity_level > 0 {
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

    info!(logger, "Initializing"; "args" => FnValue(|_| {
         let v: Vec<_> = std::env::args().collect();
         format!("{:?}", v)
    }), "workspace" => ?workspace_layout);

    logger
}

fn configure_output_format(matches: &clap::ArgMatches<'_>) -> OutputConfig {
    let is_tty = console::Term::stdout().features().is_attended();

    let verbosity_level = matches.occurrences_of("v") as u8;
    if verbosity_level > 0 {
        std::env::set_var("RUST_BACKTRACE", "1");
    }

    let format_str = if let (_, Some(submatches)) = matches.subcommand() {
        submatches.value_of("output-format")
    } else {
        None
    };

    let format = match format_str {
        Some("csv") => OutputFormat::Csv,
        Some("json") => OutputFormat::Json,
        Some("table") => OutputFormat::Table,
        Some(f) => unimplemented!("Unrecognized format: {:?}", f),
        None => {
            if is_tty {
                OutputFormat::Table
            } else {
                OutputFormat::Csv
            }
        }
    };

    OutputConfig {
        verbosity_level: verbosity_level,
        is_tty: is_tty,
        format: format,
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
