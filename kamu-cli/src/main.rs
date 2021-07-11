#![feature(backtrace)]

use dill::*;
use kamu::domain::*;
use kamu::infra::utils::docker_client::{ContainerRuntimeConfig, DockerClient};
use kamu::infra::*;
use kamu_cli::commands::*;
use kamu_cli::config;
use kamu_cli::output::*;
use kamu_cli::{cli_parser, config::ConfigScope};
use opendatafabric::DatasetIDBuf;

use clap::value_t_or_exit;
use console::style;
use slog::{info, o, Drain, FnValue, Logger};
use std::backtrace::BacktraceStatus;
use std::error::Error as StdError;
use std::path::Path;

const BINARY_NAME: &str = "kamu";
const VERSION: &str = env!("CARGO_PKG_VERSION");

fn configure_catalog() -> Result<Catalog, InjectionError> {
    let mut catalog = Catalog::new();

    catalog.add::<config::ConfigService>();
    catalog.add::<DockerClient>();

    catalog.add::<MetadataRepositoryImpl>();
    catalog.bind::<dyn MetadataRepository, MetadataRepositoryImpl>()?;

    catalog.add::<ResourceLoaderImpl>();
    catalog.bind::<dyn ResourceLoader, ResourceLoaderImpl>()?;

    catalog.add::<IngestServiceImpl>();
    catalog.bind::<dyn IngestService, IngestServiceImpl>()?;

    catalog.add::<TransformServiceImpl>();
    catalog.bind::<dyn TransformService, TransformServiceImpl>()?;

    catalog.add::<PullServiceImpl>();
    catalog.bind::<dyn PullService, PullServiceImpl>()?;

    catalog.add::<SyncServiceImpl>();
    catalog.bind::<dyn SyncService, SyncServiceImpl>()?;

    catalog.add::<RemoteFactory>();
    catalog.add::<EngineFactory>();

    Ok(catalog)
}

fn load_config(catalog: &mut Catalog) {
    let config_svc = catalog.get_one::<config::ConfigService>().unwrap();
    let config = config_svc.load_with_defaults(ConfigScope::Flattened);

    catalog.add_value(ContainerRuntimeConfig {
        runtime: config.engine.as_ref().unwrap().runtime.unwrap(),
        network_ns: config.engine.as_ref().unwrap().network_ns.unwrap(),
    });
}

fn main() {
    let mut catalog = configure_catalog().unwrap();

    let matches = cli_parser::cli(BINARY_NAME, VERSION).get_matches();
    let output_format = configure_output_format(&matches);

    let workspace_layout = find_workspace();
    let local_volume_layout = VolumeLayout::new(&workspace_layout.local_volume_dir);

    catalog.add_value(workspace_layout.clone());
    catalog.add_value(local_volume_layout.clone());
    catalog.add_value(output_format.clone());

    // Cleanup run info dir
    if workspace_layout.run_info_dir.exists() {
        std::fs::remove_dir_all(&workspace_layout.run_info_dir).unwrap();
        std::fs::create_dir(&workspace_layout.run_info_dir).unwrap();
    }

    let logger = configure_logging(&output_format, &workspace_layout);
    let logger_moved = logger.clone();
    catalog.add_factory(move || logger_moved.new(o!()));

    load_config(&mut catalog);

    let mut command: Box<dyn Command> = match matches.subcommand() {
        ("add", Some(submatches)) => Box::new(AddCommand::new(
            catalog.get_one().unwrap(),
            catalog.get_one().unwrap(),
            submatches.values_of("manifest").unwrap(),
            submatches.is_present("recursive"),
            submatches.is_present("replace"),
        )),
        ("complete", Some(submatches)) => Box::new(CompleteCommand::new(
            if in_workspace(&workspace_layout) {
                Some(catalog.get_one().unwrap())
            } else {
                None
            },
            catalog.get_one().unwrap(),
            cli_parser::cli(BINARY_NAME, VERSION),
            submatches.value_of("input").unwrap().into(),
            submatches.value_of("current").unwrap().parse().unwrap(),
        )),
        ("completions", Some(submatches)) => Box::new(CompletionsCommand::new(
            cli_parser::cli(BINARY_NAME, VERSION),
            value_t_or_exit!(submatches.value_of("shell"), clap::Shell),
        )),
        ("config", Some(config_matches)) => match config_matches.subcommand() {
            ("list", Some(list_matches)) => Box::new(ConfigListCommand::new(
                catalog.get_one().unwrap(),
                list_matches.is_present("user"),
                list_matches.is_present("with-defaults"),
            )),
            ("get", Some(get_matches)) => Box::new(ConfigGetCommand::new(
                catalog.get_one().unwrap(),
                get_matches.is_present("user"),
                get_matches.is_present("with-defaults"),
                get_matches.value_of("cfgkey").unwrap().to_owned(),
            )),
            ("set", Some(set_matches)) => Box::new(ConfigSetCommand::new(
                catalog.get_one().unwrap(),
                set_matches.is_present("user"),
                set_matches.value_of("cfgkey").unwrap().to_owned(),
                set_matches.value_of("value").map(|s| s.to_owned()),
            )),
            _ => unimplemented!(),
        },
        ("delete", Some(submatches)) => Box::new(DeleteCommand::new(
            catalog.get_one().unwrap(),
            submatches.values_of("dataset").unwrap_or_default(),
            submatches.is_present("all"),
            submatches.is_present("recursive"),
            submatches.is_present("yes"),
        )),
        ("init", Some(submatches)) => {
            if submatches.is_present("pull-images") {
                Box::new(PullImagesCommand::new(catalog.get_one().unwrap(), false))
            } else if submatches.is_present("pull-test-images") {
                Box::new(PullImagesCommand::new(catalog.get_one().unwrap(), true))
            } else {
                Box::new(InitCommand::new(&workspace_layout))
            }
        }
        ("list", Some(submatches)) => match submatches.subcommand() {
            ("", _) => Box::new(ListCommand::new(
                catalog.get_one().unwrap(),
                catalog.get_one().unwrap(),
            )),
            ("depgraph", _) => Box::new(DepgraphCommand::new(catalog.get_one().unwrap())),
            _ => unimplemented!(),
        },
        ("log", Some(submatches)) => Box::new(LogCommand::new(
            catalog.get_one().unwrap(),
            value_t_or_exit!(submatches.value_of("dataset"), DatasetIDBuf),
            output_format.clone(),
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
            catalog.get_one().unwrap(),
            submatches.values_of("env").unwrap_or_default(),
            logger.new(o!()),
        )),
        ("pull", Some(submatches)) => {
            if submatches.is_present("set-watermark") {
                let datasets = submatches.values_of("dataset").unwrap_or_default();
                if datasets.len() != 1 {}
                Box::new(SetWatermarkCommand::new(
                    catalog.get_one().unwrap(),
                    submatches.values_of("dataset").unwrap_or_default(),
                    submatches.is_present("all"),
                    submatches.is_present("recursive"),
                    submatches.value_of("set-watermark").unwrap(),
                ))
            } else if submatches.is_present("remote") {
                Box::new(SyncFromCommand::new(
                    catalog.get_one().unwrap(),
                    submatches.values_of("dataset").unwrap_or_default(),
                    submatches.value_of("remote"),
                    &output_format,
                ))
            } else {
                Box::new(PullCommand::new(
                    catalog.get_one().unwrap(),
                    submatches.values_of("dataset").unwrap_or_default(),
                    submatches.is_present("all"),
                    submatches.is_present("recursive"),
                    submatches.is_present("force-uncacheable"),
                    &output_format,
                ))
            }
        }
        ("push", Some(push_matches)) => Box::new(PushCommand::new(
            catalog.get_one().unwrap(),
            push_matches.values_of("dataset").unwrap_or_default(),
            push_matches.value_of("remote"),
            &output_format,
        )),
        ("remote", Some(remote_matches)) => match remote_matches.subcommand() {
            ("add", Some(add_matches)) => Box::new(RemoteAddCommand::new(
                catalog.get_one().unwrap(),
                add_matches.value_of("name").unwrap(),
                add_matches.value_of("url").unwrap(),
            )),
            ("delete", Some(delete_matches)) => Box::new(RemoteDeleteCommand::new(
                catalog.get_one().unwrap(),
                delete_matches.values_of("remote").unwrap_or_default(),
                delete_matches.is_present("all"),
                delete_matches.is_present("yes"),
            )),
            ("list", _) => Box::new(RemoteListCommand::new(
                catalog.get_one().unwrap(),
                &output_format,
            )),
            _ => unimplemented!(),
        },
        ("sql", Some(submatches)) => match submatches.subcommand() {
            ("", None) => Box::new(SqlShellCommand::new(
                &workspace_layout,
                &local_volume_layout,
                &output_format,
                catalog.get_one().unwrap(),
                submatches.value_of("command"),
                submatches.value_of("url"),
                logger.new(o!()),
            )),
            ("server", Some(server_matches)) => Box::new(SqlServerCommand::new(
                &workspace_layout,
                &local_volume_layout,
                &output_format,
                catalog.get_one().unwrap(),
                logger.new(o!()),
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
