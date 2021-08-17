use std::path::Path;
use std::sync::Arc;

use dill::*;
use kamu::domain::*;
use kamu::infra::utils::docker_client::*;
use kamu::infra::*;
use slog::Drain;
use slog::{info, FnValue};
use slog::{o, Logger};

use crate::cli_commands;
use crate::commands::Command;
use crate::config::*;
use crate::error::*;
use crate::output::*;

/////////////////////////////////////////////////////////////////////////////////////////

pub const BINARY_NAME: &str = "kamu";
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/////////////////////////////////////////////////////////////////////////////////////////

pub fn run(
    workspace_layout: WorkspaceLayout,
    local_volume_layout: VolumeLayout,
    matches: clap::ArgMatches,
) -> Result<(), CLIError> {
    // Cleanup run info dir
    if workspace_layout.run_info_dir.exists() {
        std::fs::remove_dir_all(&workspace_layout.run_info_dir).unwrap();
        std::fs::create_dir(&workspace_layout.run_info_dir).unwrap();
    }

    let mut catalog = configure_catalog().unwrap();
    catalog.add_value(workspace_layout.clone());
    catalog.add_value(local_volume_layout.clone());

    let output_format = configure_output_format(&matches);
    catalog.add_value(output_format.clone());

    let logger = configure_logging(&output_format, &workspace_layout);
    let logger_moved = logger.clone();
    catalog.add_factory(move || logger_moved.new(o!()));

    load_config(&mut catalog);

    let mut command: Box<dyn Command> = cli_commands::get_command(&catalog, matches)?;

    if command.needs_workspace() && !in_workspace(catalog.get_one().unwrap()) {
        Err(CLIError::usage_error_from(NotInWorkspace))
    } else {
        command.run()
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// Catalog
/////////////////////////////////////////////////////////////////////////////////////////

fn configure_catalog() -> Result<Catalog, InjectionError> {
    let mut catalog = Catalog::new();

    catalog.add::<ConfigService>();
    catalog.add::<DockerClient>();

    catalog.add::<MetadataRepositoryImpl>();
    catalog.bind::<dyn MetadataRepository, MetadataRepositoryImpl>()?;

    catalog.add::<ResourceLoaderImpl>();
    catalog.bind::<dyn ResourceLoader, ResourceLoaderImpl>()?;

    catalog.add::<IngestServiceImpl>();
    catalog.bind::<dyn IngestService, IngestServiceImpl>()?;

    catalog.add::<TransformServiceImpl>();
    catalog.bind::<dyn TransformService, TransformServiceImpl>()?;

    catalog.add::<SearchServiceImpl>();
    catalog.bind::<dyn SearchService, SearchServiceImpl>()?;

    catalog.add::<SyncServiceImpl>();
    catalog.bind::<dyn SyncService, SyncServiceImpl>()?;

    catalog.add::<PullServiceImpl>();
    catalog.bind::<dyn PullService, PullServiceImpl>()?;

    catalog.add::<PushServiceImpl>();
    catalog.bind::<dyn PushService, PushServiceImpl>()?;

    catalog.add::<RepositoryFactory>();

    catalog.add::<EngineFactoryImpl>();
    catalog.bind::<dyn EngineFactory, EngineFactoryImpl>()?;

    Ok(catalog)
}

/////////////////////////////////////////////////////////////////////////////////////////
// Config
/////////////////////////////////////////////////////////////////////////////////////////

fn load_config(catalog: &mut Catalog) {
    let config_svc = catalog.get_one::<ConfigService>().unwrap();
    let config = config_svc.load_with_defaults(ConfigScope::Flattened);

    catalog.add_value(ContainerRuntimeConfig {
        runtime: config.engine.as_ref().unwrap().runtime.unwrap(),
        network_ns: config.engine.as_ref().unwrap().network_ns.unwrap(),
    });
}

/////////////////////////////////////////////////////////////////////////////////////////
// Workspace
/////////////////////////////////////////////////////////////////////////////////////////

pub fn find_workspace() -> WorkspaceLayout {
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

pub(crate) fn in_workspace(workspace_layout: Arc<WorkspaceLayout>) -> bool {
    workspace_layout.kamu_root_dir.is_dir()
}

/////////////////////////////////////////////////////////////////////////////////////////
// Logging
/////////////////////////////////////////////////////////////////////////////////////////

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

/////////////////////////////////////////////////////////////////////////////////////////
// Output format
/////////////////////////////////////////////////////////////////////////////////////////

fn configure_output_format(matches: &clap::ArgMatches<'_>) -> OutputConfig {
    let is_tty = console::Term::stdout().features().is_attended();

    let verbosity_level = matches.occurrences_of("verbose") as u8;
    if verbosity_level > 0 {
        std::env::set_var("RUST_BACKTRACE", "1");
    }

    let quiet = matches.is_present("quiet");

    let format_str = get_output_format_recursive(matches);

    let format = match format_str {
        Some("csv") => OutputFormat::Csv,
        Some("json") => OutputFormat::Json,
        Some("table") => OutputFormat::Table,
        None | Some(_) => {
            if is_tty {
                OutputFormat::Table
            } else {
                OutputFormat::Csv
            }
        }
    };

    OutputConfig {
        quiet,
        verbosity_level: verbosity_level,
        is_tty: is_tty,
        format: format,
    }
}

fn get_output_format_recursive<'a>(matches: &'a clap::ArgMatches<'_>) -> Option<&'a str> {
    if let (_, Some(submatches)) = matches.subcommand() {
        if let Some(fmt) = submatches.value_of("output-format") {
            Some(fmt)
        } else {
            get_output_format_recursive(submatches)
        }
    } else {
        None
    }
}
