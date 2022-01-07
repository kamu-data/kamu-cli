// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;
use std::sync::Arc;

use container_runtime::{ContainerRuntime, ContainerRuntimeConfig};
use dill::*;
use kamu::domain::*;
use kamu::infra::*;
use tracing::error;
use tracing::info;

use crate::cli_commands;
use crate::commands::Command;
use crate::config::*;
use crate::error::*;
use crate::output::*;

/////////////////////////////////////////////////////////////////////////////////////////

pub const BINARY_NAME: &str = "kamu";
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

const DEFAULT_LOGGING_CONFIG: &str = "info";
const VERBOSE_LOGGING_CONFIG: &str = "debug";

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

    let _guard = configure_logging(&output_format, &workspace_layout);
    info!(version = VERSION, args = ?std::env::args().collect::<Vec<_>>(), "Initializing kamu-cli");

    load_config(&mut catalog);

    let mut command: Box<dyn Command> = cli_commands::get_command(&catalog, matches)?;

    let result = if command.needs_workspace() && !in_workspace(catalog.get_one().unwrap()) {
        Err(CLIError::usage_error_from(NotInWorkspace))
    } else {
        command.run()
    };

    match result {
        Ok(res) => {
            info!("Command successful");
            Ok(res)
        }
        Err(e) => {
            error!(error = ?e, "Command failed");
            Err(e)
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// Catalog
/////////////////////////////////////////////////////////////////////////////////////////

fn configure_catalog() -> Result<Catalog, InjectionError> {
    let mut catalog = Catalog::new();

    catalog.add::<ConfigService>();
    catalog.add::<ContainerRuntime>();

    catalog.add::<MetadataRepositoryImpl>();
    catalog.bind::<dyn MetadataRepository, MetadataRepositoryImpl>()?;

    catalog.add::<RemoteRepositoryRegistryImpl>();
    catalog.bind::<dyn RemoteRepositoryRegistry, RemoteRepositoryRegistryImpl>()?;

    catalog.add::<RemoteAliasesRegistryImpl>();
    catalog.bind::<dyn RemoteAliasesRegistry, RemoteAliasesRegistryImpl>()?;

    catalog.add::<ResourceLoaderImpl>();
    catalog.bind::<dyn ResourceLoader, ResourceLoaderImpl>()?;

    catalog.add::<IngestServiceImpl>();
    catalog.bind::<dyn IngestService, IngestServiceImpl>()?;

    catalog.add::<TransformServiceImpl>();
    catalog.bind::<dyn TransformService, TransformServiceImpl>()?;

    catalog.add::<VerificationServiceImpl>();
    catalog.bind::<dyn VerificationService, VerificationServiceImpl>()?;

    catalog.add::<SearchServiceImpl>();
    catalog.bind::<dyn SearchService, SearchServiceImpl>()?;

    catalog.add::<SyncServiceImpl>();
    catalog.bind::<dyn SyncService, SyncServiceImpl>()?;

    catalog.add::<PullServiceImpl>();
    catalog.bind::<dyn PullService, PullServiceImpl>()?;

    catalog.add::<PushServiceImpl>();
    catalog.bind::<dyn PushService, PushServiceImpl>()?;

    catalog.add::<ProvenanceServiceImpl>();
    catalog.bind::<dyn ProvenanceService, ProvenanceServiceImpl>()?;

    catalog.add::<QueryServiceImpl>();
    catalog.bind::<dyn QueryService, QueryServiceImpl>()?;

    catalog.add::<RepositoryFactory>();

    catalog.add::<EngineProvisionerLocal>();
    catalog.bind::<dyn EngineProvisioner, EngineProvisionerLocal>()?;

    Ok(catalog)
}

/////////////////////////////////////////////////////////////////////////////////////////
// Config
/////////////////////////////////////////////////////////////////////////////////////////

fn load_config(catalog: &mut Catalog) {
    let config_svc = catalog.get_one::<ConfigService>().unwrap();
    let config = config_svc.load_with_defaults(ConfigScope::Flattened);

    info!(config = ?config, "Loaded configuration");

    let network_ns = config.engine.as_ref().unwrap().network_ns.unwrap();

    catalog.add_value(ContainerRuntimeConfig {
        runtime: config.engine.as_ref().unwrap().runtime.unwrap(),
        network_ns,
    });

    catalog.add_value(EngineProvisionerLocalConfig {
        max_concurrency: config.engine.as_ref().unwrap().max_concurrency,
        start_timeout: config
            .engine
            .as_ref()
            .unwrap()
            .start_timeout
            .unwrap()
            .into(),
        shutdown_timeout: config
            .engine
            .as_ref()
            .unwrap()
            .shutdown_timeout
            .unwrap()
            .into(),
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

fn configure_logging(
    output_config: &OutputConfig,
    workspace_layout: &WorkspaceLayout,
) -> Option<tracing_appender::non_blocking::WorkerGuard> {
    use tracing_bunyan_formatter::{BunyanFormattingLayer, JsonStorageLayer};
    use tracing_log::LogTracer;
    use tracing_subscriber::fmt::format::FmtSpan;
    use tracing_subscriber::{layer::SubscriberExt, EnvFilter};

    // Logging may be already initialized when running under tests
    if tracing::dispatcher::has_been_set() {
        return None;
    }

    // Use configuration from RUST_LOG env var if provided
    let env_filter = match EnvFilter::try_from_default_env() {
        Ok(filter) => filter,
        Err(_) => match output_config.verbosity_level {
            0 | 1 => EnvFilter::new(DEFAULT_LOGGING_CONFIG.to_owned()),
            _ => EnvFilter::new(VERBOSE_LOGGING_CONFIG.to_owned()),
        },
    };

    if output_config.verbosity_level > 0 {
        // Log to STDERR
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
            .with_thread_names(true)
            .with_writer(std::io::stderr)
            .init();

        None
    } else if workspace_layout.run_info_dir.exists() {
        // Log to file with Bunyan JSON formatter
        let log_path = workspace_layout.run_info_dir.join("kamu.log");

        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&log_path)
            .unwrap_or_else(|e| {
                panic!("Failed to create log file at {}: {}", log_path.display(), e)
            });

        let (appender, guard) = tracing_appender::non_blocking(file);

        let formatting_layer = BunyanFormattingLayer::new(BINARY_NAME.to_owned(), appender);
        let subscriber = tracing_subscriber::registry()
            .with(env_filter)
            .with(JsonStorageLayer)
            .with(formatting_layer);

        // Redirect all standard logging to tracing events
        LogTracer::init().expect("Failed to set LogTracer");

        tracing::subscriber::set_global_default(subscriber).expect("Failed to set subscriber");

        Some(guard)
    } else {
        // Discard logs
        None
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// Output format
/////////////////////////////////////////////////////////////////////////////////////////

fn configure_output_format(matches: &clap::ArgMatches) -> OutputConfig {
    let is_tty = console::Term::stdout().features().is_attended();

    let verbosity_level = matches.occurrences_of("verbose") as u8;
    if verbosity_level > 0 {
        std::env::set_var("RUST_BACKTRACE", "1");
    }

    let quiet = matches.is_present("quiet");

    let format_str = get_output_format_recursive(matches, &super::cli());

    let format = match format_str {
        Some("csv") => OutputFormat::Csv,
        Some("json") => OutputFormat::Json,
        Some("json-ld") => OutputFormat::JsonLD,
        Some("json-soa") => OutputFormat::JsonSoA,
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

fn get_output_format_recursive<'a>(
    matches: &'a clap::ArgMatches,
    cmd: &clap::App<'_>,
) -> Option<&'a str> {
    if let Some((subcommand_name, submatches)) = matches.subcommand() {
        let subcommand = cmd
            .get_subcommands()
            .find(|s| s.get_name() == subcommand_name)
            .unwrap();
        let has_output_format = subcommand
            .get_opts()
            .find(|opt| opt.get_name() == "output-format")
            .is_some();

        if has_output_format {
            if let Some(fmt) = submatches.value_of("output-format") {
                return Some(fmt);
            }
        }

        get_output_format_recursive(submatches, subcommand)
    } else {
        None
    }
}
