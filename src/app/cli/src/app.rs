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
use kamu::*;

use crate::error::*;
use crate::explore::TraceServer;
use crate::output::*;
use crate::{
    accounts,
    cli_commands,
    config,
    odf_server,
    GcService,
    WorkspaceLayout,
    WorkspaceService,
};

/////////////////////////////////////////////////////////////////////////////////////////

pub const BINARY_NAME: &str = "kamu";
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

const DEFAULT_LOGGING_CONFIG: &str = "info";
const VERBOSE_LOGGING_CONFIG: &str = "debug";

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn run(
    workspace_layout: WorkspaceLayout,
    matches: clap::ArgMatches,
) -> Result<(), CLIError> {
    // Always capture backtraces for logging - we will separately decide whether to
    // display them to the user based on verbosity level
    if std::env::var_os("RUST_BACKTRACE").is_none() {
        std::env::set_var("RUST_BACKTRACE", "1");
    }

    let workspace_svc = WorkspaceService::new(Arc::new(workspace_layout.clone()));
    let workspace_version = workspace_svc.workspace_version()?;
    let config = load_config(&workspace_layout);

    let current_account = accounts::AccountService::current_account_indication(
        &matches,
        workspace_svc.is_multi_tenant_workspace(),
        config.users.as_ref().unwrap(),
    );

    prepare_run_dir(&workspace_layout.run_info_dir);

    // Configure application
    let (guards, base_catalog, cli_catalog, output_config) = {
        let dependencies_graph_repository = prepare_dependencies_graph_repository(
            &workspace_layout,
            workspace_svc.is_multi_tenant_workspace(),
            current_account.to_current_account_subject(),
        );

        let mut base_catalog_builder =
            configure_base_catalog(&workspace_layout, workspace_svc.is_multi_tenant_workspace());

        base_catalog_builder
            .add_value(dependencies_graph_repository)
            .bind::<dyn domain::DependencyGraphRepository, DependencyGraphRepositoryInMemory>();

        let output_config = configure_output_format(&matches, &workspace_svc);
        base_catalog_builder.add_value(output_config.clone());

        let guards = configure_logging(&output_config, &workspace_layout);
        tracing::info!(
            version = VERSION,
            args = ?std::env::args().collect::<Vec<_>>(),
            ?workspace_version,
            workspace_root = ?workspace_layout.root_dir,
            "Initializing kamu-cli"
        );

        register_config_in_catalog(
            &config,
            &mut base_catalog_builder,
            workspace_svc.is_multi_tenant_workspace(),
        );

        let base_catalog = base_catalog_builder.build();

        let cli_catalog = configure_cli_catalog(&base_catalog)
            .add_value(current_account.to_current_account_subject())
            .build();

        (guards, base_catalog, cli_catalog, output_config)
    };

    // Evict cache
    if workspace_svc.is_in_workspace() && !workspace_svc.is_upgrade_needed()? {
        cli_catalog.get_one::<GcService>()?.evict_cache()?;
    }

    let result = match cli_commands::get_command(&base_catalog, &cli_catalog, &matches) {
        Ok(mut command) => {
            if command.needs_workspace() && !workspace_svc.is_in_workspace() {
                Err(CLIError::usage_error_from(NotInWorkspace))
            } else if command.needs_workspace() && workspace_svc.is_upgrade_needed()? {
                Err(CLIError::usage_error_from(WorkspaceUpgradeRequired))
            } else if current_account.is_explicit() && !workspace_svc.is_multi_tenant_workspace() {
                Err(CLIError::usage_error_from(NotInMultiTenantWorkspace))
            } else {
                command.run().await
            }
        }
        Err(e) => Err(e),
    };

    match &result {
        Ok(()) => {
            tracing::info!("Command successful");
        }
        Err(err) => {
            tracing::error!(
                error_dbg = ?err,
                error = %err.pretty(true),
                "Command failed",
            );

            if output_config.is_tty && output_config.verbosity_level == 0 {
                eprintln!("{}", err.pretty(false));
            }
        }
    }

    // Flush all logging sinks
    drop(guards);

    if let Some(trace_file) = &output_config.trace_file {
        // Run a web server and open the trace in the browser if environment allows
        let _ = TraceServer::maybe_serve_in_browser(trace_file).await;
    }

    result
}

/////////////////////////////////////////////////////////////////////////////////////////
// Catalog
/////////////////////////////////////////////////////////////////////////////////////////

pub fn prepare_dependencies_graph_repository(
    workspace_layout: &WorkspaceLayout,
    multi_tenant_workspace: bool,
    current_account_subject: CurrentAccountSubject,
) -> DependencyGraphRepositoryInMemory {
    // Construct a special catalog just to create 1 object, but with a repository
    // bound to CLI user. It also should be authorized to access any dataset.

    let special_catalog_for_graph = CatalogBuilder::new()
        .add::<event_bus::EventBus>()
        .add_builder(
            DatasetRepositoryLocalFs::builder()
                .with_root(workspace_layout.datasets_dir.clone())
                .with_multi_tenant(multi_tenant_workspace),
        )
        .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
        .add_value(current_account_subject)
        .add::<kamu::domain::auth::AlwaysHappyDatasetActionAuthorizer>()
        .add::<kamu::DependencyGraphServiceInMemory>()
        // Don't add it's own initializer, leave optional dependency uninitialized
        .build();

    let dataset_repo = special_catalog_for_graph.get_one().unwrap();

    DependencyGraphRepositoryInMemory::new(dataset_repo)
}

// Public only for tests
pub fn configure_base_catalog(
    workspace_layout: &WorkspaceLayout,
    multi_tenant_workspace: bool,
) -> CatalogBuilder {
    let mut b = CatalogBuilder::new();

    b.add_value(workspace_layout.clone());

    b.add::<ContainerRuntime>();

    b.add::<SystemTimeSourceDefault>();

    b.add::<event_bus::EventBus>();

    b.add_builder(
        DatasetRepositoryLocalFs::builder()
            .with_root(workspace_layout.datasets_dir.clone())
            .with_multi_tenant(multi_tenant_workspace),
    );
    b.bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>();

    b.add::<DatasetFactoryImpl>();

    b.add_builder(
        RemoteRepositoryRegistryImpl::builder().with_repos_dir(workspace_layout.repos_dir.clone()),
    );
    b.bind::<dyn RemoteRepositoryRegistry, RemoteRepositoryRegistryImpl>();

    b.add::<RemoteAliasesRegistryImpl>();

    b.add::<ResourceLoaderImpl>();

    b.add::<DataFormatRegistryImpl>();

    b.add_builder(
        PollingIngestServiceImpl::builder()
            .with_run_info_dir(workspace_layout.run_info_dir.clone())
            .with_cache_dir(workspace_layout.cache_dir.clone()),
    );
    b.bind::<dyn PollingIngestService, PollingIngestServiceImpl>();

    b.add_builder(
        PushIngestServiceImpl::builder().with_run_info_dir(workspace_layout.run_info_dir.clone()),
    );
    b.bind::<dyn PushIngestService, PushIngestServiceImpl>();

    b.add::<TransformServiceImpl>();

    b.add::<VerificationServiceImpl>();

    b.add::<SearchServiceImpl>();

    b.add::<SyncServiceImpl>();

    b.add::<PullServiceImpl>();

    b.add::<PushServiceImpl>();

    b.add::<ResetServiceImpl>();

    b.add::<ProvenanceServiceImpl>();

    b.add::<QueryServiceImpl>();

    b.add::<ObjectStoreRegistryImpl>();

    b.add::<ObjectStoreBuilderLocalFs>();

    b.add_builder(
        EngineProvisionerLocal::builder().with_run_info_dir(workspace_layout.run_info_dir.clone()),
    );
    b.bind::<dyn EngineProvisioner, EngineProvisionerLocal>();

    b.add::<kamu_adapter_http::SmartTransferProtocolClientWs>();

    b.add::<kamu_task_system_inmem::TaskSchedulerInMemory>();

    b.add::<kamu_task_system_inmem::TaskExecutorInMemory>();

    b.add::<kamu_task_system_inmem::TaskSystemEventStoreInMemory>();

    // TODO: initialize graph dependencies when starting API server
    b.add::<DependencyGraphServiceInMemory>();

    b.add::<kamu_flow_system_inmem::FlowConfigurationServiceInMemory>();
    b.add::<kamu_flow_system_inmem::FlowServiceInMemory>();
    b.add_value(kamu_flow_system_inmem::domain::FlowServiceRunConfig::new(
        chrono::Duration::seconds(1),
        chrono::Duration::minutes(1),
    ));

    b.add::<kamu_flow_system_inmem::FlowEventStoreInMem>();
    b.add::<kamu_flow_system_inmem::FlowConfigurationEventStoreInMem>();

    b.add::<accounts::AccountService>();

    // No Github login possible for single-tenant workspace
    if multi_tenant_workspace {
        b.add::<kamu_adapter_oauth::OAuthGithub>();
    }

    b.add::<AuthenticationServiceImpl>();

    // Give both CLI and server access to stored repo access tokens
    b.add::<odf_server::AccessTokenRegistryService>();
    b.add::<odf_server::CLIAccessTokenStore>();

    b.add::<kamu_adapter_auth_oso::KamuAuthOso>();
    b.add::<kamu_adapter_auth_oso::OsoDatasetAuthorizer>();

    b
}

// Public only for tests
pub fn configure_cli_catalog(base_catalog: &Catalog) -> CatalogBuilder {
    let mut b = CatalogBuilder::new_chained(base_catalog);

    b.add::<config::ConfigService>();
    b.add::<GcService>();
    b.add::<WorkspaceService>();
    b.add::<odf_server::LoginService>();

    b
}

/////////////////////////////////////////////////////////////////////////////////////////
// Config
/////////////////////////////////////////////////////////////////////////////////////////

fn load_config(workspace_layout: &WorkspaceLayout) -> config::CLIConfig {
    let config_svc = config::ConfigService::new(workspace_layout);
    let config = config_svc.load_with_defaults(config::ConfigScope::Flattened);

    tracing::info!(?config, "Loaded configuration");
    config
}

pub fn register_config_in_catalog(
    config: &config::CLIConfig,
    catalog_builder: &mut CatalogBuilder,
    multi_tenant_workspace: bool,
) {
    let network_ns = config.engine.as_ref().unwrap().network_ns.unwrap();

    // Register JupyterConfig used by some commands
    catalog_builder.add_value(config.frontend.as_ref().unwrap().jupyter.clone().unwrap());

    catalog_builder.add_value(ContainerRuntimeConfig {
        runtime: config.engine.as_ref().unwrap().runtime.unwrap(),
        network_ns,
    });

    catalog_builder.add_value(EngineProvisionerLocalConfig {
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
        spark_image: config
            .engine
            .as_ref()
            .unwrap()
            .images
            .as_ref()
            .unwrap()
            .spark
            .clone()
            .unwrap(),
        flink_image: config
            .engine
            .as_ref()
            .unwrap()
            .images
            .as_ref()
            .unwrap()
            .flink
            .clone()
            .unwrap(),
        datafusion_image: config
            .engine
            .as_ref()
            .unwrap()
            .images
            .as_ref()
            .unwrap()
            .datafusion
            .clone()
            .unwrap(),
    });

    let ipfs_conf = config.protocol.as_ref().unwrap().ipfs.as_ref().unwrap();

    catalog_builder.add_value(IpfsGateway {
        url: ipfs_conf.http_gateway.clone().unwrap(),
        pre_resolve_dnslink: ipfs_conf.pre_resolve_dnslink.unwrap(),
    });
    catalog_builder.add_value(kamu::utils::ipfs_wrapper::IpfsClient::default());

    if multi_tenant_workspace {
        catalog_builder.add_value(config.users.clone().unwrap());
    } else {
        if let Some(users) = &config.users {
            assert!(
                users.predefined.is_empty(),
                "There cannot be predefined users in a single-tenant workspace"
            );
        }

        catalog_builder.add_value(config::UsersConfig::single_tenant());
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// Logging
/////////////////////////////////////////////////////////////////////////////////////////

fn prepare_run_dir(run_dir: &Path) {
    if run_dir.exists() {
        std::fs::remove_dir_all(run_dir).unwrap_or_else(|e| {
            panic!(
                "Unable to clean up run directory {}: {}",
                run_dir.display(),
                e
            )
        });
        std::fs::create_dir(run_dir).unwrap_or_else(|e| {
            panic!(
                "Unable to create run directory {}: {}",
                run_dir.display(),
                e
            )
        });
    }
}

fn configure_logging(output_config: &OutputConfig, workspace_layout: &WorkspaceLayout) -> Guards {
    use tracing_bunyan_formatter::{BunyanFormattingLayer, JsonStorageLayer};
    use tracing_log::LogTracer;
    use tracing_subscriber::fmt::format::FmtSpan;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::EnvFilter;

    // Setup custom panic hook
    std::panic::update_hook(move |prev, info| {
        prev(info);
        eprintln!(
            "\n{}",
            console::style(
                "Oh no, looks like kamu has crashed! \n\
                Please try running `kamu system \
                 diagnose` to verify your environment. \n\
                If the issue persists, help us \
                 by reporting this problem at https://github.com/kamu-data/kamu-cli/issues"
            )
            .bold()
        );
    });

    // Logging may be already initialized when running under tests
    if tracing::dispatcher::has_been_set() {
        return Guards::default();
    }

    // Use configuration from RUST_LOG env var if provided
    let env_filter = match EnvFilter::try_from_default_env() {
        Ok(filter) => filter,
        Err(_) => match output_config.verbosity_level {
            0 | 1 => EnvFilter::new(DEFAULT_LOGGING_CONFIG),
            _ => EnvFilter::new(VERBOSE_LOGGING_CONFIG),
        },
    };

    if output_config.verbosity_level > 0 {
        // Log to STDERR
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
            .with_writer(std::io::stderr)
            .pretty()
            .init();

        return Guards::default();
    }

    if !workspace_layout.run_info_dir.exists() {
        // Running outside of workspace - discard logs
        return Guards::default();
    }

    // Configure Perfetto tracing if enabled
    let (maybe_perfetto_layer, perfetto_guard) = if let Some(trace_file) = &output_config.trace_file
    {
        let (layer, guard) = tracing_perfetto::PerfettoLayer::new(trace_file);
        (Some(layer), Some(guard))
    } else {
        (None, None)
    };

    // Log to file with JSON formatter
    let (appender, appender_guard) = {
        let log_path = workspace_layout.run_info_dir.join("kamu.log");
        tracing_appender::non_blocking(std::fs::File::create(&log_path).unwrap_or_else(|e| {
            panic!("Failed to create log file at {}: {}", log_path.display(), e)
        }))
    };

    let subscriber = tracing_subscriber::registry()
        .with(env_filter)
        .with(JsonStorageLayer)
        .with(maybe_perfetto_layer)
        .with(BunyanFormattingLayer::new(BINARY_NAME.to_owned(), appender));

    // Redirect all standard logging to tracing events
    LogTracer::init().expect("Failed to set LogTracer");

    tracing::subscriber::set_global_default(subscriber).expect("Failed to set subscriber");

    Guards {
        appender: Some(appender_guard),
        perfetto: perfetto_guard,
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// Output format
/////////////////////////////////////////////////////////////////////////////////////////

fn configure_output_format(
    matches: &clap::ArgMatches,
    workspace_svc: &WorkspaceService,
) -> OutputConfig {
    let is_tty = console::Term::stdout().features().is_attended();

    let verbosity_level = matches.get_count("verbose");

    let quiet = matches.get_flag("quiet");

    let trace_file = if workspace_svc.is_in_workspace() && matches.get_flag("trace") {
        Some(
            workspace_svc
                .layout()
                .unwrap()
                .run_info_dir
                .join("kamu.perfetto.json"),
        )
    } else {
        None
    };

    let format_str = get_output_format_recursive(matches, &super::cli());

    let format = match format_str {
        Some("csv") => OutputFormat::Csv,
        Some("json") => OutputFormat::Json,
        Some("ndjson") => OutputFormat::NdJson,
        Some("json-soa") => OutputFormat::JsonSoA,
        Some("table") => OutputFormat::Table,
        None | Some(_) => {
            if is_tty {
                OutputFormat::Table
            } else {
                OutputFormat::Json
            }
        }
    };

    OutputConfig {
        quiet,
        verbosity_level,
        is_tty,
        format,
        trace_file,
    }
}

fn get_output_format_recursive<'a>(
    matches: &'a clap::ArgMatches,
    cmd: &clap::Command,
) -> Option<&'a str> {
    if let Some((subcommand_name, submatches)) = matches.subcommand() {
        let subcommand = cmd
            .get_subcommands()
            .find(|s| s.get_name() == subcommand_name)
            .unwrap();
        let has_output_format = subcommand
            .get_opts()
            .any(|opt| opt.get_id() == "output-format");

        if has_output_format {
            if let Some(fmt) = submatches.get_one("output-format").map(String::as_str) {
                return Some(fmt);
            }
        }

        get_output_format_recursive(submatches, subcommand)
    } else {
        None
    }
}

#[allow(dead_code)]
#[derive(Default)]
struct Guards {
    appender: Option<tracing_appender::non_blocking::WorkerGuard>,
    perfetto: Option<tracing_perfetto::FlushGuard>,
}
