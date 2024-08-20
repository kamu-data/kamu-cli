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

use chrono::{DateTime, Duration, Utc};
use container_runtime::{ContainerRuntime, ContainerRuntimeConfig};
use database_common::DatabaseTransactionRunner;
use dill::*;
use kamu::domain::*;
use kamu::*;
use kamu_accounts::*;
use kamu_accounts_services::PredefinedAccountsRegistrator;
use kamu_adapter_http::{FileUploadLimitConfig, UploadServiceLocal};
use kamu_adapter_oauth::GithubAuthenticationConfig;
use kamu_auth_rebac_services::RebacServiceImpl;
use kamu_datasets::DatasetEnvVar;
use kamu_flow_system_inmem::domain::FlowConfigurationUpdatedMessage;
use kamu_flow_system_services::MESSAGE_PRODUCER_KAMU_FLOW_CONFIGURATION_SERVICE;
use kamu_task_system_inmem::domain::{TaskProgressMessage, MESSAGE_PRODUCER_KAMU_TASK_EXECUTOR};
use messaging_outbox::{register_message_dispatcher, Outbox, OutboxDispatchingImpl};
use time_source::{SystemTimeSource, SystemTimeSourceDefault, SystemTimeSourceStub};
use tracing::warn;

use crate::accounts::AccountService;
use crate::error::*;
use crate::explore::TraceServer;
use crate::output::*;
use crate::{
    cli_commands,
    config,
    configure_database_components,
    configure_in_memory_components,
    connect_database_initially,
    odf_server,
    spawn_password_refreshing_job,
    try_build_db_connection_settings,
    GcService,
    WorkspaceLayout,
    WorkspaceService,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const BINARY_NAME: &str = "kamu";
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

const DEFAULT_LOGGING_CONFIG: &str = "info";
const VERBOSE_LOGGING_CONFIG: &str = "debug";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Errors before commands are executed are not output anywhere -- log them
pub async fn run(
    workspace_layout: WorkspaceLayout,
    matches: clap::ArgMatches,
) -> Result<(), CLIError> {
    // Always capture backtraces for logging - we will separately decide whether to
    // display them to the user based on verbosity level
    if std::env::var_os("RUST_BACKTRACE").is_none() {
        std::env::set_var("RUST_BACKTRACE", "1");
    }

    // Sometimes (in the case of predefined users), we need to know whether the
    // workspace to be created will be multi-tenant or not right away, even before
    // the `kamu init` command itself is processed.
    let init_multi_tenant_workspace = matches!(matches.subcommand(), Some(("init", arg_matches)) if arg_matches.get_flag("multi-tenant"));
    let workspace_svc = WorkspaceService::new(
        Arc::new(workspace_layout.clone()),
        init_multi_tenant_workspace,
    );
    let workspace_version = workspace_svc.workspace_version()?;

    let is_multi_tenant_workspace = workspace_svc.is_multi_tenant_workspace();
    let config = load_config(&workspace_layout);
    let current_account = AccountService::current_account_indication(
        &matches,
        is_multi_tenant_workspace,
        config.users.as_ref().unwrap(),
    );

    let system_time: Option<DateTime<Utc>> = matches
        .get_one::<String>("system-time")
        .map(|s| DateTime::parse_from_rfc3339(s))
        .transpose()
        .map_err(CLIError::usage_error_from)?
        .map(Into::into);
    let is_e2e_testing = matches.contains_id("e2e-output-data-path");

    prepare_run_dir(&workspace_layout.run_info_dir);

    let maybe_db_connection_settings = config
        .database
        .clone()
        .and_then(try_build_db_connection_settings);

    // Configure application
    let (guards, base_catalog, cli_catalog, output_config) = {
        let dependencies_graph_repository = prepare_dependencies_graph_repository(
            &workspace_layout,
            is_multi_tenant_workspace,
            current_account.to_current_account_subject(),
        );

        let mut base_catalog_builder = configure_base_catalog(
            &workspace_layout,
            is_multi_tenant_workspace,
            system_time,
            is_e2e_testing,
        );

        base_catalog_builder.add_value(JwtAuthenticationConfig::load_from_env());
        base_catalog_builder.add_value(GithubAuthenticationConfig::load_from_env());

        if let Some(db_connection_settings) = maybe_db_connection_settings.as_ref() {
            configure_database_components(
                &mut base_catalog_builder,
                config.database.as_ref().unwrap(),
                db_connection_settings.clone(),
            );
        } else {
            configure_in_memory_components(&mut base_catalog_builder);
        };

        base_catalog_builder
            .add_value(dependencies_graph_repository)
            .bind::<dyn DependencyGraphRepository, DependencyGraphRepositoryInMemory>();

        let output_config = configure_output_format(&matches, &workspace_svc);
        base_catalog_builder.add_value(output_config.clone());

        let no_color_output = matches.get_flag("no-color");
        let guards = configure_logging(&output_config, &workspace_layout, no_color_output);

        tracing::info!(
            version = VERSION,
            args = ?std::env::args().collect::<Vec<_>>(),
            ?workspace_version,
            workspace_root = ?workspace_layout.root_dir,
            "Initializing {BINARY_NAME}"
        );

        register_config_in_catalog(
            &config,
            &mut base_catalog_builder,
            is_multi_tenant_workspace,
        );

        let base_catalog = base_catalog_builder.build();

        // Database requires extra actions:
        let final_base_catalog = if let Some(db_config) = config.database {
            // Connect database and obtain a connection pool
            let catalog_with_pool = connect_database_initially(&base_catalog).await?;

            // Periodically refresh password in the connection pool, if configured
            spawn_password_refreshing_job(&db_config, &catalog_with_pool).await;

            catalog_with_pool
        } else {
            base_catalog
        };

        let cli_catalog = configure_cli_catalog(&final_base_catalog, is_multi_tenant_workspace)
            .add_value(current_account.to_current_account_subject())
            .build();

        (guards, final_base_catalog, cli_catalog, output_config)
    };

    // Evict cache
    if workspace_svc.is_in_workspace() && !workspace_svc.is_upgrade_needed()? {
        cli_catalog.get_one::<GcService>()?.evict_cache()?;
    }

    initialize_components(&cli_catalog).await?;

    let need_to_wrap_with_transaction = cli_commands::command_needs_transaction(&matches)?;
    let run_command = |catalog: Catalog| async move {
        match cli_commands::get_command(&base_catalog, &catalog, &matches) {
            Ok(mut command) => {
                if command.needs_workspace() && !workspace_svc.is_in_workspace() {
                    Err(CLIError::usage_error_from(NotInWorkspace))
                } else if command.needs_workspace() && workspace_svc.is_upgrade_needed()? {
                    Err(CLIError::usage_error_from(WorkspaceUpgradeRequired))
                } else if current_account.is_explicit() && !is_multi_tenant_workspace {
                    Err(CLIError::usage_error_from(NotInMultiTenantWorkspace))
                } else {
                    command.before_run().await?;
                    command.run().await
                }
            }
            Err(e) => Err(e),
        }
    };
    let is_database_used = maybe_db_connection_settings.is_some();
    let command_result = if is_database_used && need_to_wrap_with_transaction {
        let transaction_runner = DatabaseTransactionRunner::new(cli_catalog);

        transaction_runner
            .transactional(|transactional_catalog| async move {
                run_command(transactional_catalog).await
            })
            .await
    } else {
        run_command(cli_catalog).await
    };

    match &command_result {
        Ok(()) => {
            tracing::info!("Command successful");
        }
        Err(err) => {
            tracing::error!(
                error_dbg = ?err,
                error = %err.pretty(true),
                "Command failed",
            );

            if output_config.verbosity_level == 0 {
                eprintln!("{}", err.pretty(false));
            }
        }
    }

    // Flush all logging sinks
    drop(guards);

    if let Some(trace_file) = &output_config.trace_file {
        // Run a web server and open the trace in the browser if the environment allows
        let _ = TraceServer::maybe_serve_in_browser(trace_file).await;
    }

    command_result
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Catalog
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn prepare_dependencies_graph_repository(
    workspace_layout: &WorkspaceLayout,
    multi_tenant_workspace: bool,
    current_account_subject: CurrentAccountSubject,
) -> DependencyGraphRepositoryInMemory {
    // Construct a special catalog just to create 1 object, but with a repository
    // bound to CLI user. It also should be authorized to access any dataset.

    let special_catalog_for_graph = CatalogBuilder::new()
        .add::<SystemTimeSourceDefault>()
        .add_builder(
            DatasetRepositoryLocalFs::builder()
                .with_root(workspace_layout.datasets_dir.clone())
                .with_multi_tenant(multi_tenant_workspace),
        )
        .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
        .bind::<dyn DatasetRepositoryWriter, DatasetRepositoryLocalFs>()
        .add_value(current_account_subject)
        .add::<auth::AlwaysHappyDatasetActionAuthorizer>()
        .add::<DependencyGraphServiceInMemory>()
        // Don't add its own initializer, leave optional dependency uninitialized
        .build();

    let dataset_repo = special_catalog_for_graph.get_one().unwrap();

    DependencyGraphRepositoryInMemory::new(dataset_repo)
}

// Public only for tests
pub fn configure_base_catalog(
    workspace_layout: &WorkspaceLayout,
    multi_tenant_workspace: bool,
    system_time: Option<DateTime<Utc>>,
    is_e2e_testing: bool,
) -> CatalogBuilder {
    let mut b = CatalogBuilder::new();

    b.add_value(workspace_layout.clone());
    b.add_value(RunInfoDir::new(&workspace_layout.run_info_dir));
    b.add_value(CacheDir::new(&workspace_layout.cache_dir));
    b.add_value(RemoteReposDir::new(&workspace_layout.repos_dir));

    b.add::<ContainerRuntime>();

    if let Some(system_time) = system_time {
        b.add_value(SystemTimeSourceStub::new_set(system_time));
        b.bind::<dyn SystemTimeSource, SystemTimeSourceStub>();
    } else {
        b.add::<SystemTimeSourceDefault>();
    }

    b.add_builder(
        DatasetRepositoryLocalFs::builder()
            .with_root(workspace_layout.datasets_dir.clone())
            .with_multi_tenant(multi_tenant_workspace),
    );
    b.bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>();
    b.bind::<dyn DatasetRepositoryWriter, DatasetRepositoryLocalFs>();

    b.add::<DatasetFactoryImpl>();

    b.add::<DatasetChangesServiceImpl>();

    b.add::<RemoteRepositoryRegistryImpl>();

    b.add::<RemoteAliasesRegistryImpl>();

    b.add::<ResourceLoaderImpl>();

    b.add::<DataFormatRegistryImpl>();

    b.add::<FetchService>();

    b.add::<PollingIngestServiceImpl>();

    b.add::<PushIngestServiceImpl>();

    b.add::<TransformServiceImpl>();

    b.add::<VerificationServiceImpl>();

    b.add::<CompactionServiceImpl>();

    b.add::<SearchServiceImpl>();

    b.add::<SyncServiceImpl>();

    b.add::<PullServiceImpl>();

    b.add::<PushServiceImpl>();

    b.add::<ResetServiceImpl>();

    b.add::<ProvenanceServiceImpl>();

    b.add::<QueryServiceImpl>();

    b.add::<ObjectStoreRegistryImpl>();

    b.add::<ObjectStoreBuilderLocalFs>();

    b.add::<EngineProvisionerLocal>();

    b.add::<kamu_adapter_http::SmartTransferProtocolClientWs>();

    b.add::<kamu_task_system_services::TaskSchedulerImpl>();

    b.add::<kamu_task_system_services::TaskExecutorImpl>();

    b.add::<DependencyGraphServiceInMemory>();

    b.add::<DatasetOwnershipServiceInMemory>();
    b.add::<DatasetOwnershipServiceInMemoryStateInitializer>();

    b.add::<AppendDatasetMetadataBatchUseCaseImpl>();
    b.add::<CommitDatasetEventUseCaseImpl>();
    b.add::<CreateDatasetUseCaseImpl>();
    b.add::<CreateDatasetFromSnapshotUseCaseImpl>();
    b.add::<DeleteDatasetUseCaseImpl>();
    b.add::<RenameDatasetUseCaseImpl>();

    b.add::<kamu_flow_system_services::FlowConfigurationServiceImpl>();
    b.add::<kamu_flow_system_services::FlowServiceImpl>();
    b.add_value(kamu_flow_system_inmem::domain::FlowServiceRunConfig::new(
        chrono::Duration::try_seconds(1).unwrap(),
        chrono::Duration::try_minutes(1).unwrap(),
    ));

    b.add::<kamu_accounts_services::LoginPasswordAuthProvider>();

    // No GitHub login possible for single-tenant workspace
    if multi_tenant_workspace {
        if is_e2e_testing {
            b.add::<kamu_adapter_oauth::DummyOAuthGithub>();
        } else {
            b.add::<kamu_adapter_oauth::OAuthGithub>();
        }
    }

    b.add::<kamu_accounts_services::AuthenticationServiceImpl>();
    b.add::<kamu_accounts_services::AccessTokenServiceImpl>();
    b.add::<PredefinedAccountsRegistrator>();

    // Give both CLI and server access to stored repo access tokens
    b.add::<odf_server::AccessTokenRegistryService>();
    b.add::<odf_server::CLIAccessTokenStore>();

    b.add::<kamu_adapter_auth_oso::KamuAuthOso>();
    b.add::<kamu_adapter_auth_oso::OsoDatasetAuthorizer>();

    b.add::<UploadServiceLocal>();

    b.add::<DatabaseTransactionRunner>();

    b.add_builder(
        messaging_outbox::OutboxImmediateImpl::builder()
            .with_consumer_filter(messaging_outbox::ConsumerFilter::BestEffortConsumers),
    );
    b.add::<messaging_outbox::OutboxTransactionalImpl>();
    b.add::<messaging_outbox::OutboxDispatchingImpl>();
    b.bind::<dyn Outbox, OutboxDispatchingImpl>();
    b.add::<messaging_outbox::OutboxTransactionalProcessor>();

    register_message_dispatcher::<DatasetLifecycleMessage>(
        &mut b,
        MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
    );
    register_message_dispatcher::<TaskProgressMessage>(&mut b, MESSAGE_PRODUCER_KAMU_TASK_EXECUTOR);
    register_message_dispatcher::<FlowConfigurationUpdatedMessage>(
        &mut b,
        MESSAGE_PRODUCER_KAMU_FLOW_CONFIGURATION_SERVICE,
    );

    b.add::<RebacServiceImpl>();

    b
}

// Public only for tests
pub fn configure_cli_catalog(
    base_catalog: &Catalog,
    multi_tenant_workspace: bool,
) -> CatalogBuilder {
    let mut b = CatalogBuilder::new_chained(base_catalog);

    b.add::<config::ConfigService>();
    b.add::<GcService>();
    b.add_builder(WorkspaceService::builder().with_multi_tenant(multi_tenant_workspace));
    b.add::<odf_server::LoginService>();

    b
}

#[tracing::instrument(level = "info", skip_all)]
async fn initialize_components(cli_catalog: &Catalog) -> Result<(), CLIError> {
    // TODO: Generalize on-startup initialization into a trait
    DatabaseTransactionRunner::new(cli_catalog.clone())
        .transactional(|transactional_catalog| async move {
            let registrator = transactional_catalog
                .get_one::<PredefinedAccountsRegistrator>()
                .map_err(CLIError::critical)?;

            registrator
                .ensure_predefined_accounts_are_registered()
                .await
                .map_err(CLIError::critical)?;

            let initializer = transactional_catalog
                .get_one::<DatasetOwnershipServiceInMemoryStateInitializer>()
                .map_err(CLIError::critical)?;

            initializer
                .eager_initialization()
                .await
                .map_err(CLIError::critical)
        })
        .await?;

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Config
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
        risingwave_image: config
            .engine
            .as_ref()
            .unwrap()
            .images
            .as_ref()
            .unwrap()
            .risingwave
            .clone()
            .unwrap(),
    });

    catalog_builder.add_value(config.source.as_ref().unwrap().to_infra_cfg());
    catalog_builder.add_value(
        config
            .source
            .as_ref()
            .unwrap()
            .http
            .as_ref()
            .unwrap()
            .to_infra_cfg(),
    );
    catalog_builder.add_value(
        config
            .source
            .as_ref()
            .unwrap()
            .mqtt
            .as_ref()
            .unwrap()
            .to_infra_cfg(),
    );
    catalog_builder.add_value(
        config
            .source
            .as_ref()
            .unwrap()
            .ethereum
            .as_ref()
            .unwrap()
            .to_infra_cfg(),
    );

    let ipfs_conf = config.protocol.as_ref().unwrap().ipfs.as_ref().unwrap();

    catalog_builder.add_value(IpfsGateway {
        url: ipfs_conf.http_gateway.clone().unwrap(),
        pre_resolve_dnslink: ipfs_conf.pre_resolve_dnslink.unwrap(),
    });
    catalog_builder.add_value(kamu::utils::ipfs_wrapper::IpfsClient::default());

    if multi_tenant_workspace {
        let mut implicit_user_config = PredefinedAccountsConfig::new();
        implicit_user_config.predefined.push(
            AccountConfig::from_name(opendatafabric::AccountName::new_unchecked(
                AccountService::default_account_name(true).as_str(),
            ))
            .set_display_name(AccountService::default_user_name(true)),
        );

        use merge::Merge;
        let mut user_config = config.users.clone().unwrap();
        user_config.merge(implicit_user_config);
        catalog_builder.add_value(user_config);
    } else {
        if let Some(users) = &config.users {
            assert!(
                users.predefined.is_empty(),
                "There cannot be predefined users in a single-tenant workspace"
            );
        }

        catalog_builder.add_value(PredefinedAccountsConfig::single_tenant());
    }

    let uploads_config = config.uploads.as_ref().unwrap();
    catalog_builder.add_value(FileUploadLimitConfig::new_in_mb(
        uploads_config.max_file_size_in_mb.unwrap(),
    ));

    catalog_builder.add_value(config.dataset_env_vars.clone().unwrap());

    let dataset_env_vars_config = config.dataset_env_vars.as_ref().unwrap();
    match dataset_env_vars_config.encryption_key.as_ref() {
        None => {
            match dataset_env_vars_config.enabled.as_ref() {
                None => {
                    warn!("Dataset env vars configuration is missing. Feature will be disabled");
                }
                Some(true) => panic!("Dataset env vars encryption key is required"),
                _ => {}
            }
            catalog_builder.add::<kamu_datasets_services::DatasetKeyValueServiceSysEnv>();
            catalog_builder.add::<kamu_datasets_services::DatasetEnvVarServiceNull>();
        }
        Some(encryption_key) => {
            if let Some(enabled) = &dataset_env_vars_config.enabled
                && !enabled
            {
                warn!("Dataset env vars feature will be disabled");
                catalog_builder.add::<kamu_datasets_services::DatasetKeyValueServiceSysEnv>();
                catalog_builder.add::<kamu_datasets_services::DatasetEnvVarServiceNull>();
            } else {
                assert!(
                    DatasetEnvVar::try_asm_256_gcm_from_str(encryption_key).is_ok(),
                    "Invalid dataset env var encryption key",
                );
                catalog_builder.add::<kamu_datasets_services::DatasetKeyValueServiceImpl>();
                catalog_builder.add::<kamu_datasets_services::DatasetEnvVarServiceImpl>();
            }
        }
    }

    let outbox_config = config.outbox.as_ref().unwrap();
    catalog_builder.add_value(messaging_outbox::OutboxConfig::new(
        Duration::seconds(outbox_config.awaiting_step_secs.unwrap()),
        outbox_config.batch_size.unwrap(),
    ));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Logging
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

fn configure_logging(
    output_config: &OutputConfig,
    workspace_layout: &WorkspaceLayout,
    no_color_output: bool,
) -> Guards {
    use tracing_bunyan_formatter::{BunyanFormattingLayer, JsonStorageLayer};
    use tracing_log::LogTracer;
    use tracing_subscriber::fmt::format::FmtSpan;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::EnvFilter;

    if no_color_output {
        console::set_colors_enabled(false);
        console::set_colors_enabled_stderr(false);
    }

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
            .with_ansi(!no_color_output)
            .init();

        return Guards::default();
    }

    if !workspace_layout.run_info_dir.exists() {
        // Running outside workspace - discard logs
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Output format
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
        Some("json-aoa") => OutputFormat::JsonAoA,
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[allow(dead_code)]
#[derive(Default)]
struct Guards {
    appender: Option<tracing_appender::non_blocking::WorkerGuard>,
    perfetto: Option<tracing_perfetto::FlushGuard>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
