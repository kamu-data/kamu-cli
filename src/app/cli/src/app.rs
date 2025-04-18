// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;
use std::path::Path;
use std::sync::Arc;

use async_utils::ResultAsync;
use chrono::{DateTime, Duration, Utc};
use container_runtime::{ContainerRuntime, ContainerRuntimeConfig};
use database_common::DatabaseTransactionRunner;
use dill::*;
use internal_error::{InternalError, ResultIntoInternal};
use kamu::domain::*;
use kamu::*;
use kamu_accounts::*;
use kamu_adapter_http::platform::{FileUploadLimitConfig, UploadServiceLocal};
use kamu_adapter_oauth::GithubAuthenticationConfig;
use kamu_flow_system_inmem::domain::{
    FlowConfigurationUpdatedMessage,
    FlowProgressMessage,
    FlowTriggerUpdatedMessage,
};
use kamu_flow_system_services::{
    MESSAGE_PRODUCER_KAMU_FLOW_CONFIGURATION_SERVICE,
    MESSAGE_PRODUCER_KAMU_FLOW_PROGRESS_SERVICE,
    MESSAGE_PRODUCER_KAMU_FLOW_TRIGGER_SERVICE,
};
use kamu_task_system_inmem::domain::{TaskProgressMessage, MESSAGE_PRODUCER_KAMU_TASK_AGENT};
use merge::Merge as _;
use messaging_outbox::{register_message_dispatcher, Outbox, OutboxDispatchingImpl};
use time_source::{SystemTimeSource, SystemTimeSourceDefault, SystemTimeSourceStub};
use tracing::{warn, Instrument};

use crate::accounts::AccountService;
use crate::cli::Command;
use crate::error::*;
use crate::output::*;
use crate::{
    build_db_connection_settings,
    cli,
    cli_commands,
    config,
    configure_database_components,
    configure_in_memory_components,
    connect_database_initially,
    explore,
    get_app_database_config,
    move_initial_database_to_workspace_if_needed,
    odf_server,
    spawn_password_refreshing_job,
    ConfirmDeleteService,
    GcService,
    WorkspaceLayout,
    WorkspaceService,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const BINARY_NAME: &str = "kamu";
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

const LOG_LEVELS: [&str; 5] = [
    // Default
    "info",
    // First level of verbosity simply direct log to foreground
    "info",
    "debug,hyper=info,oso=info",
    "debug",
    "trace",
];

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Errors before commands are executed are not output anywhere -- log them
pub async fn run(workspace_layout: WorkspaceLayout, args: cli::Cli) -> Result<(), CLIError> {
    // Always capture backtraces for logging - we will separately decide whether to
    // display them to the user based on verbosity level
    if std::env::var_os("RUST_BACKTRACE").is_none() {
        std::env::set_var("RUST_BACKTRACE", "1");
    }

    // Sometimes (in the case of predefined users), we need to know whether the
    // workspace to be created will be multi-tenant or not right away, even before
    // the `kamu init` command itself is processed.
    let maybe_init_command = match &args.command {
        Command::Init(c) if c.creates_workspace() => Some(c.clone()),
        _ => None,
    };
    let init_multi_tenant_workspace = matches!(&maybe_init_command, Some(c) if c.multi_tenant);
    let workspace_svc = WorkspaceService::new(
        Arc::new(workspace_layout.clone()),
        init_multi_tenant_workspace,
    );
    let workspace_version = workspace_svc.workspace_version()?;

    let tenancy_config = if workspace_svc.is_multi_tenant_workspace() {
        TenancyConfig::MultiTenant
    } else {
        TenancyConfig::SingleTenant
    };
    let is_in_workspace = workspace_svc.is_in_workspace();
    let workspace_status = match (maybe_init_command.is_some(), is_in_workspace) {
        (false, false) => WorkspaceStatus::NoWorkspace,
        (true, false) => WorkspaceStatus::AboutToBeCreated(tenancy_config),
        (_, true) => WorkspaceStatus::Created(tenancy_config),
    };

    let config = load_config(&workspace_layout);
    let current_account = AccountService::current_account_indication(
        args.account.clone(),
        tenancy_config,
        config.users.as_ref().unwrap(),
    );

    prepare_run_dir(&workspace_layout.run_info_dir);

    let app_database_config = get_app_database_config(&workspace_layout, &config, workspace_status);
    let (database_config, maybe_temp_database_path) = app_database_config.into_inner();
    let maybe_db_connection_settings = database_config.as_ref().map(build_db_connection_settings);

    // Configure application
    let (guards, base_catalog, cli_catalog, maybe_server_catalog, output_config) = {
        let is_e2e_testing = args.e2e_output_data_path.is_some();

        let mut base_catalog_builder = configure_base_catalog(
            &workspace_layout,
            workspace_status,
            tenancy_config,
            args.system_time.map(Into::into),
            is_e2e_testing,
        );

        base_catalog_builder.add_value(JwtAuthenticationConfig::load_from_env());
        base_catalog_builder.add_value(GithubAuthenticationConfig::load_from_env());

        if let Some(db_connection_settings) = maybe_db_connection_settings.as_ref() {
            configure_database_components(
                &mut base_catalog_builder,
                database_config.as_ref().unwrap(),
                db_connection_settings.clone(),
            );
        } else {
            configure_in_memory_components(&mut base_catalog_builder);
        };

        let output_config = configure_output_format(&args, &workspace_svc);
        base_catalog_builder.add_value(output_config.clone());
        base_catalog_builder.add_value(Interact::new(args.yes, output_config.is_tty));

        let guards = configure_logging(&output_config, &workspace_layout, args.no_color);

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
            workspace_status,
            args.password_hashing_mode,
            is_e2e_testing,
        )?;

        let base_catalog = base_catalog_builder.build();

        // Database requires extra actions:
        let final_base_catalog = if let Some(db_config) = database_config {
            // Connect database and obtain a connection pool
            let catalog_with_pool = connect_database_initially(&base_catalog).await?;

            // Periodically refresh password in the connection pool, if configured
            spawn_password_refreshing_job(&db_config, &catalog_with_pool).await;

            catalog_with_pool
        } else {
            base_catalog
        };

        let maybe_server_catalog = if cli_commands::command_needs_server_components(&args) {
            let server_catalog = configure_server_catalog(&final_base_catalog).build();
            Some(server_catalog)
        } else {
            None
        };

        let cli_catalog = configure_cli_catalog(
            maybe_server_catalog.as_ref().unwrap_or(&final_base_catalog),
            tenancy_config,
        )
        .add_value(current_account.to_current_account_subject())
        .build();

        (
            guards,
            final_base_catalog,
            cli_catalog,
            maybe_server_catalog,
            output_config,
        )
    };

    // Register metrics
    let metrics_registry = observability::metrics::register_all(&cli_catalog);

    let is_workspace_upgrade_needed = workspace_svc.is_upgrade_needed()?;

    if is_in_workspace && !is_workspace_upgrade_needed {
        // Evict cache
        cli_catalog.get_one::<GcService>()?.evict_cache()?;
    }

    // Some extra steps are necessary for commands that require workspace
    let mut command_result: Result<(), CLIError> = if cli_commands::command_needs_workspace(&args) {
        if !workspace_svc.is_in_workspace() {
            Err(CLIError::usage_error_from(NotInWorkspace))
        } else if is_workspace_upgrade_needed {
            Err(CLIError::usage_error_from(WorkspaceUpgradeRequired))
        } else if current_account.is_explicit() && tenancy_config == TenancyConfig::SingleTenant {
            Err(CLIError::usage_error_from(NotInMultiTenantWorkspace))
        } else {
            Ok(())
        }
    } else {
        Ok(())
    };

    if command_result.is_ok() && cli_commands::command_needs_startup_jobs(&args) {
        command_result = run_startup_initializations(&cli_catalog).await;
    }

    if command_result.is_ok() {
        let is_transactional = maybe_db_connection_settings.is_some()
            && cli_commands::command_needs_transaction(&args);
        let is_outbox_processing_required = maybe_db_connection_settings.is_some()
            && cli_commands::command_needs_outbox_processing(&args);
        let work_catalog = maybe_server_catalog.as_ref().unwrap_or(&base_catalog);

        command_result = maybe_transactional(
            is_transactional,
            cli_catalog.clone(),
            |maybe_transactional_cli_catalog: Catalog| async move {
                let command_builder = cli_commands::get_command(
                    work_catalog,
                    &maybe_transactional_cli_catalog,
                    args,
                )?;

                let command = command_builder
                    .get(&maybe_transactional_cli_catalog)
                    .int_err()?;

                command.validate_args().await?;

                let command_name = command.name();

                command
                    .run()
                    .instrument(tracing::info_span!(
                        "Running command",
                        %is_transactional,
                        %command_name,
                    ))
                    .await
            },
        )
        .instrument(tracing::debug_span!("app::run_command"))
        .await;

        if is_outbox_processing_required {
            command_result = command_result
                // If successful, then process the Outbox messages while they are present
                .and_then_async(|_| async {
                    let outbox_agent = cli_catalog.get_one::<messaging_outbox::OutboxAgent>()?;
                    outbox_agent
                        .run_while_has_tasks()
                        .await
                        .map_err(CLIError::critical)
                })
                .instrument(tracing::debug_span!(
                    "Consume accumulated the Outbox messages"
                ))
                .await
                // If we had a temporary directory, we move the database from it to the expected
                // location.
                .and_then_async(|_| async {
                    move_initial_database_to_workspace_if_needed(
                        &workspace_layout,
                        maybe_temp_database_path,
                    )
                    .await
                    .map_int_err(CLIError::critical)
                })
                .await;
        }
    }

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

    if let Some(metrics_file) = &output_config.metrics_file {
        if let Ok(mut file) = std::fs::File::create(metrics_file) {
            use prometheus::Encoder as _;
            let _ = prometheus::TextEncoder::new().encode(&metrics_registry.gather(), &mut file);
            eprintln!("Saving metrics to {}", metrics_file.display());
        }
    }

    if let Some(trace_file) = &output_config.trace_file {
        // Run a web server and open the trace in the browser if the environment allows
        let _ = explore::TraceServer::maybe_serve_in_browser(trace_file).await;
    }

    command_result
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn maybe_transactional<F, RF, RT, RE>(
    transactional: bool,
    catalog: Catalog,
    f: F,
) -> Result<RT, RE>
where
    F: FnOnce(Catalog) -> RF,
    RF: Future<Output = Result<RT, RE>>,
    RE: From<InternalError>,
{
    if !transactional {
        f(catalog).await
    } else {
        let transaction_runner = DatabaseTransactionRunner::new(catalog);

        transaction_runner
            .transactional(|transactional_catalog| async move { f(transactional_catalog).await })
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Catalog
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Public only for tests
pub fn configure_base_catalog(
    workspace_layout: &WorkspaceLayout,
    workspace_status: WorkspaceStatus,
    tenancy_config: TenancyConfig,
    system_time: Option<DateTime<Utc>>,
    is_e2e_testing: bool,
) -> CatalogBuilder {
    let mut b = CatalogBuilder::new();

    b.add_value(workspace_layout.clone());
    b.add_value(RunInfoDir::new(&workspace_layout.run_info_dir));
    b.add_value(CacheDir::new(&workspace_layout.cache_dir));
    b.add_value(RemoteReposDir::new(&workspace_layout.repos_dir));

    b.add_value(prometheus::Registry::new());

    b.add::<ContainerRuntime>();

    b.add_value(tenancy_config);

    if let Some(system_time) = system_time {
        b.add_value(SystemTimeSourceStub::new_set(system_time));
        b.bind::<dyn SystemTimeSource, SystemTimeSourceStub>();
    } else {
        b.add::<SystemTimeSourceDefault>();
    }

    b.add::<DidGeneratorDefault>();

    b.add_builder(odf::dataset::DatasetStorageUnitLocalFs::builder(
        workspace_layout.datasets_dir.clone(),
    ));
    b.add::<kamu_datasets_services::DatasetLfsBuilderDatabaseBackedImpl>();

    b.add::<odf::dataset::DatasetFactoryImpl>();

    b.add::<RemoteRepositoryRegistryImpl>();

    b.add::<RemoteAliasesRegistryImpl>();

    b.add::<RemoteAliasResolverImpl>();

    b.add::<ResourceLoaderImpl>();

    b.add::<DataFormatRegistryImpl>();

    b.add::<MetadataQueryServiceImpl>();

    b.add::<FetchService>();

    b.add::<PollingIngestServiceImpl>();

    b.add::<PushIngestExecutorImpl>();
    b.add::<PushIngestPlannerImpl>();

    b.add::<TransformRequestPlannerImpl>();
    b.add::<TransformElaborationServiceImpl>();
    b.add::<TransformExecutorImpl>();

    b.add::<VerificationServiceImpl>();

    b.add::<CompactionPlannerImpl>();
    b.add::<CompactionExecutorImpl>();

    b.add_builder(SearchServiceRemoteImpl::builder(None));

    b.add::<SyncServiceImpl>();
    b.add::<SyncRequestBuilder>();

    b.add::<PullRequestPlannerImpl>();

    b.add::<PushRequestPlannerImpl>();

    b.add::<SetWatermarkPlannerImpl>();
    b.add::<SetWatermarkExecutorImpl>();

    b.add::<RemoteStatusServiceImpl>();

    b.add::<ResetPlannerImpl>();
    b.add::<ResetExecutorImpl>();

    b.add::<ProvenanceServiceImpl>();

    b.add::<QueryServiceImpl>();

    b.add::<ExportServiceImpl>();

    b.add::<ObjectStoreRegistryImpl>();

    b.add::<ObjectStoreBuilderLocalFs>();

    b.add::<EngineProvisionerLocal>();

    b.add::<kamu::utils::simple_transfer_protocol::SimpleTransferProtocol>();
    b.add::<kamu_adapter_http::SmartTransferProtocolClientWs>();

    b.add::<CompactDatasetUseCaseImpl>();
    b.add::<PushIngestDataUseCaseImpl>();
    b.add::<PullDatasetUseCaseImpl>();
    b.add::<PushDatasetUseCaseImpl>();
    b.add::<ResetDatasetUseCaseImpl>();
    b.add::<SetWatermarkUseCaseImpl>();
    b.add::<VerifyDatasetUseCaseImpl>();

    // No GitHub login possible for single-tenant workspace
    if tenancy_config == TenancyConfig::MultiTenant {
        if is_e2e_testing {
            b.add::<kamu_adapter_oauth::DummyOAuthGithub>();
        } else {
            b.add::<kamu_adapter_oauth::OAuthGithub>();
        }
    }

    kamu_accounts_services::register_dependencies(
        &mut b,
        workspace_status.is_indexing_needed(),
        !is_e2e_testing,
    );

    odf_server::register_dependencies(&mut b, is_e2e_testing);

    kamu_adapter_auth_oso_rebac::register_dependencies(&mut b);
    kamu_datasets_services::register_dependencies(&mut b, workspace_status.is_indexing_needed());
    kamu_auth_rebac_services::register_dependencies(&mut b, workspace_status.is_indexing_needed());

    b.add::<DatabaseTransactionRunner>();

    b.add::<kamu_adapter_flight_sql::SessionAuthAnonymous>();
    b.add::<kamu_adapter_flight_sql::SessionManagerCaching>();
    b.add::<kamu_adapter_flight_sql::SessionManagerCachingState>();
    b.add_value(
        kamu_adapter_flight_sql::sql_info::default_sql_info()
            .build()
            .unwrap(),
    );
    b.add::<kamu_adapter_flight_sql::KamuFlightSqlService>();

    b.add_builder(
        messaging_outbox::OutboxImmediateImpl::builder()
            .with_consumer_filter(messaging_outbox::ConsumerFilter::ImmediateConsumers),
    );
    b.add::<messaging_outbox::OutboxTransactionalImpl>();
    b.add::<messaging_outbox::OutboxDispatchingImpl>();
    b.bind::<dyn Outbox, OutboxDispatchingImpl>();
    b.add::<messaging_outbox::OutboxAgent>();
    b.add::<messaging_outbox::OutboxAgentMetrics>();

    explore::register_dependencies(&mut b);

    register_message_dispatcher::<kamu_datasets::DatasetLifecycleMessage>(
        &mut b,
        kamu_datasets::MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
    );

    register_message_dispatcher::<kamu_datasets::DatasetReferenceMessage>(
        &mut b,
        kamu_datasets::MESSAGE_PRODUCER_KAMU_DATASET_REFERENCE_SERVICE,
    );

    register_message_dispatcher::<kamu_datasets::DatasetDependenciesMessage>(
        &mut b,
        kamu_datasets::MESSAGE_PRODUCER_KAMU_DATASET_DEPENDENCY_GRAPH_SERVICE,
    );

    register_message_dispatcher::<kamu_datasets::DatasetExternallyChangedMessage>(
        &mut b,
        kamu_datasets::MESSAGE_PRODUCER_KAMU_HTTP_ADAPTER,
    );

    b
}

// Public only for tests
pub fn configure_cli_catalog(
    base_catalog: &Catalog,
    tenancy_config: TenancyConfig,
) -> CatalogBuilder {
    let mut b = CatalogBuilder::new_chained(base_catalog);

    b.add::<config::ConfigService>();
    b.add::<GcService>();
    b.add_builder(
        WorkspaceService::builder().with_multi_tenant(tenancy_config == TenancyConfig::MultiTenant),
    );
    b.add::<ConfirmDeleteService>();

    b
}

// Public only for tests
pub fn configure_server_catalog(base_catalog: &Catalog) -> CatalogBuilder {
    let mut b = CatalogBuilder::new_chained(base_catalog);

    b.add::<DatasetChangesServiceImpl>();

    kamu_task_system_services::register_dependencies(&mut b);

    kamu_flow_system_services::register_dependencies(&mut b);

    b.add::<UploadServiceLocal>();

    register_message_dispatcher::<FlowProgressMessage>(
        &mut b,
        MESSAGE_PRODUCER_KAMU_FLOW_PROGRESS_SERVICE,
    );
    register_message_dispatcher::<FlowConfigurationUpdatedMessage>(
        &mut b,
        MESSAGE_PRODUCER_KAMU_FLOW_CONFIGURATION_SERVICE,
    );
    register_message_dispatcher::<FlowTriggerUpdatedMessage>(
        &mut b,
        MESSAGE_PRODUCER_KAMU_FLOW_TRIGGER_SERVICE,
    );
    register_message_dispatcher::<TaskProgressMessage>(&mut b, MESSAGE_PRODUCER_KAMU_TASK_AGENT);
    register_message_dispatcher::<AccountLifecycleMessage>(
        &mut b,
        MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE,
    );
    register_message_dispatcher::<AccessTokenLifecycleMessage>(
        &mut b,
        MESSAGE_PRODUCER_KAMU_ACCESS_TOKEN_SERVICE,
    );

    b
}

async fn run_startup_initializations(catalog: &Catalog) -> Result<(), CLIError> {
    let init_result = init_on_startup::run_startup_jobs(catalog)
        .await
        .map_err(CLIError::failure);

    if let Err(e) = init_result {
        tracing::error!(
            error_dbg = ?e,
            error = %e.pretty(true),
            "Initialize components failed",
        );
        return Err(e);
    }

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
    workspace_status: WorkspaceStatus,
    password_hashing_mode: Option<cli::PasswordHashingMode>,
    is_e2e_testing: bool,
) -> Result<(), CLIError> {
    let network_ns = config.engine.as_ref().unwrap().network_ns.unwrap();

    // Register JupyterConfig used by some commands
    catalog_builder.add_value(config.frontend.as_ref().unwrap().jupyter.clone().unwrap());

    // Container runtime configuration
    catalog_builder.add_value(ContainerRuntimeConfig {
        runtime: config.engine.as_ref().unwrap().runtime.unwrap(),
        network_ns,
    });
    //

    // Engine configuration
    {
        let engine_config = config.engine.clone().unwrap();
        let images_config = engine_config.images.unwrap();

        catalog_builder.add_value(EngineProvisionerLocalConfig {
            max_concurrency: engine_config.max_concurrency,
            start_timeout: engine_config.start_timeout.unwrap().into(),
            shutdown_timeout: engine_config.shutdown_timeout.unwrap().into(),
            spark_image: images_config.spark.clone().unwrap(),
            flink_image: images_config.flink.clone().unwrap(),
            datafusion_image: images_config.datafusion.clone().unwrap(),
            risingwave_image: images_config.risingwave.clone().unwrap(),
        });

        let (ingest_config, batch_query_config, compaction_config) =
            engine_config.datafusion_embedded.unwrap().into_system()?;
        catalog_builder.add_value(ingest_config);
        catalog_builder.add_value(batch_query_config);
        catalog_builder.add_value(compaction_config);
    }
    //

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

    // Identity configuration
    if let Some(identity_config) = config.identity.as_ref().unwrap().to_infra_cfg() {
        catalog_builder.add_value(identity_config);
    }
    //

    // IPFS configuration
    let ipfs_conf = config.protocol.as_ref().unwrap().ipfs.as_ref().unwrap();

    catalog_builder.add_value(odf::dataset::IpfsGateway {
        url: ipfs_conf.http_gateway.clone().unwrap(),
        pre_resolve_dnslink: ipfs_conf.pre_resolve_dnslink.unwrap(),
    });
    catalog_builder.add_value(kamu::utils::ipfs_wrapper::IpfsClient::default());
    //

    // Flight SQL configuration
    let flight_sql_conf = config
        .protocol
        .as_ref()
        .unwrap()
        .flight_sql
        .as_ref()
        .unwrap();
    catalog_builder.add_value(flight_sql_conf.to_session_auth_config());
    catalog_builder.add_value(flight_sql_conf.to_session_caching_config());
    //

    // Tenancy configuration
    if let Some(tenancy_config) = workspace_status.into_tenancy_config() {
        if tenancy_config == TenancyConfig::MultiTenant {
            let mut implicit_user_config = PredefinedAccountsConfig::new();
            implicit_user_config.predefined.push(
                AccountConfig::test_config_from_name(odf::AccountName::new_unchecked(
                    AccountService::default_account_name(TenancyConfig::MultiTenant).as_str(),
                ))
                .set_display_name(AccountService::default_user_name(
                    TenancyConfig::MultiTenant,
                )),
            );

            if is_e2e_testing {
                let e2e_user_config = AccountConfig::test_config_from_name(
                    odf::AccountName::new_unchecked("e2e-user"),
                );

                implicit_user_config.predefined.push(e2e_user_config);
            }

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
    } else {
        // No workspace
        catalog_builder.add_value(PredefinedAccountsConfig::new());
    }
    //

    // Uploads configuration
    let uploads_config = config.uploads.as_ref().unwrap();
    catalog_builder.add_value(FileUploadLimitConfig::new_in_mb(
        uploads_config.max_file_size_in_mb.unwrap(),
    ));
    //

    // Dataset env vars configuration
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
                    kamu_datasets::DatasetEnvVar::try_asm_256_gcm_from_str(encryption_key).is_ok(),
                    "Invalid dataset env var encryption key",
                );
                catalog_builder.add::<kamu_datasets_services::DatasetKeyValueServiceImpl>();
                catalog_builder.add::<kamu_datasets_services::DatasetEnvVarServiceImpl>();
            }
        }
    }
    //

    // Outbox configuration
    let outbox_config = config.outbox.as_ref().unwrap();
    catalog_builder.add_value(messaging_outbox::OutboxConfig::new(
        Duration::seconds(outbox_config.awaiting_step_secs.unwrap()),
        outbox_config.batch_size.unwrap(),
    ));
    //

    // Password hashing mode configuration
    match password_hashing_mode {
        Some(cli::PasswordHashingMode::Testing) => {
            catalog_builder.add_value(kamu_accounts_services::PasswordHashingMode::Minimal);
        }
        Some(cli::PasswordHashingMode::Production) | None => {
            catalog_builder.add_value(kamu_accounts_services::PasswordHashingMode::Default);
        }
    }
    //

    // Flow system configuration
    let kamu_flow_system_config = config.flow_system.as_ref().unwrap();
    let flow_agent_config = kamu_flow_system_config.flow_agent.as_ref().unwrap();

    catalog_builder.add_value(kamu_flow_system_inmem::domain::FlowAgentConfig::new(
        Duration::seconds(flow_agent_config.awaiting_step_secs.unwrap()),
        Duration::seconds(flow_agent_config.mandatory_throttling_period_secs.unwrap()),
    ));

    let task_agent_config = kamu_flow_system_config.task_agent.as_ref().unwrap();
    catalog_builder.add_value(kamu_task_system_inmem::domain::TaskAgentConfig::new(
        Duration::seconds(task_agent_config.task_checking_interval_secs.unwrap()),
    ));
    //

    // Search configuration
    let crate::config::SearchConfig {
        indexer,
        embeddings_chunker,
        embeddings_encoder,
        vector_repo,
        overfetch_factor,
        overfetch_amount,
    } = config.search.clone().unwrap();

    catalog_builder.add::<kamu_search_services::SearchServiceLocalImplLazyInit>();
    catalog_builder.add_value(kamu_search_services::SearchServiceLocalConfig {
        overfetch_factor: overfetch_factor.unwrap(),
        overfetch_amount: overfetch_amount.unwrap(),
    });

    let indexer = indexer.unwrap_or_default();
    catalog_builder.add_value(kamu_search_services::SearchServiceLocalIndexerConfig {
        clear_on_start: indexer.clear_on_start,
        skip_datasets_with_no_description: indexer.skip_datasets_with_no_description,
        skip_datasets_with_no_data: indexer.skip_datasets_with_no_data,
        payload_include_content: indexer.payload_include_content,
    });

    match embeddings_chunker.unwrap_or_default() {
        config::EmbeddingsChunkerConfig::Simple(cfg) => {
            catalog_builder.add::<kamu_search_services::EmbeddingsChunkerSimple>();
            catalog_builder.add_value(kamu_search_services::EmbeddingsChunkerConfigSimple {
                split_sections: cfg.split_sections.unwrap(),
                split_paragraphs: cfg.split_paragraphs.unwrap(),
            });
        }
    }

    match embeddings_encoder.unwrap_or_default() {
        config::EmbeddingsEncoderConfig::OpenAi(mut cfg) => {
            cfg.merge(config::EmbeddingsEncoderConfigOpenAi::default());

            catalog_builder.add::<kamu_search_openai::EmbeddingsEncoderOpenAi>();
            catalog_builder.add_value(kamu_search_openai::EmbeddingsEncoderConfigOpenAI {
                url: cfg.url,
                api_key: cfg.api_key.map(Into::into),
                model_name: cfg.model_name.unwrap(),
                dimensions: cfg.dimensions.unwrap(),
            });
        }
    }

    match vector_repo.unwrap_or_default() {
        config::VectorRepoConfig::Qdrant(mut cfg) => {
            cfg.merge(config::VectorRepoConfigQdrant::default());

            catalog_builder.add::<kamu_search_qdrant::VectorRepositoryQdrant>();
            catalog_builder.add_value(kamu_search_qdrant::VectorRepositoryConfigQdrant {
                url: cfg.url,
                collection_name: cfg.collection_name.unwrap(),
                dimensions: cfg.dimensions.unwrap(),
            });
        }
        config::VectorRepoConfig::QdrantContainer(mut cfg) => {
            cfg.merge(config::VectorRepoConfigQdrantContainer::default());

            catalog_builder.add::<kamu_search_qdrant::VectorRepositoryQdrantContainer>();
            catalog_builder.add_value(kamu_search_qdrant::VectorRepositoryConfigQdrantContainer {
                image: cfg.image.unwrap(),
                dimensions: cfg.dimensions.unwrap(),
                start_timeout: cfg.start_timeout.unwrap().into(),
            });
        }
    }
    //

    Ok(())
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum WorkspaceStatus {
    NoWorkspace,
    AboutToBeCreated(TenancyConfig),
    Created(TenancyConfig),
}

impl WorkspaceStatus {
    fn into_tenancy_config(self) -> Option<TenancyConfig> {
        match self {
            WorkspaceStatus::NoWorkspace => None,
            WorkspaceStatus::AboutToBeCreated(tenancy_config)
            | WorkspaceStatus::Created(tenancy_config) => Some(tenancy_config),
        }
    }

    fn is_indexing_needed(self) -> bool {
        match self {
            WorkspaceStatus::NoWorkspace => false,
            WorkspaceStatus::AboutToBeCreated(_) | WorkspaceStatus::Created(_) => true,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Logging
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn prepare_run_dir(run_dir: &Path) {
    if run_dir.exists() {
        std::fs::remove_dir_all(run_dir).unwrap_or_else(|e| {
            panic!(
                "Unable to clean up run directory {}: {e}",
                run_dir.display(),
            )
        });
        std::fs::create_dir(run_dir).unwrap_or_else(|e| {
            panic!("Unable to create run directory {}: {e}", run_dir.display(),)
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
        Err(_) => EnvFilter::new(
            LOG_LEVELS[(output_config.verbosity_level as usize).clamp(0, LOG_LEVELS.len() - 1)],
        ),
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

fn configure_output_format(args: &cli::Cli, workspace_svc: &WorkspaceService) -> OutputConfig {
    let is_tty = console::Term::stdout().features().is_attended();

    let trace_file = if args.trace && workspace_svc.is_in_workspace() {
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

    let metrics_file = if args.metrics && workspace_svc.is_in_workspace() {
        Some(
            workspace_svc
                .layout()
                .unwrap()
                .run_info_dir
                .join("kamu.metrics.txt"),
        )
    } else {
        None
    };

    let format = args.tabular_output_format().unwrap_or(if is_tty {
        OutputFormat::Table
    } else {
        OutputFormat::Json
    });

    OutputConfig {
        quiet: args.quiet,
        verbosity_level: args.verbose,
        is_tty,
        format,
        trace_file,
        metrics_file,
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
