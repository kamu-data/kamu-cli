// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::{Path, PathBuf};

use database_common::*;
use dill::{Catalog, CatalogBuilder};
use internal_error::{InternalError, ResultIntoInternal};
use secrecy::SecretString;
use tempfile::TempDir;

use crate::config::{DatabaseConfig, DatabaseCredentialSourceConfig, RemoteDatabaseConfig};
use crate::{config, WorkspaceLayout, WorkspaceStatus};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum AppDatabaseConfig {
    /// No settings are specified
    None,
    /// The user has specified custom database settings
    Explicit(DatabaseConfig),
    /// No settings are specified, default settings will be used
    Default(DatabaseConfig),
    /// Since there is no workspace, we use a temporary directory to create the
    /// database
    DefaultInitCommand(DatabaseConfig, OwnedTempPath),
}

impl AppDatabaseConfig {
    pub fn into_inner(self) -> (Option<DatabaseConfig>, Option<OwnedTempPath>) {
        match self {
            AppDatabaseConfig::None => (None, None),
            AppDatabaseConfig::Explicit(c) | AppDatabaseConfig::Default(c) => (Some(c), None),
            AppDatabaseConfig::DefaultInitCommand(c, path) => (Some(c), Some(path)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn get_app_database_config(
    workspace_layout: &WorkspaceLayout,
    config: &config::CLIConfig,
    workspace_status: WorkspaceStatus,
) -> AppDatabaseConfig {
    if workspace_status == WorkspaceStatus::NoWorkspace {
        return AppDatabaseConfig::None;
    }

    if let Some(database_config) = config.database.clone() {
        return AppDatabaseConfig::Explicit(database_config);
    }

    let database_path = workspace_layout.default_workspace_database_path();
    let database_not_exist = !database_path.exists();

    // Note: do not overwrite the database if present
    if let WorkspaceStatus::AboutToBeCreated(_) = workspace_status
        && database_not_exist
    {
        let temp_dir = tempfile::tempdir().unwrap();
        let database_path = temp_dir
            .as_ref()
            .join(DEFAULT_WORKSPACE_SQLITE_DATABASE_NAME);
        let config = DatabaseConfig::sqlite(&database_path);
        let temp_database_path = OwnedTempPath::new(database_path, temp_dir);

        AppDatabaseConfig::DefaultInitCommand(config, temp_database_path)
    } else {
        // Use already created database
        AppDatabaseConfig::Default(DatabaseConfig::sqlite(&database_path))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn move_initial_database_to_workspace_if_needed(
    workspace_layout: &WorkspaceLayout,
    maybe_temp_database_path: Option<OwnedTempPath>,
) -> Result<(), std::io::Error> {
    if let Some(temp_database_path) = maybe_temp_database_path {
        tokio::fs::copy(
            temp_database_path.path(),
            workspace_layout.default_workspace_database_path(),
        )
        .await?;
    };

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn configure_database_components(
    b: &mut CatalogBuilder,
    raw_db_config: &DatabaseConfig,
    db_connection_settings: DatabaseConnectionSettings,
) {
    match db_connection_settings.provider {
        DatabaseProvider::Postgres => {
            PostgresPlugin::init_database_components(b);

            b.add::<kamu_accounts_postgres::PostgresAccountRepository>();
            b.add::<kamu_accounts_postgres::PostgresAccessTokenRepository>();
            b.add::<kamu_accounts_postgres::PostgresOAuthDeviceCodeRepository>();
            b.add::<kamu_accounts_postgres::PostgresDidSecretKeyRepository>();

            b.add::<kamu_datasets_postgres::PostgresDatasetEnvVarRepository>();
            b.add::<kamu_datasets_postgres::PostgresDatasetEntryRepository>();
            b.add::<kamu_datasets_postgres::PostgresDatasetDependencyRepository>();
            b.add::<kamu_datasets_postgres::PostgresDatasetReferenceRepository>();
            b.add::<kamu_datasets_postgres::PostgresDatasetStatisticsRepository>();
            b.add::<kamu_datasets_postgres::PostgresDatasetKeyBlockRepository>();

            b.add::<kamu_flow_system_postgres::PostgresFlowConfigurationEventStore>();
            b.add::<kamu_flow_system_postgres::PostgresFlowTriggerEventStore>();
            b.add::<kamu_flow_system_postgres::PostgresFlowEventStore>();

            b.add::<kamu_task_system_postgres::PostgresTaskEventStore>();

            b.add::<kamu_messaging_outbox_postgres::PostgresOutboxMessageRepository>();
            b.add::<kamu_messaging_outbox_postgres::PostgresOutboxMessageConsumptionRepository>();

            b.add::<kamu_auth_rebac_postgres::PostgresRebacRepository>();

            b.add::<kamu_webhooks_postgres::PostgresWebhookEventRepository>();
            b.add::<kamu_webhooks_postgres::PostgresWebhookDeliveryRepository>();
            b.add::<kamu_webhooks_postgres::PostgresWebhookSubscriptionEventStore>();

            b.add::<kamu_auth_web3_postgres::PostgresWeb3AuthEip4361NonceRepository>();
        }
        DatabaseProvider::MySql | DatabaseProvider::MariaDB => {
            MySqlPlugin::init_database_components(b);

            // TODO: many components are not implemented for MySQL
            //    and are substituted with in-memory equivalents

            b.add::<kamu_accounts_mysql::MySqlAccountRepository>();
            b.add::<kamu_accounts_mysql::MySqlAccessTokenRepository>();
            b.add::<kamu_accounts_inmem::InMemoryOAuthDeviceCodeRepository>();
            b.add::<kamu_accounts_inmem::InMemoryDidSecretKeyRepository>();

            b.add::<kamu_datasets_inmem::InMemoryDatasetEnvVarRepository>();
            b.add::<kamu_datasets_inmem::InMemoryDatasetEntryRepository>();
            b.add::<kamu_datasets_inmem::InMemoryDatasetDependencyRepository>();
            b.add::<kamu_datasets_inmem::InMemoryDatasetReferenceRepository>();
            b.add::<kamu_datasets_inmem::InMemoryDatasetStatisticsRepository>();
            b.add::<kamu_datasets_inmem::InMemoryDatasetKeyBlockRepository>();

            b.add::<kamu_flow_system_inmem::InMemoryFlowConfigurationEventStore>();
            b.add::<kamu_flow_system_inmem::InMemoryFlowTriggerEventStore>();
            b.add::<kamu_flow_system_inmem::InMemoryFlowEventStore>();

            b.add::<kamu_task_system_inmem::InMemoryTaskEventStore>();

            b.add::<kamu_messaging_outbox_inmem::InMemoryOutboxMessageRepository>();
            b.add::<kamu_messaging_outbox_inmem::InMemoryOutboxMessageConsumptionRepository>();

            b.add::<kamu_auth_rebac_inmem::InMemoryRebacRepository>();

            b.add::<kamu_webhooks_inmem::InMemoryWebhookEventRepository>();
            b.add::<kamu_webhooks_inmem::InMemoryWebhookDeliveryRepository>();
            b.add::<kamu_webhooks_inmem::InMemoryWebhookSubscriptionEventStore>();

            b.add::<kamu_auth_web3_inmem::InMemoryWeb3AuthEip4361NonceRepository>();
        }
        DatabaseProvider::Sqlite => {
            SqlitePlugin::init_database_components(b);

            b.add::<kamu_accounts_sqlite::SqliteAccountRepository>();
            b.add::<kamu_accounts_sqlite::SqliteAccessTokenRepository>();
            b.add::<kamu_accounts_sqlite::SqliteOAuthDeviceCodeRepository>();
            b.add::<kamu_accounts_sqlite::SqliteDidSecretKeyRepository>();

            b.add::<kamu_datasets_sqlite::SqliteDatasetEnvVarRepository>();
            b.add::<kamu_datasets_sqlite::SqliteDatasetEntryRepository>();
            b.add::<kamu_datasets_sqlite::SqliteDatasetDependencyRepository>();
            b.add::<kamu_datasets_sqlite::SqliteDatasetReferenceRepository>();
            b.add::<kamu_datasets_sqlite::SqliteDatasetStatisticsRepository>();
            b.add::<kamu_datasets_sqlite::SqliteDatasetKeyBlockRepository>();

            b.add::<kamu_flow_system_sqlite::SqliteFlowConfigurationEventStore>();
            b.add::<kamu_flow_system_sqlite::SqliteFlowTriggerEventStore>();
            b.add::<kamu_flow_system_sqlite::SqliteFlowEventStore>();

            b.add::<kamu_task_system_sqlite::SqliteTaskEventStore>();

            b.add::<kamu_messaging_outbox_sqlite::SqliteOutboxMessageRepository>();
            b.add::<kamu_messaging_outbox_sqlite::SqliteOutboxMessageConsumptionRepository>();

            b.add::<kamu_auth_rebac_sqlite::SqliteRebacRepository>();

            b.add::<kamu_webhooks_sqlite::SqliteWebhookEventRepository>();
            b.add::<kamu_webhooks_sqlite::SqliteWebhookDeliveryRepository>();
            b.add::<kamu_webhooks_sqlite::SqliteWebhookSubscriptionEventStore>();

            b.add::<kamu_auth_web3_sqlite::SqliteWeb3AuthEip4361NonceRepository>();
        }
    }

    b.add_value(db_connection_settings);

    init_database_password_provider(b, raw_db_config);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Public only for tests
pub fn configure_in_memory_components(b: &mut CatalogBuilder) {
    b.add::<kamu_messaging_outbox_inmem::InMemoryOutboxMessageRepository>();
    b.add::<kamu_messaging_outbox_inmem::InMemoryOutboxMessageConsumptionRepository>();

    b.add::<kamu_accounts_inmem::InMemoryAccountRepository>();
    b.add::<kamu_accounts_inmem::InMemoryAccessTokenRepository>();
    b.add::<kamu_accounts_inmem::InMemoryOAuthDeviceCodeRepository>();
    b.add::<kamu_accounts_inmem::InMemoryDidSecretKeyRepository>();

    b.add::<kamu_flow_system_inmem::InMemoryFlowConfigurationEventStore>();
    b.add::<kamu_flow_system_inmem::InMemoryFlowTriggerEventStore>();
    b.add::<kamu_flow_system_inmem::InMemoryFlowEventStore>();

    b.add::<kamu_task_system_inmem::InMemoryTaskEventStore>();

    b.add::<kamu_datasets_inmem::InMemoryDatasetEnvVarRepository>();
    b.add::<kamu_datasets_inmem::InMemoryDatasetEntryRepository>();
    b.add::<kamu_datasets_inmem::InMemoryDatasetDependencyRepository>();
    b.add::<kamu_datasets_inmem::InMemoryDatasetReferenceRepository>();
    b.add::<kamu_datasets_inmem::InMemoryDatasetStatisticsRepository>();
    b.add::<kamu_datasets_inmem::InMemoryDatasetKeyBlockRepository>();

    b.add::<kamu_auth_rebac_inmem::InMemoryRebacRepository>();

    b.add::<kamu_webhooks_inmem::InMemoryWebhookEventRepository>();
    b.add::<kamu_webhooks_inmem::InMemoryWebhookDeliveryRepository>();
    b.add::<kamu_webhooks_inmem::InMemoryWebhookSubscriptionEventStore>();

    b.add::<kamu_auth_web3_inmem::InMemoryWeb3AuthEip4361NonceRepository>();

    NoOpDatabasePlugin::init_database_components(b);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn build_db_connection_settings(raw_db_config: &DatabaseConfig) -> DatabaseConnectionSettings {
    fn convert(c: &RemoteDatabaseConfig, provider: DatabaseProvider) -> DatabaseConnectionSettings {
        DatabaseConnectionSettings::new(
            provider,
            c.database_name.clone(),
            c.host.clone(),
            c.port,
            c.max_connections,
            c.max_lifetime_secs,
            c.acquire_timeout_secs,
        )
    }

    match raw_db_config {
        DatabaseConfig::Sqlite(c) => {
            let path = Path::new(&c.database_path);
            DatabaseConnectionSettings::sqlite_from(path)
        }
        DatabaseConfig::Postgres(config) => convert(config, DatabaseProvider::Postgres),
        DatabaseConfig::MySql(config) => convert(config, DatabaseProvider::MySql),
        DatabaseConfig::MariaDB(config) => convert(config, DatabaseProvider::MariaDB),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn connect_database_initially(base_catalog: &Catalog) -> Result<Catalog, InternalError> {
    let db_connection_settings = base_catalog
        .get_one::<DatabaseConnectionSettings>()
        .unwrap();
    let db_password_provider = base_catalog
        .get_one::<dyn DatabasePasswordProvider>()
        .unwrap();

    let db_credentials = db_password_provider.provide_credentials().await?;

    match db_connection_settings.provider {
        DatabaseProvider::Postgres => PostgresPlugin::catalog_with_connected_pool(
            base_catalog,
            &db_connection_settings,
            db_credentials.as_ref(),
        )
        .int_err(),
        DatabaseProvider::MySql | DatabaseProvider::MariaDB => {
            MySqlPlugin::catalog_with_connected_pool(
                base_catalog,
                &db_connection_settings,
                db_credentials.as_ref(),
            )
            .int_err()
        }
        DatabaseProvider::Sqlite => {
            SqlitePlugin::catalog_with_connected_pool(base_catalog, &db_connection_settings)
                .await
                .int_err()
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn spawn_password_refreshing_job(db_config: &DatabaseConfig, catalog: &Catalog) {
    let credentials_policy_config = match db_config {
        DatabaseConfig::Sqlite(_) => None,
        DatabaseConfig::Postgres(config)
        | DatabaseConfig::MySql(config)
        | DatabaseConfig::MariaDB(config) => Some(config.credentials_policy.clone()),
    };

    if let Some(rotation_frequency_in_minutes) =
        credentials_policy_config.and_then(|config| config.rotation_frequency_in_minutes)
    {
        let awaiting_duration = std::time::Duration::from_mins(rotation_frequency_in_minutes);

        let catalog = catalog.clone();

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(awaiting_duration).await;

                let password_refresher =
                    catalog.get_one::<dyn DatabasePasswordRefresher>().unwrap();
                password_refresher
                    .refresh_password()
                    .await
                    .expect("Password refreshing failed");
            }
        });
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn init_database_password_provider(b: &mut CatalogBuilder, raw_db_config: &DatabaseConfig) {
    match raw_db_config {
        DatabaseConfig::Sqlite(_) => {
            b.add::<DatabaseNoPasswordProvider>();
        }
        DatabaseConfig::MySql(config)
        | DatabaseConfig::Postgres(config)
        | DatabaseConfig::MariaDB(config) => match &config.credentials_policy.source {
            DatabaseCredentialSourceConfig::RawPassword(raw_password_config) => {
                b.add_builder(DatabaseFixedPasswordProvider::builder(
                    SecretString::from(raw_password_config.user_name.clone()),
                    SecretString::from(raw_password_config.raw_password.clone()),
                ));
            }
            DatabaseCredentialSourceConfig::AwsSecret(aws_secret_config) => {
                b.add_builder(DatabaseAwsSecretPasswordProvider::builder(
                    aws_secret_config.secret_name.clone(),
                ));
            }
            DatabaseCredentialSourceConfig::AwsIamToken(aws_iam_config) => {
                b.add_builder(DatabaseAwsIamTokenProvider::builder(SecretString::from(
                    aws_iam_config.user_name.clone(),
                )));
            }
        },
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct OwnedTempPath {
    path: PathBuf,
    _temp_dir: TempDir,
}

impl OwnedTempPath {
    pub fn new(path: PathBuf, temp_dir: TempDir) -> Self {
        Self {
            path,
            _temp_dir: temp_dir,
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
