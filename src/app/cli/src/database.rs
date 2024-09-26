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
use dill::{Catalog, CatalogBuilder, Component};
use internal_error::{InternalError, ResultIntoInternal};
use secrecy::SecretString;
use tempfile::TempDir;

use crate::config::{DatabaseConfig, DatabaseCredentialSourceConfig, RemoteDatabaseConfig};
use crate::{config, WorkspaceLayout, DEFAULT_MULTI_TENANT_SQLITE_DATABASE_NAME};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum AppDatabaseConfig {
    /// No settings are specified
    None,
    /// The user has specified custom database settings
    Explicit(DatabaseConfig),
    /// No settings are specified, default settings will be used
    DefaultMultiTenant(DatabaseConfig),
    /// Since there is no workspace, we use a temporary directory to create the
    /// database
    DefaultMultiTenantInitCommand(DatabaseConfig, OwnedTempPath),
}

impl AppDatabaseConfig {
    pub fn into_inner(self) -> (Option<DatabaseConfig>, Option<OwnedTempPath>) {
        match self {
            AppDatabaseConfig::None => (None, None),
            AppDatabaseConfig::Explicit(c) | AppDatabaseConfig::DefaultMultiTenant(c) => {
                (Some(c), None)
            }
            AppDatabaseConfig::DefaultMultiTenantInitCommand(c, path) => (Some(c), Some(path)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn get_app_database_config(
    workspace_layout: &WorkspaceLayout,
    config: &config::CLIConfig,
    multi_tenant_workspace: bool,
    init_command: bool,
) -> AppDatabaseConfig {
    if let Some(database_config) = config.database.clone() {
        return AppDatabaseConfig::Explicit(database_config);
    }

    if !multi_tenant_workspace {
        // Default for multi-tenant workspace only
        return AppDatabaseConfig::None;
    };

    let database_path = workspace_layout.default_multi_tenant_database_path();
    let database_not_exist = !database_path.exists();

    // Note: do not overwrite the database if present
    if init_command && database_not_exist {
        let temp_dir = tempfile::tempdir().unwrap();
        let database_path = temp_dir
            .as_ref()
            .join(DEFAULT_MULTI_TENANT_SQLITE_DATABASE_NAME);
        let config = DatabaseConfig::sqlite(&database_path);
        let temp_database_path = OwnedTempPath::new(database_path, temp_dir);

        AppDatabaseConfig::DefaultMultiTenantInitCommand(config, temp_database_path)
    } else {
        // Use already created database
        AppDatabaseConfig::DefaultMultiTenant(DatabaseConfig::sqlite(&database_path))
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
            workspace_layout.default_multi_tenant_database_path(),
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

            b.add::<kamu_datasets_postgres::PostgresDatasetEnvVarRepository>();

            b.add::<kamu_flow_system_postgres::PostgresFlowConfigurationEventStore>();
            b.add::<kamu_flow_system_postgres::PostgresFlowEventStore>();

            b.add::<kamu_task_system_postgres::PostgresTaskEventStore>();

            b.add::<kamu_messaging_outbox_postgres::PostgresOutboxMessageRepository>();
            b.add::<kamu_messaging_outbox_postgres::PostgresOutboxMessageConsumptionRepository>();

            // TODO: Private Datasets: implement database-related version
            b.add::<kamu_auth_rebac_inmem::InMemoryRebacRepository>();
        }
        DatabaseProvider::MySql | DatabaseProvider::MariaDB => {
            MySqlPlugin::init_database_components(b);

            // TODO: many components are not implemented for MySQL
            //    and are substituted with in-memory equivalents

            b.add::<kamu_accounts_mysql::MySqlAccountRepository>();
            b.add::<kamu_accounts_mysql::MySqlAccessTokenRepository>();

            b.add::<kamu_datasets_inmem::InMemoryDatasetEnvVarRepository>();

            b.add::<kamu_flow_system_inmem::InMemoryFlowConfigurationEventStore>();
            b.add::<kamu_flow_system_inmem::InMemoryFlowEventStore>();

            b.add::<kamu_task_system_inmem::InMemoryTaskEventStore>();

            b.add::<kamu_messaging_outbox_inmem::InMemoryOutboxMessageRepository>();
            b.add::<kamu_messaging_outbox_inmem::InMemoryOutboxMessageConsumptionRepository>();

            b.add::<kamu_auth_rebac_inmem::InMemoryRebacRepository>();
        }
        DatabaseProvider::Sqlite => {
            SqlitePlugin::init_database_components(b);

            b.add::<kamu_accounts_sqlite::SqliteAccountRepository>();
            b.add::<kamu_accounts_sqlite::SqliteAccessTokenRepository>();

            b.add::<kamu_datasets_sqlite::SqliteDatasetEnvVarRepository>();

            b.add::<kamu_flow_system_sqlite::SqliteFlowConfigurationEventStore>();
            b.add::<kamu_flow_system_sqlite::SqliteFlowEventStore>();

            b.add::<kamu_task_system_sqlite::SqliteTaskSystemEventStore>();

            b.add::<kamu_messaging_outbox_sqlite::SqliteOutboxMessageRepository>();
            b.add::<kamu_messaging_outbox_sqlite::SqliteOutboxMessageConsumptionRepository>();

            b.add::<kamu_auth_rebac_sqlite::SqliteRebacRepository>();
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
    b.add::<kamu_flow_system_inmem::InMemoryFlowConfigurationEventStore>();
    b.add::<kamu_flow_system_inmem::InMemoryFlowEventStore>();
    b.add::<kamu_task_system_inmem::InMemoryTaskEventStore>();
    b.add::<kamu_datasets_inmem::InMemoryDatasetEnvVarRepository>();
    b.add::<kamu_auth_rebac_inmem::InMemoryRebacRepository>();

    NoOpDatabasePlugin::init_database_components(b);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn try_build_db_connection_settings(
    raw_db_config: &DatabaseConfig,
) -> Option<DatabaseConnectionSettings> {
    fn convert(c: &RemoteDatabaseConfig, provider: DatabaseProvider) -> DatabaseConnectionSettings {
        DatabaseConnectionSettings::new(provider, c.database_name.clone(), c.host.clone(), c.port)
    }

    match raw_db_config {
        DatabaseConfig::Sqlite(c) => {
            let path = Path::new(&c.database_path);
            Some(DatabaseConnectionSettings::sqlite_from(path))
        }
        DatabaseConfig::Postgres(config) => Some(convert(config, DatabaseProvider::Postgres)),
        DatabaseConfig::MySql(config) => Some(convert(config, DatabaseProvider::MySql)),
        DatabaseConfig::MariaDB(config) => Some(convert(config, DatabaseProvider::MariaDB)),
        DatabaseConfig::InMemory => None,
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
        DatabaseConfig::Sqlite(_) | DatabaseConfig::InMemory => None,
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
        DatabaseConfig::InMemory => unreachable!(),
        DatabaseConfig::Sqlite(_) => {
            b.add::<DatabaseNoPasswordProvider>();
        }
        DatabaseConfig::MySql(config)
        | DatabaseConfig::Postgres(config)
        | DatabaseConfig::MariaDB(config) => match &config.credentials_policy.source {
            DatabaseCredentialSourceConfig::RawPassword(raw_password_config) => {
                b.add_builder(
                    DatabaseFixedPasswordProvider::builder()
                        .with_db_user_name(SecretString::from(
                            raw_password_config.user_name.clone(),
                        ))
                        .with_fixed_password(SecretString::from(
                            raw_password_config.raw_password.clone(),
                        )),
                );
                b.bind::<dyn DatabasePasswordProvider, DatabaseFixedPasswordProvider>();
            }
            DatabaseCredentialSourceConfig::AwsSecret(aws_secret_config) => {
                b.add_builder(
                    DatabaseAwsSecretPasswordProvider::builder()
                        .with_secret_name(aws_secret_config.secret_name.clone()),
                );
                b.bind::<dyn DatabasePasswordProvider, DatabaseAwsSecretPasswordProvider>();
            }
            DatabaseCredentialSourceConfig::AwsIamToken(aws_iam_config) => {
                b.add_builder(
                    DatabaseAwsIamTokenProvider::builder()
                        .with_db_user_name(SecretString::from(aws_iam_config.user_name.clone())),
                );
                b.bind::<dyn DatabasePasswordProvider, DatabaseAwsIamTokenProvider>();
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
