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

use database_common::*;
use dill::CatalogBuilder;
use internal_error::{InternalError, ResultIntoInternal};
use secrecy::Secret;

use crate::config::{
    DatabaseConfig,
    DatabasePasswordPolicyConfig,
    DatabasePasswordSourceConfig,
    RemoteDatabaseConfig,
};

///////////////////////////////////////////////////////////////////////////////

pub async fn configure_database_components(
    b: &mut CatalogBuilder,
    raw_db_config: &DatabaseConfig,
    db_credentials: DatabaseCredentials,
) -> Result<(), InternalError> {
    // TODO: Remove after adding implementation of FlowEventStore for databases
    b.add::<kamu_flow_system_inmem::FlowEventStoreInMem>();

    // TODO: Delete after preparing services for transactional work and replace with
    //       permanent storage options
    b.add::<kamu_flow_system_inmem::FlowConfigurationEventStoreInMem>();
    b.add::<kamu_task_system_inmem::TaskSystemEventStoreInMemory>();

    let db_password_provider = make_database_password_provider(raw_db_config, &db_credentials);
    let db_password = db_password_provider.provide_password().await?;
    let connection_string = db_credentials.connection_string(db_password);

    match db_credentials.provider {
        DatabaseProvider::Postgres => {
            database_common::PostgresPlugin::init_database_components(b, &connection_string)
                .int_err()?;

            b.add::<kamu_accounts_postgres::PostgresAccountRepository>();
            b.add::<kamu_accounts_postgres::PostgresAccessTokenRepository>();
        }
        DatabaseProvider::MySql | DatabaseProvider::MariaDB => {
            database_common::MySqlPlugin::init_database_components(b, &connection_string)
                .int_err()?;

            b.add::<kamu_accounts_mysql::MySqlAccountRepository>();
            b.add::<kamu_accounts_mysql::MysqlAccessTokenRepository>();

            // TODO: Task & Flow System MySQL versions
        }
        DatabaseProvider::Sqlite => {
            database_common::SqlitePlugin::init_database_components(b, &connection_string)
                .int_err()?;

            b.add::<kamu_accounts_sqlite::SqliteAccountRepository>();
            b.add::<kamu_accounts_sqlite::SqliteAccessTokenRepository>();
        }
    }

    b.add_value(db_credentials);
    b.add_value(db_password_provider);

    Ok(())
}

///////////////////////////////////////////////////////////////////////////////

// Public only for tests
pub fn configure_in_memory_components(b: &mut CatalogBuilder) {
    b.add::<kamu_accounts_inmem::AccountRepositoryInMemory>();
    b.add::<kamu_accounts_inmem::AccessTokenRepositoryInMemory>();
    b.add::<kamu_flow_system_inmem::FlowConfigurationEventStoreInMem>();
    b.add::<kamu_flow_system_inmem::FlowEventStoreInMem>();
    b.add::<kamu_task_system_inmem::TaskSystemEventStoreInMemory>();

    database_common::NoOpDatabasePlugin::init_database_components(b);
}

///////////////////////////////////////////////////////////////////////////////

pub fn try_build_db_credentials(raw_db_config: DatabaseConfig) -> Option<DatabaseCredentials> {
    fn convert(c: RemoteDatabaseConfig, provider: DatabaseProvider) -> DatabaseCredentials {
        DatabaseCredentials::new(provider, c.user, c.database_name, c.host, c.port)
    }

    match raw_db_config {
        DatabaseConfig::Sqlite(c) => {
            let path = Path::new(&c.database_path);
            Some(DatabaseCredentials::sqlite_from(path))
        }
        DatabaseConfig::Postgres(config) => Some(convert(config, DatabaseProvider::Postgres)),
        DatabaseConfig::MySql(config) => Some(convert(config, DatabaseProvider::MySql)),
        DatabaseConfig::MariaDB(config) => Some(convert(config, DatabaseProvider::MariaDB)),
        DatabaseConfig::InMemory => None,
    }
}

///////////////////////////////////////////////////////////////////////////////

fn make_database_password_provider(
    raw_db_config: &DatabaseConfig,
    db_credentials: &DatabaseCredentials,
) -> Arc<dyn DatabasePasswordProvider> {
    match raw_db_config {
        DatabaseConfig::InMemory => unreachable!(),
        DatabaseConfig::Sqlite(_) => Arc::new(DatabaseNoPasswordProvider {}),
        DatabaseConfig::MySql(config)
        | DatabaseConfig::Postgres(config)
        | DatabaseConfig::MariaDB(config) => {
            make_remote_database_password_provider(db_credentials, &config.password_policy)
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

fn make_remote_database_password_provider(
    db_configuration: &DatabaseCredentials,
    password_policy_config: &DatabasePasswordPolicyConfig,
) -> Arc<dyn DatabasePasswordProvider> {
    match &password_policy_config.source {
        DatabasePasswordSourceConfig::RawPassword(raw_password_config) => {
            Arc::new(DatabaseFixedPasswordProvider::new(Secret::new(
                raw_password_config.raw_password.clone(),
            )))
        }
        DatabasePasswordSourceConfig::AwsSecret(aws_secret_config) => Arc::new(
            DatabaseAwsSecretPasswordProvider::new(aws_secret_config.secret_name.clone()),
        ),
        DatabasePasswordSourceConfig::AwsIamToken => {
            Arc::new(DatabaseAwsIamTokenProvider::new(db_configuration.clone()))
        }
    }
}

///////////////////////////////////////////////////////////////////////////////
