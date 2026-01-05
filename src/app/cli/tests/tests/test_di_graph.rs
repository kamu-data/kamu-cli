// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;

use database_common::{DatabaseConnectionSettings, SqlitePoolOptions, TransactionRef};
use dill::CatalogBuilder;
use kamu::domain::{KamuBackgroundCatalog, ServerUrlConfig, TenancyConfig};
use kamu_accounts::{CurrentAccountSubject, JwtAuthenticationConfig};
use kamu_adapter_http::AccessToken;
use kamu_adapter_oauth::GithubAuthenticationConfig;
use kamu_cli::services::config::DatabaseConfig;
use kamu_cli::{self, Interact, OutputConfig, WorkspaceLayout, WorkspaceStatus};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

macro_rules! test_di_permutations {
    ($test_name: expr) => {
        paste::paste! {
            #[test_log::test(tokio::test)]
            pub async fn [<$test_name "_st_inmem">]() {
                $test_name(TenancyConfig::SingleTenant, RepositoriesConfig::InMemory);
            }

            #[test_log::test(tokio::test)]
            pub async fn [<$test_name "_st_sqlite">]() {
                $test_name(TenancyConfig::SingleTenant, RepositoriesConfig::Sqlite);
            }

            #[test_log::test(tokio::test)]
            pub async fn [<$test_name "_mt_inmem">]() {
                $test_name(TenancyConfig::MultiTenant, RepositoriesConfig::InMemory);
            }

            #[test_log::test(tokio::test)]
            pub async fn [<$test_name "_mt_sqlite">]() {
                $test_name(TenancyConfig::MultiTenant, RepositoriesConfig::Sqlite);
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

test_di_permutations!(test_di_cli_graph_validates);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

test_di_permutations!(test_di_server_graph_validates);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Implementations
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone)]
pub enum RepositoriesConfig {
    InMemory,
    Sqlite,
}

fn test_di_cli_graph_validates(
    tenancy_config: TenancyConfig,
    repositories_config: RepositoriesConfig,
) {
    let temp_dir = tempfile::tempdir().unwrap();
    let workspace_layout = WorkspaceLayout::new(temp_dir.path());
    let mut base_catalog_builder = kamu_cli::configure_base_catalog(
        &workspace_layout,
        WorkspaceStatus::Created(tenancy_config),
        tenancy_config,
        None,
        false,
    );

    register_repositories(&mut base_catalog_builder, repositories_config);

    base_catalog_builder.add_value(OutputConfig::default());

    kamu_cli::register_config_in_catalog(
        &kamu_cli::config::CLIConfig::default(),
        &mut base_catalog_builder,
        WorkspaceStatus::Created(tenancy_config),
        None,
        false,
    )
    .unwrap();
    base_catalog_builder.add_value(Interact::new(false, false));
    let base_catalog = base_catalog_builder.build();

    let mut cli_catalog_builder = kamu_cli::configure_cli_catalog(&base_catalog, tenancy_config);

    cli_catalog_builder.add_value(CurrentAccountSubject::new_test());
    cli_catalog_builder.add_value(JwtAuthenticationConfig::default());
    cli_catalog_builder.add_value(GithubAuthenticationConfig::default());
    cli_catalog_builder.add_value(kamu_adapter_flight_sql::SessionId(String::new()));

    let validate_result = cli_catalog_builder.validate();

    assert!(validate_result.is_ok(), "{}", validate_result.unwrap_err());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn test_di_server_graph_validates(
    tenancy_config: TenancyConfig,
    repositories_config: RepositoriesConfig,
) {
    let temp_dir = tempfile::tempdir().unwrap();
    let workspace_layout = WorkspaceLayout::new(temp_dir.path());
    let mut base_catalog_builder = kamu_cli::configure_base_catalog(
        &workspace_layout,
        WorkspaceStatus::Created(tenancy_config),
        tenancy_config,
        None,
        false,
    );

    register_repositories(&mut base_catalog_builder, repositories_config);

    base_catalog_builder.add_value(OutputConfig::default());

    let config = kamu_cli::config::CLIConfig::default();

    kamu_cli::register_config_in_catalog(
        &config,
        &mut base_catalog_builder,
        WorkspaceStatus::Created(tenancy_config),
        None,
        false,
    )
    .unwrap();
    let base_catalog = base_catalog_builder.build();

    let system_user_subject = CurrentAccountSubject::new_test();

    let wrapped_catalog = dill::CatalogBuilder::new_chained(&base_catalog)
        .add_value(KamuBackgroundCatalog::new(
            base_catalog,
            system_user_subject.clone(),
        ))
        .build();

    let mut server_catalog_builder =
        kamu_cli::configure_server_catalog(&wrapped_catalog, tenancy_config);
    server_catalog_builder.add_value(JwtAuthenticationConfig::default());
    server_catalog_builder.add_value(GithubAuthenticationConfig::default());
    server_catalog_builder.add_value(ServerUrlConfig::new_test(None));
    server_catalog_builder.add_value(AccessToken::new("some-test-token"));
    server_catalog_builder.add_value(kamu_adapter_flight_sql::SessionId(String::new()));
    let server_catalog = server_catalog_builder.build();

    let mut cli_catalog_builder = dill::CatalogBuilder::new_chained(&server_catalog);
    cli_catalog_builder.add_value(system_user_subject);

    // TODO: We should ensure this test covers parameters requested by commands and
    // types needed for GQL/HTTP adapter that are currently being constructed
    // manually
    let validate_result = cli_catalog_builder.validate();

    assert!(validate_result.is_ok(), "{}", validate_result.unwrap_err());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn register_repositories(b: &mut CatalogBuilder, repositories_config: RepositoriesConfig) {
    match repositories_config {
        RepositoriesConfig::InMemory => {
            kamu_cli::configure_in_memory_components(b);
        }
        RepositoriesConfig::Sqlite => {
            let fake_path = Path::new("will-not-be-created.db.sqlite");
            let database_config = DatabaseConfig::sqlite(fake_path);
            let db_connection_settings = DatabaseConnectionSettings::sqlite_from(fake_path);

            kamu_cli::configure_database_components(b, &database_config, db_connection_settings);

            let pool = SqlitePoolOptions::default()
                .connect_lazy("sqlite::memory:")
                .unwrap();

            b.add_value(pool.clone());

            let transaction_ref = TransactionRef::new(pool);
            transaction_ref.register(b);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
