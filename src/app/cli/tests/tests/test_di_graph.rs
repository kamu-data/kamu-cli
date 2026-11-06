// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;

use async_utils::BackgroundAgent;
use database_common::{DatabaseConnectionSettings, SqlitePoolOptions, TransactionRef};
use dill::CatalogBuilder;
use kamu::domain::{KamuBackgroundCatalog, ServerUrlConfig, TenancyConfig};
use kamu_accounts::{
    CurrentAccountSubject,
    CurrentAccountSubjectCatalogExt as _,
    JwtAuthenticationConfig,
    Principal,
};
use kamu_adapter_http::AccessToken;
use kamu_adapter_oauth::GithubAuthenticationConfig;
use kamu_cli::config::CLIConfig;
use kamu_cli::services::config::DatabaseConfig;
use kamu_cli::{self, Interact, OutputConfig, WorkspaceLayout, WorkspaceStatus};
use pretty_assertions::assert_eq;

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

test_di_permutations!(test_di_background_catalog_acts_as_node);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

test_di_permutations!(test_di_server_background_catalog_acts_as_node);

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
    let base_catalog =
        build_test_base_catalog(temp_dir.path(), tenancy_config, repositories_config);
    let mut cli_catalog_builder = configure_test_cli_catalog(&base_catalog, tenancy_config);

    let validate_result = cli_catalog_builder.validate();

    assert!(validate_result.is_ok(), "{}", validate_result.unwrap_err());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn test_di_background_catalog_acts_as_node(
    tenancy_config: TenancyConfig,
    repositories_config: RepositoriesConfig,
) {
    let temp_dir = tempfile::tempdir().unwrap();
    let base_catalog =
        build_test_base_catalog(temp_dir.path(), tenancy_config, repositories_config);
    let subject = CurrentAccountSubject::new_test();

    let mut background_catalog_builder =
        kamu_cli::configure_background_catalog(&base_catalog, subject.clone(), tenancy_config);
    background_catalog_builder.add_value(JwtAuthenticationConfig::default());
    background_catalog_builder.add_value(GithubAuthenticationConfig::default());
    background_catalog_builder.add_value(kamu_adapter_flight_sql::SessionId(String::new()));
    let validate_result = background_catalog_builder.validate();
    assert!(validate_result.is_ok(), "{}", validate_result.unwrap_err());

    let background_catalog = background_catalog_builder.build();
    let cli_catalog = configure_test_cli_catalog(&base_catalog, tenancy_config).build();

    assert_eq!(
        *cli_catalog.get_one::<Principal>().unwrap(),
        Principal::from(&subject)
    );
    assert_eq!(
        *background_catalog.get_one::<Principal>().unwrap(),
        Principal::Node
    );
    // Name scope reads the caller's subject, not the node
    assert_eq!(
        background_catalog
            .get_one::<CurrentAccountSubject>()
            .unwrap()
            .account_handle(),
        subject.account_handle()
    );

    let system_user_catalog =
        KamuBackgroundCatalog::new(base_catalog, subject).system_user_catalog();
    assert_eq!(
        *system_user_catalog.get_one::<Principal>().unwrap(),
        Principal::Node
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn build_test_base_catalog(
    workspace_path: &Path,
    tenancy_config: TenancyConfig,
    repositories_config: RepositoriesConfig,
) -> dill::Catalog {
    let workspace_layout = WorkspaceLayout::new(workspace_path);
    let mut base_catalog_builder = kamu_cli::configure_base_catalog(
        &workspace_layout,
        WorkspaceStatus::Created(tenancy_config),
        &CLIConfig::default(),
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
    base_catalog_builder.build()
}

/// The server catalog `system api-server run` chains its catalogs from
fn build_test_server_catalog(
    workspace_path: &Path,
    tenancy_config: TenancyConfig,
    repositories_config: RepositoriesConfig,
) -> dill::Catalog {
    let workspace_layout = WorkspaceLayout::new(workspace_path);
    let mut base_catalog_builder = kamu_cli::configure_base_catalog(
        &workspace_layout,
        WorkspaceStatus::Created(tenancy_config),
        &CLIConfig::default(),
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
    base_catalog_builder.add_value(Interact::new(false, false));
    let base_catalog = base_catalog_builder.build();

    let wrapped_catalog = dill::CatalogBuilder::new_chained(&base_catalog)
        .add_value(KamuBackgroundCatalog::new(
            base_catalog,
            CurrentAccountSubject::new_test(),
        ))
        .build();

    let mut server_catalog_builder =
        kamu_cli::configure_server_catalog(&wrapped_catalog, tenancy_config);
    server_catalog_builder.add_value(JwtAuthenticationConfig::default());
    server_catalog_builder.add_value(GithubAuthenticationConfig::default());
    server_catalog_builder.add_value(ServerUrlConfig::new_test(None));
    server_catalog_builder.add_value(AccessToken::new("some-test-token"));
    server_catalog_builder.add_value(kamu_adapter_flight_sql::SessionId(String::new()));
    server_catalog_builder.build()
}

fn configure_test_cli_catalog(
    base_catalog: &dill::Catalog,
    tenancy_config: TenancyConfig,
) -> CatalogBuilder {
    let mut cli_catalog_builder = kamu_cli::configure_cli_catalog(base_catalog, tenancy_config);

    cli_catalog_builder.add_current_account_subject(CurrentAccountSubject::new_test());
    cli_catalog_builder.add_value(JwtAuthenticationConfig::default());
    cli_catalog_builder.add_value(GithubAuthenticationConfig::default());
    cli_catalog_builder.add_value(kamu_adapter_flight_sql::SessionId(String::new()));

    cli_catalog_builder
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn test_di_server_graph_validates(
    tenancy_config: TenancyConfig,
    repositories_config: RepositoriesConfig,
) {
    let temp_dir = tempfile::tempdir().unwrap();
    let server_catalog =
        build_test_server_catalog(temp_dir.path(), tenancy_config, repositories_config);
    let system_user_subject = CurrentAccountSubject::new_test();

    let mut cli_catalog_builder = dill::CatalogBuilder::new_chained(&server_catalog);
    cli_catalog_builder.add_current_account_subject(system_user_subject);

    // TODO: We should ensure this test covers parameters requested by commands and
    // types needed for GQL/HTTP adapter that are currently being constructed
    // manually
    let validate_result = cli_catalog_builder.validate();

    assert!(validate_result.is_ok(), "{}", validate_result.unwrap_err());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The background catalog of `system api-server run`: built over the server
/// catalog, it is where the server's agents are resolved
fn test_di_server_background_catalog_acts_as_node(
    tenancy_config: TenancyConfig,
    repositories_config: RepositoriesConfig,
) {
    let temp_dir = tempfile::tempdir().unwrap();
    let server_catalog =
        build_test_server_catalog(temp_dir.path(), tenancy_config, repositories_config);

    let mut background_catalog_builder = kamu_cli::configure_background_catalog(
        &server_catalog,
        CurrentAccountSubject::new_test(),
        tenancy_config,
    );
    let validate_result = background_catalog_builder.validate();
    assert!(validate_result.is_ok(), "{}", validate_result.unwrap_err());

    let background_catalog = background_catalog_builder.build();
    assert_eq!(
        *background_catalog.get_one::<Principal>().unwrap(),
        Principal::Node
    );

    let background_agents = background_catalog
        .get::<dill::AllOf<dyn BackgroundAgent>>()
        .unwrap();
    assert!(!background_agents.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(resourcegen)]
#[test_log::test(tokio::test)]
async fn update_di_graph() {
    let tenancy_config = TenancyConfig::MultiTenant;
    let repositories_config = RepositoriesConfig::Sqlite;

    let temp_dir = tempfile::tempdir().unwrap();
    let workspace_layout = WorkspaceLayout::new(temp_dir.path());
    let mut base_catalog_builder = kamu_cli::configure_base_catalog(
        &workspace_layout,
        WorkspaceStatus::Created(tenancy_config),
        &CLIConfig::default(),
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

    cli_catalog_builder.add_current_account_subject(CurrentAccountSubject::new_test());
    cli_catalog_builder.add_value(JwtAuthenticationConfig::default());
    cli_catalog_builder.add_value(GithubAuthenticationConfig::default());
    cli_catalog_builder.add_value(kamu_adapter_flight_sql::SessionId(String::new()));

    let puml = dill::utils::plantuml::render(&cli_catalog_builder.build());

    let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("../../../resources/di.puml");

    std::fs::write(&path, puml.as_bytes()).unwrap();
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
