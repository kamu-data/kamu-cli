// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;

use background_agent_host::{BackgroundAgent, BackgroundAgentHost};
use database_common::{DatabaseConnectionSettings, SqlitePoolOptions, TransactionRef};
use dill::CatalogBuilder;
use kamu::domain::{KamuBackgroundCatalog, ServerUrlConfig, TenancyConfig};
use kamu_accounts::{CurrentAccountSubject, JwtAuthenticationConfig, Principal};
use kamu_adapter_flight_sql::SessionId;
use kamu_adapter_http::AccessToken;
use kamu_adapter_oauth::GithubAuthenticationConfig;
use kamu_cli::config::CLIConfig;
use kamu_cli::services::config::DatabaseConfig;
use kamu_cli::{self, Interact, OutputConfig, WorkspaceLayout, WorkspaceStatus};
use pretty_assertions::assert_eq;
use tempfile::TempDir;

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn test_di_cli_graph_validates(
    tenancy_config: TenancyConfig,
    repositories_config: RepositoriesConfig,
) {
    let harness = DiGraphHarness::new(tenancy_config, repositories_config);

    let validate_result = harness.cli_catalog_builder().validate();

    assert!(validate_result.is_ok(), "{}", validate_result.unwrap_err());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn test_di_background_catalog_acts_as_node(
    tenancy_config: TenancyConfig,
    repositories_config: RepositoriesConfig,
) {
    let harness = DiGraphHarness::new(tenancy_config, repositories_config);
    let subject = CurrentAccountSubject::new_test();

    let mut background_catalog_builder = harness.background_catalog_builder();
    let validate_result = background_catalog_builder.validate();
    assert!(validate_result.is_ok(), "{}", validate_result.unwrap_err());

    let background_catalog = background_catalog_builder.build();
    let cli_catalog = harness.cli_catalog_builder().build();

    assert_eq!(
        *cli_catalog.get_one::<Principal>().unwrap(),
        Principal::from(&subject)
    );
    assert_eq!(
        *background_catalog.get_one::<Principal>().unwrap(),
        Principal::Node
    );
    // Background work has no caller
    assert!(
        background_catalog
            .get_one::<CurrentAccountSubject>()
            .is_err()
    );
    // Remote pulls in background still find the process account's saved tokens
    background_catalog
        .get_one::<dyn odf::dataset::DatasetFactory>()
        .unwrap();

    let node_catalog = harness.node_catalog();
    assert_eq!(
        *node_catalog.get_one::<Principal>().unwrap(),
        Principal::Node
    );
    assert!(node_catalog.get_one::<CurrentAccountSubject>().is_err());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn test_di_server_graph_validates(
    tenancy_config: TenancyConfig,
    repositories_config: RepositoriesConfig,
) {
    let harness = DiGraphHarness::new(tenancy_config, repositories_config);

    // TODO: We should ensure this test covers parameters requested by commands and
    // types needed for GQL/HTTP adapter that are currently being constructed
    // manually
    let validate_result = harness.server_request_catalog_builder().validate();

    assert!(validate_result.is_ok(), "{}", validate_result.unwrap_err());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The background catalog of `system api-server run`: built over the server
/// catalog, it is where the server's agents are resolved
fn test_di_server_background_catalog_acts_as_node(
    tenancy_config: TenancyConfig,
    repositories_config: RepositoriesConfig,
) {
    let harness = DiGraphHarness::new(tenancy_config, repositories_config);

    let mut background_catalog_builder = harness.server_background_catalog_builder();
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

    // What `api-server` runs them with
    background_catalog.get_one::<BackgroundAgentHost>().unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(resourcegen)]
#[test_log::test(tokio::test)]
async fn update_di_graph() {
    let harness = DiGraphHarness::new(TenancyConfig::MultiTenant, RepositoriesConfig::Sqlite);

    let puml = dill::utils::plantuml::render(&harness.cli_catalog_builder().build());

    let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("../../../resources/di.puml");

    std::fs::write(&path, puml.as_bytes()).unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct DiGraphHarness {
    tenancy_config: TenancyConfig,
    process_catalog: dill::Catalog,
    _temp_dir: TempDir,
}

impl DiGraphHarness {
    fn new(tenancy_config: TenancyConfig, repositories_config: RepositoriesConfig) -> Self {
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

        Self::register_repositories(&mut base_catalog_builder, repositories_config);

        base_catalog_builder.add_value(OutputConfig::default());

        kamu_cli::register_config_in_catalog(
            &CLIConfig::default(),
            &mut base_catalog_builder,
            WorkspaceStatus::Created(tenancy_config),
            None,
            false,
        )
        .unwrap();
        base_catalog_builder.add_value(Interact::new(false, false));
        let base_catalog = base_catalog_builder.build();

        let process_catalog =
            kamu_cli::configure_process_catalog(&base_catalog, &CurrentAccountSubject::new_test())
                .build();

        Self {
            tenancy_config,
            process_catalog,
            _temp_dir: temp_dir,
        }
    }

    fn register_repositories(b: &mut CatalogBuilder, repositories_config: RepositoriesConfig) {
        match repositories_config {
            RepositoriesConfig::InMemory => {
                kamu_cli::configure_in_memory_components(b);
            }
            RepositoriesConfig::Sqlite => {
                let fake_path = Path::new("will-not-be-created.db.sqlite");
                let database_config = DatabaseConfig::sqlite(fake_path);
                let db_connection_settings = DatabaseConnectionSettings::sqlite_from(fake_path);

                kamu_cli::configure_database_components(
                    b,
                    &database_config,
                    db_connection_settings,
                );

                let pool = SqlitePoolOptions::default()
                    .connect_lazy("sqlite::memory:")
                    .unwrap();

                b.add_value(pool.clone());

                let transaction_ref = TransactionRef::new(pool);
                transaction_ref.register(b);
            }
        }
    }

    /// The catalog a CLI command runs in
    fn cli_catalog_builder(&self) -> CatalogBuilder {
        let mut b = kamu_cli::configure_cli_catalog(&self.process_catalog, self.tenancy_config);

        CurrentAccountSubject::new_test().register(&mut b);
        b.add_value(JwtAuthenticationConfig::default());
        b.add_value(GithubAuthenticationConfig::default());
        b.add_value(SessionId(String::new()));

        b
    }

    /// The catalog CLI background work runs in
    fn background_catalog_builder(&self) -> CatalogBuilder {
        kamu_cli::configure_background_catalog(&self.process_catalog)
    }

    fn node_catalog(&self) -> dill::Catalog {
        KamuBackgroundCatalog::new(self.process_catalog.clone()).node_catalog()
    }

    /// The server catalog `system api-server run` chains its catalogs from
    fn server_catalog(&self) -> dill::Catalog {
        let wrapped_catalog = CatalogBuilder::new_chained(&self.process_catalog)
            .add_value(KamuBackgroundCatalog::new(self.process_catalog.clone()))
            .build();

        let mut b = kamu_cli::configure_server_catalog(&wrapped_catalog, self.tenancy_config);
        b.add_value(JwtAuthenticationConfig::default());
        b.add_value(GithubAuthenticationConfig::default());
        b.add_value(ServerUrlConfig::new_test(None));
        b.add_value(AccessToken::new("some-test-token"));
        b.add_value(SessionId(String::new()));
        b.build()
    }

    /// The catalog a server request runs in, on behalf of a test account
    fn server_request_catalog_builder(&self) -> CatalogBuilder {
        let mut b = CatalogBuilder::new_chained(&self.server_catalog());
        CurrentAccountSubject::new_test().register(&mut b);
        b
    }

    /// The background catalog of the server
    fn server_background_catalog_builder(&self) -> CatalogBuilder {
        kamu_cli::configure_background_catalog(&self.server_catalog())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
