// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::domain::ServerUrlConfig;
use kamu_accounts::{CurrentAccountSubject, JwtAuthenticationConfig};
use kamu_adapter_http::AccessToken;
use kamu_cli::{self, OutputConfig, WorkspaceLayout};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_di_cli_graph_validates_st() {
    test_di_cli_graph_validates(false).await
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_di_cli_graph_validates_mt() {
    test_di_cli_graph_validates(true).await
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_di_server_graph_validates_st() {
    test_di_server_graph_validates(false).await
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_di_server_graph_validates_mt() {
    test_di_server_graph_validates(true).await
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_di_cli_graph_validates(multi_tenant_workspace: bool) {
    let temp_dir = tempfile::tempdir().unwrap();
    let workspace_layout = WorkspaceLayout::new(temp_dir.path());
    let mut base_catalog_builder =
        kamu_cli::configure_base_catalog(&workspace_layout, false, None, false);
    kamu_cli::configure_in_memory_components(&mut base_catalog_builder);
    base_catalog_builder.add_value(OutputConfig::default());

    kamu_cli::register_config_in_catalog(
        &kamu_cli::config::CLIConfig::default(),
        &mut base_catalog_builder,
        multi_tenant_workspace,
    );
    let base_catalog = base_catalog_builder.build();

    let mut cli_catalog_builder =
        kamu_cli::configure_cli_catalog(&base_catalog, multi_tenant_workspace);

    cli_catalog_builder.add_value(CurrentAccountSubject::new_test());
    cli_catalog_builder.add_value(JwtAuthenticationConfig::default());

    let validate_result = cli_catalog_builder.validate();

    assert!(
        validate_result.is_ok(),
        "{}",
        validate_result.err().unwrap()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_di_server_graph_validates(multi_tenant_workspace: bool) {
    let temp_dir = tempfile::tempdir().unwrap();
    let workspace_layout = WorkspaceLayout::new(temp_dir.path());
    let mut base_catalog_builder =
        kamu_cli::configure_base_catalog(&workspace_layout, false, None, false);
    kamu_cli::configure_in_memory_components(&mut base_catalog_builder);
    base_catalog_builder.add_value(OutputConfig::default());

    kamu_cli::register_config_in_catalog(
        &kamu_cli::config::CLIConfig::default(),
        &mut base_catalog_builder,
        multi_tenant_workspace,
    );
    let base_catalog = base_catalog_builder.build();

    let mut cli_catalog_builder =
        kamu_cli::configure_server_catalog(&base_catalog, multi_tenant_workspace);

    cli_catalog_builder.add_value(CurrentAccountSubject::new_test());
    cli_catalog_builder.add_value(JwtAuthenticationConfig::default());
    cli_catalog_builder.add_value(ServerUrlConfig::new_test(None));
    cli_catalog_builder.add_value(AccessToken::new("some-test-token"));

    // TODO: We should ensure this test covers parameters requested by commands and
    // types needed for GQL/HTTP adapter that are currently being constructed
    // manually
    let validate_result = cli_catalog_builder.validate();

    assert!(
        validate_result.is_ok(),
        "{}",
        validate_result.err().unwrap()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
