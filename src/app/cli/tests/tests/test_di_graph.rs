// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::*;
use kamu::domain::auth::JwtAuthenticationConfig;
use kamu_accounts::CurrentAccountSubject;
use kamu_cli::{self, OutputConfig, WorkspaceLayout};

#[test_log::test(tokio::test)]
async fn test_di_graph_validates() {
    let tempdir = tempfile::tempdir().unwrap();
    let workspace_layout = WorkspaceLayout::new(tempdir.path());
    let mut base_catalog_builder = kamu_cli::configure_base_catalog(&workspace_layout, false, None);
    kamu_cli::configure_in_memory_components(&mut base_catalog_builder);
    base_catalog_builder.add_value(OutputConfig::default());

    kamu_cli::register_config_in_catalog(
        &kamu_cli::config::CLIConfig::default(),
        &mut base_catalog_builder,
        false,
    );
    let base_catalog = base_catalog_builder.build();

    let mut cli_catalog_builder = kamu_cli::configure_cli_catalog(&base_catalog);
    cli_catalog_builder.add_value(CurrentAccountSubject::new_test());
    cli_catalog_builder.add_value(JwtAuthenticationConfig::default());

    // TODO: We should ensure this test covers parameters requested by commands and
    // types needed for GQL/HTTP adapter that are currently being constructed
    // manually
    let validate_result = cli_catalog_builder.validate().ignore::<WorkspaceLayout>();

    assert!(
        validate_result.is_ok(),
        "{}",
        validate_result.err().unwrap()
    );
}
