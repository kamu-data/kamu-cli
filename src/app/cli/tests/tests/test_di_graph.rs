// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::*;
use kamu::domain::CurrentAccountConfig;
use kamu::WorkspaceLayout;
use kamu_cli;

#[test_log::test(tokio::test)]
async fn test_di_graph_validates() {
    let tempdir = tempfile::tempdir().unwrap();
    let workspace_layout = WorkspaceLayout::new(tempdir.path());
    let mut catalog_builder = kamu_cli::configure_catalog(&workspace_layout, false);
    kamu_cli::register_config_in_catalog(&kamu_cli::CLIConfig::default(), &mut catalog_builder);
    catalog_builder.add_value(CurrentAccountConfig::new("kamu", false));

    // TODO: We should ensure this test covers parameters requested by commands and
    // types needed for GQL/HTTP adapter that are currently being constructed
    // manually
    let validate_result = catalog_builder.validate().ignore::<WorkspaceLayout>();

    assert!(
        validate_result.is_ok(),
        "{}",
        validate_result.err().unwrap()
    );
}
