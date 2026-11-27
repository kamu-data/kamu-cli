// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use kamu::domain::TenancyConfig;
use kamu_cli::config::{CLIConfig, ResourceRetentionConfig, ResourcesConfig};
use kamu_cli::{
    CLIError,
    WorkspaceLayout,
    WorkspaceStatus,
    check_resource_retention_config,
    configure_base_catalog,
    configure_in_memory_components,
    register_config_in_catalog,
};
use tempfile::TempDir;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_cli_refuses_retention_of_unknown_resource_types() {
    for (types, expected_error) in [
        (&["VariableSet", "SecretSet"][..], None),
        (
            &["VariableSet", "Nope"][..],
            Some("Retention configured for unknown resource types: Nope;"),
        ),
    ] {
        let harness = RetentionTypesHarness::new(types);

        match (harness.check_retention_types(), expected_error) {
            (Ok(()), None) => {}
            (Err(CLIError::UsageError(err)), Some(expected)) => {
                assert!(err.to_string().starts_with(expected), "{types:?}: {err}");
            }
            (result, expected) => panic!("{types:?}: {result:?}, expected {expected:?}"),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The base catalog of a CLI configured with a retention of one day for each
/// of the given types
struct RetentionTypesHarness {
    base_catalog: dill::Catalog,
    _temp_dir: TempDir,
}

impl RetentionTypesHarness {
    fn new(retained_types: &[&str]) -> Self {
        let temp_dir = tempfile::tempdir().unwrap();
        let workspace_layout = WorkspaceLayout::new(temp_dir.path());
        let tenancy_config = TenancyConfig::SingleTenant;
        let cli_config = CLIConfig {
            resources: ResourcesConfig {
                retention: ResourceRetentionConfig {
                    types: retained_types
                        .iter()
                        .map(|type_name| (type_name.to_string(), "1d".parse().unwrap()))
                        .collect::<BTreeMap<_, _>>(),
                    ..ResourceRetentionConfig::default()
                },
            },
            ..CLIConfig::default()
        };

        let mut b = configure_base_catalog(
            &workspace_layout,
            WorkspaceStatus::Created(tenancy_config),
            &cli_config,
            tenancy_config,
            None,
            false,
        );
        configure_in_memory_components(&mut b);
        register_config_in_catalog(
            &cli_config,
            &mut b,
            WorkspaceStatus::Created(tenancy_config),
            None,
            false,
        )
        .unwrap();

        Self {
            base_catalog: b.build(),
            _temp_dir: temp_dir,
        }
    }

    fn check_retention_types(&self) -> Result<(), CLIError> {
        check_resource_retention_config(&self.base_catalog)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
