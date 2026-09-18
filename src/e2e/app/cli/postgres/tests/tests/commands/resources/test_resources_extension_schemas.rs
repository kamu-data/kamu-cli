// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_e2e_common::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Extension-schema behavior: canonicalization, rejections, warnings
// (emits _local + _remote per scenario)
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_resource_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::commands::test_resources_extension_schema_behavior
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// The temporary `legacy-config-target-dataset` label
// (emits _local + _remote)
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Applies a `SecretSet`, hence `with_kamu_config`.
kamu_cli_resource_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::commands::test_resources_legacy_config_target_dataset_label,
    options = Options::default().with_kamu_config(
        kamu_cli_e2e_repo_tests::resources::fixtures::SECRETS_ENCRYPTION_KAMU_CONFIG
    )
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
