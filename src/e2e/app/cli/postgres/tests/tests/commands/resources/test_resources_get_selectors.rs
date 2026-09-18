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
// Selector grammar for `get`: equivalent forms, patterns, multi-result JSON
// wrapping, --max-results, --unbounded (emits _local + _remote)
//
// Includes SecretSet resources, so secrets encryption must be enabled via
// `with_kamu_config`.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_resource_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::commands::test_resources_get_selectors,
    options = Options::default().with_kamu_config(
        kamu_cli_e2e_repo_tests::resources::fixtures::SECRETS_ENCRYPTION_KAMU_CONFIG
    )
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
