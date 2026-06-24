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
// SecretSet lifecycle + reveal safety (emits _local + _remote)
//
// Applying a SecretSet requires secrets encryption to be enabled, hence the
// `with_kamu_config`.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_resource_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::commands::test_resources_secretset_lifecycle,
    options = Options::default().with_kamu_config(
        kamu_cli_e2e_repo_tests::resources::fixtures::SECRETS_ENCRYPTION_KAMU_CONFIG
    )
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Apply a manifest with an already-encrypted secret (emits _local + _remote)
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_resource_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::commands::test_resources_secretset_apply_pre_encrypted,
    options = Options::default().with_kamu_config(
        kamu_cli_e2e_repo_tests::resources::fixtures::SECRETS_ENCRYPTION_KAMU_CONFIG
    )
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
