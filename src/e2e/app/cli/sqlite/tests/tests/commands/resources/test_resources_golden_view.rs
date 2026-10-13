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
// Golden full-document `get -o json` shape for VariableSet + SecretSet — the
// single drift guard for the render struct (emits _local + _remote).
//
// Includes a SecretSet, so secrets encryption must be enabled via
// `with_kamu_config`.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_resource_e2e_test!(
    storage = sqlite,
    fixture = kamu_cli_e2e_repo_tests::commands::test_resources_golden_view,
    options = Options::default().with_kamu_config(
        kamu_cli_e2e_repo_tests::resources::fixtures::SECRETS_ENCRYPTION_KAMU_CONFIG
    )
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
