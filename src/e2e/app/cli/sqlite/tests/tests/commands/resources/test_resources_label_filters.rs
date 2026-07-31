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
// Label filtering (`-l key=value`) on list, get and delete
// (emits _local + _remote per scenario)
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_resource_e2e_test!(
    storage = sqlite,
    fixture = kamu_cli_e2e_repo_tests::commands::test_resources_label_filter_list
);

kamu_cli_resource_e2e_test!(
    storage = sqlite,
    fixture = kamu_cli_e2e_repo_tests::commands::test_resources_label_filter_list_all
);

kamu_cli_resource_e2e_test!(
    storage = sqlite,
    fixture = kamu_cli_e2e_repo_tests::commands::test_resources_label_filter_get
);

kamu_cli_resource_e2e_test!(
    storage = sqlite,
    fixture = kamu_cli_e2e_repo_tests::commands::test_resources_label_filter_delete
);

kamu_cli_resource_e2e_test!(
    storage = sqlite,
    fixture =
        kamu_cli_e2e_repo_tests::commands::test_resources_label_filter_type_pattern_exact_name_ignores_filter
);

// Apply `SecretSet`s, hence `with_kamu_config`.
kamu_cli_resource_e2e_test!(
    storage = sqlite,
    fixture =
        kamu_cli_e2e_repo_tests::commands::test_resources_label_filter_type_pattern_name_pattern,
    options = Options::default().with_kamu_config(
        kamu_cli_e2e_repo_tests::resources::fixtures::SECRETS_ENCRYPTION_KAMU_CONFIG
    )
);

kamu_cli_resource_e2e_test!(
    storage = sqlite,
    fixture =
        kamu_cli_e2e_repo_tests::commands::test_resources_label_filter_structured_not_selectable
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
