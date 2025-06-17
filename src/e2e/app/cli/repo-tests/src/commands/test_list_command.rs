// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_puppet::KamuCliPuppet;
use kamu_cli_puppet::extensions::KamuCliPuppetExt;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_account_argument_is_not_valid_account_name_mt(kamu: KamuCliPuppet) {
    let not_valid_account_name = "not_valid_account_name";

    pretty_assertions::assert_matches!(odf::AccountName::try_from(not_valid_account_name), Err(_));

    kamu.assert_failure_command_execution(
        ["--account", not_valid_account_name, "list"],
        None,
        Some([
            format!("Error: Value '{not_valid_account_name}' is not a valid AccountName").as_str(),
        ]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_account_argument_is_not_registered_account_mt(kamu: KamuCliPuppet) {
    let valid_but_unknown_account_name = "unknown-account";

    pretty_assertions::assert_matches!(
        odf::AccountName::try_from(valid_but_unknown_account_name),
        Ok(_)
    );

    kamu.assert_failure_command_execution(
        ["--account", valid_but_unknown_account_name, "list"],
        None,
        Some([format!("Account '{valid_but_unknown_account_name}' not registered").as_str()]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
