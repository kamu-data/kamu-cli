// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_e2e_common::DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR;
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

pub async fn test_list_result_limits(kamu: KamuCliPuppet) {
    // Clap rejects conflicting flags before any datasets are needed
    kamu.assert_failure_command_execution(
        ["list", "--max-results", "10", "--unbounded"],
        None,
        Some(["error: the argument '--max-results <N>' cannot be used with '--unbounded'"]),
    )
    .await;

    // Add two datasets
    kamu.execute_with_input(
        ["add", "--stdin", "--name", "player-scores-1"],
        DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR,
    )
    .await
    .success();

    kamu.execute_with_input(
        ["add", "--stdin", "--name", "player-scores-2"],
        DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR,
    )
    .await
    .success();

    // --max-results 1 should return exactly one dataset
    {
        let assert = kamu
            .execute(["list", "--max-results", "1", "--output-format", "json"])
            .await
            .success();
        let stdout = std::str::from_utf8(&assert.get_output().stdout).unwrap();
        let results: Vec<serde_json::Value> = serde_json::from_str(stdout).unwrap();
        pretty_assertions::assert_eq!(1, results.len());
    }

    // `list datasets --max-results 1` should behave the same
    {
        let assert = kamu
            .execute([
                "list",
                "datasets",
                "--max-results",
                "1",
                "--output-format",
                "json",
            ])
            .await
            .success();
        let stdout = std::str::from_utf8(&assert.get_output().stdout).unwrap();
        let results: Vec<serde_json::Value> = serde_json::from_str(stdout).unwrap();
        pretty_assertions::assert_eq!(1, results.len());
    }

    // --unbounded should return all datasets
    {
        let assert = kamu
            .execute(["list", "--unbounded", "--output-format", "json"])
            .await
            .success();
        let stdout = std::str::from_utf8(&assert.get_output().stdout).unwrap();
        let results: Vec<serde_json::Value> = serde_json::from_str(stdout).unwrap();
        pretty_assertions::assert_eq!(2, results.len());
    }

    // `list` should behave the same as --unbounded
    {
        let assert = kamu
            .execute(["list", "--output-format", "json"])
            .await
            .success();
        let stdout = std::str::from_utf8(&assert.get_output().stdout).unwrap();
        let results: Vec<serde_json::Value> = serde_json::from_str(stdout).unwrap();
        pretty_assertions::assert_eq!(2, results.len());
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
