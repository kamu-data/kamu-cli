// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_e2e_common::{
    DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR,
};
use kamu_cli_puppet::KamuCliPuppet;
use kamu_cli_puppet::extensions::KamuCliPuppetExt;

use crate::utils::create_alice_and_bob;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_read_commands_deny_other_accounts_private_dataset(mut kamu: KamuCliPuppet) {
    let (alice, bob) = create_alice_and_bob(&kamu).await;

    kamu.set_account(Some(bob));
    for (dataset_name, visibility) in [("bob-private", "private"), ("bob-public", "public")] {
        kamu.execute_with_input(
            [
                "add",
                "--stdin",
                "--name",
                dataset_name,
                "--visibility",
                visibility,
            ],
            DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR,
        )
        .await
        .success();
        kamu.ingest_data(
            &odf::DatasetName::new_unchecked(dataset_name),
            DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
        )
        .await;
    }

    kamu.set_account(Some(alice));

    for (public_cmd, private_cmd, public_stdout_part, private_stderr) in [
        (
            vec!["log", "bob/bob-public"],
            vec!["log", "bob/bob-private"],
            "AddData",
            "User has no 'read' permission in dataset",
        ),
        (
            vec!["inspect", "query", "bob/bob-public"],
            vec!["inspect", "query", "bob/bob-private"],
            "",
            "User has no 'read' permission in dataset",
        ),
        (
            vec![
                "sql",
                "--output-format",
                "csv",
                "-c",
                r#"select count(*) as cnt from "bob/bob-public""#,
            ],
            vec![
                "sql",
                "--output-format",
                "csv",
                "-c",
                r#"select count(*) as cnt from "bob/bob-private""#,
            ],
            "cnt\n2",
            r"table 'kamu\.kamu\.bob/bob-private' not found",
        ),
        (
            vec![
                "inspect",
                "lineage",
                "bob/bob-public",
                "--output-format",
                "shell",
            ],
            vec![
                "inspect",
                "lineage",
                "bob/bob-private",
                "--output-format",
                "shell",
            ],
            "bob-public: Root",
            "User has no 'read' permission in dataset",
        ),
    ] {
        let public_result = kamu.execute(&public_cmd).await.success();
        let public_stdout = String::from_utf8_lossy(&public_result.get_output().stdout);
        assert!(
            public_stdout.contains(public_stdout_part),
            "{public_cmd:?}: {public_stdout}"
        );

        kamu.assert_failure_command_execution(&private_cmd, None, Some([private_stderr]))
            .await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
