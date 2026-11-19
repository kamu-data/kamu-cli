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
    add_named_dataset,
};
use kamu_cli_puppet::KamuCliPuppet;
use kamu_cli_puppet::extensions::KamuCliPuppetExt;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_commands_deny_other_accounts_private_dataset(mut kamu: KamuCliPuppet) {
    let [alice, bob] = kamu.create_accounts(["alice", "bob"]).await;

    kamu.set_account(Some(bob));
    for (dataset_name, visibility) in [
        ("bob-private", odf::DatasetVisibility::Private),
        ("bob-public", odf::DatasetVisibility::Public),
    ] {
        add_named_dataset(
            &kamu,
            dataset_name,
            visibility,
            DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR,
        )
        .await;
        kamu.ingest_data(
            &odf::DatasetName::new_unchecked(dataset_name),
            DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
        )
        .await;
    }

    kamu.set_account(Some(alice));

    // Each command runs against both datasets, `<dataset>` stands for the alias.
    // Alice can read the public dataset but cannot write to it
    let read_denied = "User has no 'read' permission in dataset";
    for (command, stdin, public_expected, private_stderr) in [
        (
            vec!["log", "<dataset>"],
            "",
            PublicExpected::Succeeds("AddData"),
            read_denied,
        ),
        (
            vec!["inspect", "query", "<dataset>"],
            "",
            PublicExpected::Succeeds(""),
            read_denied,
        ),
        (
            vec![
                "sql",
                "--output-format",
                "csv",
                "-c",
                r#"select count(*) as cnt from "<dataset>""#,
            ],
            "",
            PublicExpected::Succeeds("cnt\n2"),
            r"table 'kamu\.kamu\.bob/bob-private' not found",
        ),
        (
            vec![
                "inspect",
                "lineage",
                "<dataset>",
                "--output-format",
                "shell",
            ],
            "",
            PublicExpected::Succeeds("bob-public: Root"),
            read_denied,
        ),
        (
            vec!["ingest", "<dataset>", "--stdin"],
            DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
            PublicExpected::Fails("User has no 'write' permission in dataset 'bob/bob-public'"),
            "Dataset not found: bob/bob-private",
        ),
    ] {
        let command_for = |dataset: &str| {
            command
                .iter()
                .map(|arg| arg.replace("<dataset>", dataset))
                .collect::<Vec<_>>()
        };
        let public_cmd = command_for("bob/bob-public");
        let private_cmd = command_for("bob/bob-private");

        match public_expected {
            PublicExpected::Succeeds(stdout_part) => {
                let public_result = kamu.execute_with_input(&public_cmd, stdin).await.success();
                let public_stdout = String::from_utf8_lossy(&public_result.get_output().stdout);
                assert!(
                    public_stdout.contains(stdout_part),
                    "{public_cmd:?}: {public_stdout}"
                );
            }
            PublicExpected::Fails(stderr) => {
                kamu.assert_failure_command_execution_with_input(
                    &public_cmd,
                    stdin,
                    None,
                    Some([stderr]),
                )
                .await;
            }
        }

        kamu.assert_failure_command_execution_with_input(
            &private_cmd,
            stdin,
            None,
            Some([private_stderr]),
        )
        .await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

enum PublicExpected {
    /// The command succeeds and its stdout contains the text
    Succeeds(&'static str),
    /// The command fails with an error matching the pattern
    Fails(&'static str),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
