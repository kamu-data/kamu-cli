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

pub async fn test_read_commands_deny_other_accounts_private_dataset(mut kamu: KamuCliPuppet) {
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

    // Each command runs against both datasets, `<dataset>` stands for the alias
    for (command, public_stdout_part, private_stderr) in [
        (
            vec!["log", "<dataset>"],
            "AddData",
            "User has no 'read' permission in dataset",
        ),
        (
            vec!["inspect", "query", "<dataset>"],
            "",
            "User has no 'read' permission in dataset",
        ),
        (
            vec![
                "sql",
                "--output-format",
                "csv",
                "-c",
                r#"select count(*) as cnt from "<dataset>""#,
            ],
            "cnt\n2",
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
            "bob-public: Root",
            "User has no 'read' permission in dataset",
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
