// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;

use kamu_cli_e2e_common::{DATASET_FETCH_FROM_FILE_STR, DATASET_FETCH_FROM_FILE_STR_DATA};
use kamu_cli_puppet::extensions::KamuCliPuppetExt;
use kamu_cli_puppet::KamuCliPuppet;

pub async fn test_pull_from_file_success(kamu: KamuCliPuppet) {
    kamu.assert_success_command_execution_with_input(
        ["add", "--stdin"],
        DATASET_FETCH_FROM_FILE_STR,
        None,
        Some([indoc::indoc!(
            r#"
            Added: test.pull-from-file
            Added 1 dataset(s)
            "#
        )]),
    )
    .await;

    let data_path = kamu.workspace_path().join("data.csv");
    std::fs::write(&data_path, DATASET_FETCH_FROM_FILE_STR_DATA).unwrap();

    let mut workspace_dir = PathBuf::from(kamu.workspace_path());
    workspace_dir.push(""); // ensure trailing separator
    let workspace_dir = workspace_dir.as_os_str().to_str().unwrap().to_string();

    kamu.assert_success_command_execution_with_env(
        ["pull", "test.pull-from-file"],
        vec![("workspace_dir".to_string(), workspace_dir)],
        None,
        Some([indoc::indoc!(
            r#"
                1 dataset(s) updated
            "#
        )]),
    )
    .await;
}

pub async fn test_pull_from_file_failure(kamu: KamuCliPuppet) {
    kamu.assert_success_command_execution_with_input(
        ["add", "--stdin"],
        DATASET_FETCH_FROM_FILE_STR,
        None,
        Some([indoc::indoc!(
            r#"
            Added: test.pull-from-file
            Added 1 dataset(s)
            "#
        )]),
    )
    .await;

    let data_path = kamu.workspace_path().join("data.csv");
    std::fs::write(&data_path, DATASET_FETCH_FROM_FILE_STR_DATA).unwrap();

    kamu.assert_failure_command_execution(
        ["pull", "test.pull-from-file"],
        None,
        Some([indoc::indoc!(
            r#"
                Failed to pull test.pull-from-file: Missing values for variable(s): 'env.data_dir || env.workspace_dir'
            "#
        )]),
    )
        .await;
}
