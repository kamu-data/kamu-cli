// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

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

    let data_path_str = data_path.as_os_str().to_str().unwrap().to_string();

    kamu.assert_success_command_execution_with_env(
        ["pull", "test.pull-from-file"],
        vec![("data_path".to_string(), data_path_str)],
        None,
        Some([indoc::indoc!(
            r#"
                1 dataset(s) updated
            "#
        )]),
    )
    .await;
}
