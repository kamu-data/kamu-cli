// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_e2e_common::{
    DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT_STR,
    DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR,
};
use kamu_cli_puppet::extensions::KamuCliPuppetExt;
use kamu_cli_puppet::KamuCliPuppet;
use opendatafabric::DatasetName;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_verify_regular_dataset(kamu: KamuCliPuppet) {
    let dataset_name = DatasetName::new_unchecked("player-scores");

    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    let data = indoc::indoc!(
        r#"
            {"match_time": "2000-01-01", "match_id": 2, "player_id": "Bob", "score": 90}
        "#,
    );

    kamu.ingest_data(&dataset_name, data).await;

    let assert = kamu
        .execute(["verify", dataset_name.as_str()])
        .await
        .success();

    let stderr = std::str::from_utf8(&assert.get_output().stderr).unwrap();

    assert!(
        stderr.contains(indoc::indoc!(
            r#"
                1 dataset(s) are valid
            "#
        )),
        "Unexpected output:\n{stderr}",
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_verify_recursive(kamu: KamuCliPuppet) {
    let dataset_name = DatasetName::new_unchecked("player-scores");
    let dataset_derivative_name = DatasetName::new_unchecked("leaderboard");

    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    kamu.execute_with_input(
        ["add", "--stdin"],
        DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT_STR,
    )
    .await
    .success();

    let data = indoc::indoc!(
        r#"
            {"match_time": "2000-01-01", "match_id": 2, "player_id": "Bob", "score": 90}
        "#,
    );

    kamu.ingest_data(&dataset_name, data).await;

    kamu.execute(["pull", dataset_derivative_name.as_str()])
        .await
        .success();

    // Call verify without recursive flag
    let assert = kamu
        .execute(["verify", dataset_derivative_name.as_str()])
        .await
        .success();

    let stderr = std::str::from_utf8(&assert.get_output().stderr).unwrap();

    assert!(
        stderr.contains(indoc::indoc!(
            r#"
                1 dataset(s) are valid
            "#
        )),
        "Unexpected output:\n{stderr}",
    );

    // Call verify wit recursive flag
    let assert = kamu
        .execute(["verify", dataset_derivative_name.as_str(), "--recursive"])
        .await
        .success();

    let stderr = std::str::from_utf8(&assert.get_output().stderr).unwrap();

    assert!(
        stderr.contains(indoc::indoc!(
            r#"
                2 dataset(s) are valid
            "#
        )),
        "Unexpected output:\n{stderr}",
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_verify_integrity(kamu: KamuCliPuppet) {
    let dataset_name = DatasetName::new_unchecked("player-scores");

    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    let data = indoc::indoc!(
        r#"
            {"match_time": "2000-01-01", "match_id": 2, "player_id": "Bob", "score": 90}
        "#,
    );

    kamu.ingest_data(&dataset_name, data).await;

    let assert = kamu
        .execute(["verify", dataset_name.as_str(), "--integrity"])
        .await
        .success();

    let stderr = std::str::from_utf8(&assert.get_output().stderr).unwrap();

    assert!(
        stderr.contains(indoc::indoc!(
            r#"
                1 dataset(s) are valid
            "#
        )),
        "Unexpected output:\n{stderr}",
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
