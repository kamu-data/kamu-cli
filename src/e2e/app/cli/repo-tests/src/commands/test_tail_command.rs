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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_tail(kamu: KamuCliPuppet) {
    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    kamu.assert_success_command_execution(
        ["tail", "player-scores", "--output-format", "table"],
        Some(indoc::indoc!(
            r#"
            ┌┐
            ││
            ├┤
            ││
            └┘
            "#
        )),
        None::<Vec<&str>>,
    )
    .await;

    kamu.execute_with_input(
        ["ingest", "player-scores", "--stdin"],
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    )
    .await
    .success();

    kamu.assert_success_command_execution(
        ["tail", "player-scores", "--output-format", "table"],
        Some(indoc::indoc!(
            r#"
            ┌────────┬────┬──────────────────────┬──────────────────────┬──────────┬───────────┬───────┐
            │ offset │ op │     system_time      │      match_time      │ match_id │ player_id │ score │
            ├────────┼────┼──────────────────────┼──────────────────────┼──────────┼───────────┼───────┤
            │      0 │ +A │ 2050-01-02T03:04:05Z │ 2000-01-01T00:00:00Z │        1 │     Alice │   100 │
            │      1 │ +A │ 2050-01-02T03:04:05Z │ 2000-01-01T00:00:00Z │        1 │       Bob │    80 │
            └────────┴────┴──────────────────────┴──────────────────────┴──────────┴───────────┴───────┘
            "#
        )),
        None::<Vec<&str>>,
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
