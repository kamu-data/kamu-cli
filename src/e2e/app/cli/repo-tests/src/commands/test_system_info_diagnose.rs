// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_e2e_common::{
    DATASET_ROOT_PLAYER_NAME,
    DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR,
};
use kamu_cli_puppet::KamuCliPuppet;
use kamu_cli_puppet::extensions::KamuCliPuppetExt;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_system_diagnose(kamu: KamuCliPuppet) {
    kamu.execute(["system", "diagnose"]).await.success();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_system_diagnose_verifies_datasets(kamu: KamuCliPuppet) {
    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();
    kamu.ingest_data(
        &DATASET_ROOT_PLAYER_NAME,
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    )
    .await;

    // Diagnose reports a failed check and still exits successfully
    let assert = kamu.execute(["system", "diagnose"]).await.success();
    let stdout = std::str::from_utf8(&assert.get_output().stdout).unwrap();

    assert!(stdout.contains("workspace consistent... ok"), "{stdout}");
    assert!(stdout.contains("Kamu environment is healthy"), "{stdout}");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
