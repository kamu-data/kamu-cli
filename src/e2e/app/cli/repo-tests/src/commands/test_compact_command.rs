// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use kamu_cli_e2e_common::{
    DATASET_ROOT_PLAYER_NAME,
    DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_2,
    DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR,
};
use kamu_cli_puppet::extensions::KamuCliPuppetExt;
use kamu_cli_puppet::KamuCliPuppet;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_compact_hard(kamu: KamuCliPuppet) {
    let dataset_name = DATASET_ROOT_PLAYER_NAME.clone();

    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    kamu.ingest_data(
        &dataset_name,
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    )
    .await;
    kamu.ingest_data(
        &dataset_name,
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_2,
    )
    .await;

    let blocks_before_compacting = kamu.list_blocks(&dataset_name).await;

    kamu.assert_success_command_execution(
        [
            "--yes",
            "system",
            "compact",
            dataset_name.as_str(),
            "--hard",
        ],
        None,
        Some([r#"1 dataset\(s\) were compacted"#]),
    )
    .await;

    let blocks_after_compacting = kamu.list_blocks(&dataset_name).await;
    pretty_assertions::assert_eq!(
        blocks_before_compacting.len() - 1,
        blocks_after_compacting.len()
    );
    assert_matches!(
        blocks_after_compacting.first().unwrap().block.event,
        odf::MetadataEvent::AddData(_)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_compact_keep_metadata_only(kamu: KamuCliPuppet) {
    let dataset_name = DATASET_ROOT_PLAYER_NAME.clone();

    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    kamu.ingest_data(
        &dataset_name,
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    )
    .await;
    kamu.ingest_data(
        &dataset_name,
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_2,
    )
    .await;

    let blocks_before_compacting = kamu.list_blocks(&dataset_name).await;

    kamu.assert_success_command_execution(
        [
            "--yes",
            "system",
            "compact",
            dataset_name.as_str(),
            "--hard",
            "--keep-metadata-only",
        ],
        None,
        Some([r#"1 dataset\(s\) were compacted"#]),
    )
    .await;

    let blocks_after_compacting = kamu.list_blocks(&dataset_name).await;
    pretty_assertions::assert_eq!(
        blocks_before_compacting.len() - 2,
        blocks_after_compacting.len()
    );
    assert_matches!(
        blocks_after_compacting.first().unwrap().block.event,
        odf::MetadataEvent::SetDataSchema(_)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_compact_verify(kamu: KamuCliPuppet) {
    let dataset_name = DATASET_ROOT_PLAYER_NAME.clone();

    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    kamu.ingest_data(
        &dataset_name,
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    )
    .await;
    kamu.ingest_data(
        &dataset_name,
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_2,
    )
    .await;

    let blocks_before_compacting = kamu.list_blocks(&dataset_name).await;

    kamu.assert_success_command_execution(
        [
            "--yes",
            "system",
            "compact",
            dataset_name.as_str(),
            "--hard",
            "--verify",
        ],
        None,
        Some([
            "verify with target_alias: player-scores",
            r#"1 dataset\(s\) were compacted"#,
        ]),
    )
    .await;

    let blocks_after_compacting = kamu.list_blocks(&dataset_name).await;
    pretty_assertions::assert_eq!(
        blocks_before_compacting.len() - 1,
        blocks_after_compacting.len()
    );
    assert_matches!(
        blocks_after_compacting.first().unwrap().block.event,
        odf::MetadataEvent::AddData(_)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
