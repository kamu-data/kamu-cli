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
    DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR,
};
use kamu_cli_puppet::extensions::KamuCliPuppetExt;
use kamu_cli_puppet::KamuCliPuppet;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_reset(kamu: KamuCliPuppet) {
    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    let block_records_after_ingesting = kamu.list_blocks(&DATASET_ROOT_PLAYER_NAME).await;

    pretty_assertions::assert_eq!(3, block_records_after_ingesting.len());

    let set_vocab_block_record = &block_records_after_ingesting[0];

    assert_matches!(
        &set_vocab_block_record.block.event,
        odf::MetadataEvent::SetVocab(_),
    );

    pretty_assertions::assert_eq!(3, block_records_after_ingesting.len());

    kamu.execute_with_input(
        ["ingest", "player-scores", "--stdin"],
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    )
    .await
    .success();

    pretty_assertions::assert_eq!(5, kamu.list_blocks(&DATASET_ROOT_PLAYER_NAME).await.len());

    let set_vocab_block_hash = set_vocab_block_record
        .block_hash
        .as_multibase()
        .to_stack_string();

    kamu.assert_success_command_execution(
        [
            "--yes",
            "reset",
            "player-scores",
            set_vocab_block_hash.as_str(),
        ],
        None,
        Some(["Dataset was reset"]),
    )
    .await;

    let block_records_after_resetting = kamu.list_blocks(&DATASET_ROOT_PLAYER_NAME).await;

    pretty_assertions::assert_eq!(block_records_after_ingesting, block_records_after_resetting);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
