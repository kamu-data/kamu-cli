// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use bytes::Bytes;
use chrono::{TimeZone, Utc};
use odf_metadata::serde::flatbuffers::FlatbuffersMetadataBlockSerializer;
use odf_metadata::serde::MetadataBlockSerializer;
use odf_metadata::*;
use opendatafabric_storage::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_insert_block(repo: &dyn MetadataBlockRepository) {
    let (block, block_data, hash, block_size) = create_block();

    assert!(!repo.contains_block(&hash).await.unwrap());
    assert_matches!(repo.get_block(&hash).await, Err(GetBlockError::NotFound(_)),);

    assert_eq!(
        repo.insert_block(&block, InsertOpts::default())
            .await
            .unwrap(),
        InsertBlockResult { hash: hash.clone() }
    );

    assert_matches!(repo.contains_block(&hash).await, Ok(true));
    assert_matches!(repo.get_block(&hash).await, Ok(gotten_block) if gotten_block == block);
    assert_matches!(repo.get_block(&hash).await, Ok(gotten_block) if gotten_block == block);
    assert_matches!(repo.get_block_size(&hash).await, Ok(gotten_block_size) if gotten_block_size == block_size);
    assert_matches!(repo.get_block_data(&hash).await, Ok(gotten_block_data) if gotten_block_data == block_data);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn create_block() -> (MetadataBlock, Bytes, Multihash, u64) {
    let block = MetadataBlock {
        system_time: Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap(),
        prev_block_hash: None,
        sequence_number: 0,
        event: MetadataEvent::SetInfo(SetInfo {
            description: Some("foo".to_string()),
            keywords: None,
        }),
    };
    let block_data = FlatbuffersMetadataBlockSerializer
        .write_manifest(&block)
        .unwrap();
    let hash = Multihash::from_multibase(
        "f16205603b882241c71351baf996d6dba7e3ddbd571457e93c1cd282bdc61f9fed5f2",
    )
    .unwrap();
    let block_size = 112;

    (block, Bytes::copy_from_slice(&block_data), hash, block_size)
}
