// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{InternalError, ResultIntoInternal};
use kamu_datasets::DatasetBlock;
use serde_json::json;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Serializes blocks into the JSON parameter of a `json_each($1)` bulk
/// insert. Binary columns travel as hex and must be decoded with `unhex()`.
pub(crate) fn dataset_blocks_json(blocks: &[DatasetBlock]) -> Result<String, InternalError> {
    serde_json::to_string(
        &blocks
            .iter()
            .map(|block| {
                json!({
                    "event_type": block.event_kind.to_string(),
                    "sequence_number": block.sequence_number,
                    "block_hash_bin": hex::encode(block.block_hash.digest()),
                    "block_payload": hex::encode(block.block_payload.as_ref()),
                })
            })
            .collect::<Vec<_>>(),
    )
    .int_err()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
