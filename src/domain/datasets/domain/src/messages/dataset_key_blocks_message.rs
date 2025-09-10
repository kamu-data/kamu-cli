// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use messaging_outbox::Message;
use serde::{Deserialize, Serialize};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const DATASET_REFERENCE_OUTBOX_VERSION: u32 = 1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents messages related to dataset key blocks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DatasetKeyBlocksMessage {
    /// Message indicating that dataset key blocks have been introduced.
    Introduced(DatasetKeyBlocksMessageIntroduced),
}

impl DatasetKeyBlocksMessage {
    pub fn introduced(
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
        tail_key_block_hash: &odf::Multihash,
        head_key_block_hash: &odf::Multihash,
        key_blocks_event_flags: odf::metadata::MetadataEventTypeFlags,
        divergence_detected: bool,
    ) -> Self {
        Self::Introduced(DatasetKeyBlocksMessageIntroduced {
            dataset_id: dataset_id.clone(),
            block_ref: block_ref.clone(),
            tail_key_block_hash: tail_key_block_hash.clone(),
            head_key_block_hash: head_key_block_hash.clone(),
            key_blocks_event_flags,
            divergence_detected,
        })
    }
}

impl Message for DatasetKeyBlocksMessage {
    fn version() -> u32 {
        DATASET_REFERENCE_OUTBOX_VERSION
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Contains details about introduced dataset key blocks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetKeyBlocksMessageIntroduced {
    /// The unique identifier of the dataset.
    pub dataset_id: odf::DatasetID,

    /// The reference for which key blocks were introduced.
    pub block_ref: odf::BlockRef,

    /// The tail interval key block hash (inclusive).
    /// Note: Data blocks might be present within the interval (exclusive).
    pub tail_key_block_hash: odf::Multihash,

    /// The head interval key block hash (inclusive) where key blocks are
    /// present.
    /// Note: Data blocks might be present within the interval (exclusive).
    pub head_key_block_hash: odf::Multihash,

    /// Key blocks event flags present within the interval.
    pub key_blocks_event_flags: odf::metadata::MetadataEventTypeFlags,

    /// A flag indicating that a chain of blocks has been rewritten.
    /// This can happen as a result of a dataset reset or force push.
    pub divergence_detected: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
