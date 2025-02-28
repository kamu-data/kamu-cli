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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetReferenceMessage {
    pub dataset_id: odf::DatasetID,
    pub block_ref: odf::BlockRef,
    pub maybe_prev_block_hash: Option<odf::Multihash>,
    pub new_block_hash: odf::Multihash,
}

impl DatasetReferenceMessage {
    pub fn new(
        dataset_id: odf::DatasetID,
        block_ref: odf::BlockRef,
        maybe_prev_block_hash: Option<odf::Multihash>,
        new_block_hash: odf::Multihash,
    ) -> Self {
        Self {
            dataset_id,
            block_ref,
            maybe_prev_block_hash,
            new_block_hash,
        }
    }
}

impl Message for DatasetReferenceMessage {
    fn version() -> u32 {
        DATASET_REFERENCE_OUTBOX_VERSION
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
