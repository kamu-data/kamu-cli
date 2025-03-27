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

const DATASET_CHANGED_OUTBOX_VERSION: u32 = 1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DatasetChangedMessage {
    /// Message indicating that a dataset has been updated.
    Updated(DatasetUpdatedMessage),
}

impl DatasetChangedMessage {
    pub fn updated(
        dataset_id: &odf::DatasetID,
        maybe_prev_block_hash: Option<&odf::Multihash>,
        new_block_hash: &odf::Multihash,
    ) -> Self {
        Self::Updated(DatasetUpdatedMessage {
            dataset_id: dataset_id.clone(),
            maybe_prev_block_hash: maybe_prev_block_hash.cloned(),
            new_block_hash: new_block_hash.clone(),
        })
    }
}

impl Message for DatasetChangedMessage {
    fn version() -> u32 {
        DATASET_CHANGED_OUTBOX_VERSION
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Contains details about an updated dataset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetUpdatedMessage {
    /// The unique identifier of the dataset.
    pub dataset_id: odf::DatasetID,

    /// The previous block hash: this value will only be None
    /// for datasets that were just created.
    pub maybe_prev_block_hash: Option<odf::Multihash>,

    /// The new block hash after the update.
    pub new_block_hash: odf::Multihash,
}

impl DatasetUpdatedMessage {
    pub fn new(
        dataset_id: odf::DatasetID,
        maybe_prev_block_hash: Option<odf::Multihash>,
        new_block_hash: odf::Multihash,
    ) -> Self {
        Self {
            dataset_id,
            maybe_prev_block_hash,
            new_block_hash,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
