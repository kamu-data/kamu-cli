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
pub enum DatasetExternallyChangedMessage {
    /// Message indicating that a dataset has been updated.
    HttpIngest(DatasetExternallyChangedMessageHttpIngest),
    SmartTransferProtocolSync(DatasetExternallyChangedMessageSmtpSync),
}

impl DatasetExternallyChangedMessage {
    pub fn ingest_http(
        dataset_id: &odf::DatasetID,
        maybe_prev_block_hash: Option<&odf::Multihash>,
        new_block_hash: &odf::Multihash,
    ) -> Self {
        Self::HttpIngest(DatasetExternallyChangedMessageHttpIngest {
            dataset_id: dataset_id.clone(),
            maybe_prev_block_hash: maybe_prev_block_hash.cloned(),
            new_block_hash: new_block_hash.clone(),
        })
    }

    pub fn smart_transfer_protocol_sync(
        dataset_id: &odf::DatasetID,
        maybe_prev_block_hash: Option<&odf::Multihash>,
        new_block_hash: &odf::Multihash,
        account_name: Option<odf::AccountName>,
        is_force: bool,
    ) -> Self {
        Self::SmartTransferProtocolSync(DatasetExternallyChangedMessageSmtpSync {
            dataset_id: dataset_id.clone(),
            maybe_prev_block_hash: maybe_prev_block_hash.cloned(),
            new_block_hash: new_block_hash.clone(),
            account_name,
            is_force,
        })
    }
}

impl Message for DatasetExternallyChangedMessage {
    fn version() -> u32 {
        DATASET_CHANGED_OUTBOX_VERSION
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Contains details about an updated dataset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetExternallyChangedMessageHttpIngest {
    /// The unique identifier of the dataset.
    pub dataset_id: odf::DatasetID,

    /// The previous block hash: this value will only be None
    /// for datasets that were just created.
    pub maybe_prev_block_hash: Option<odf::Multihash>,

    /// The new block hash after the update.
    pub new_block_hash: odf::Multihash,
}

impl DatasetExternallyChangedMessageHttpIngest {
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

/// Contains details about an updated dataset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetExternallyChangedMessageSmtpSync {
    /// The unique identifier of the dataset.
    pub dataset_id: odf::DatasetID,

    /// The previous block hash: this value will only be None
    /// for datasets that were just created.
    pub maybe_prev_block_hash: Option<odf::Multihash>,

    /// The new block hash after the update.
    pub new_block_hash: odf::Multihash,

    // Account of sync actor
    pub account_name: Option<odf::AccountName>,

    pub is_force: bool,
}

impl DatasetExternallyChangedMessageSmtpSync {
    pub fn new(
        dataset_id: odf::DatasetID,
        maybe_prev_block_hash: Option<odf::Multihash>,
        new_block_hash: odf::Multihash,
        account_name: Option<odf::AccountName>,
        is_force: bool,
    ) -> Self {
        Self {
            dataset_id,
            maybe_prev_block_hash,
            new_block_hash,
            account_name,
            is_force,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
