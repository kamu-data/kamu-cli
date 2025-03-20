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

const DATASET_LIFECYCLE_OUTBOX_VERSION: u32 = 1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents messages related to the lifecycle of a dataset
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DatasetLifecycleMessage {
    /// Message indicating that a dataset has been created.
    Created(DatasetLifecycleMessageCreated),

    /// Message indicating that a dataset has been renamed.
    Renamed(DatasetLifecycleMessageRenamed),

    /// Message indicating that a dataset has been deleted.
    Deleted(DatasetLifecycleMessageDeleted),
}

impl DatasetLifecycleMessage {
    pub fn created(
        dataset_id: odf::DatasetID,
        owner_account_id: odf::AccountID,
        dataset_visibility: odf::DatasetVisibility,
        dataset_name: odf::DatasetName,
    ) -> Self {
        Self::Created(DatasetLifecycleMessageCreated {
            dataset_id,
            owner_account_id,
            dataset_visibility,
            dataset_name,
        })
    }

    pub fn renamed(dataset_id: odf::DatasetID, new_dataset_name: odf::DatasetName) -> Self {
        Self::Renamed(DatasetLifecycleMessageRenamed {
            dataset_id,
            new_dataset_name,
        })
    }

    pub fn deleted(dataset_id: odf::DatasetID) -> Self {
        Self::Deleted(DatasetLifecycleMessageDeleted { dataset_id })
    }
}

impl Message for DatasetLifecycleMessage {
    fn version() -> u32 {
        DATASET_LIFECYCLE_OUTBOX_VERSION
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Contains details about a newly created dataset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetLifecycleMessageCreated {
    /// The unique identifier of the dataset.
    pub dataset_id: odf::DatasetID,

    /// The account ID of the dataset owner.
    pub owner_account_id: odf::AccountID,

    /// The visibility setting of the dataset
    #[serde(default)]
    pub dataset_visibility: odf::DatasetVisibility,

    /// The name assigned to the dataset.
    pub dataset_name: odf::DatasetName,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Contains details about a dataset that has been renamed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetLifecycleMessageRenamed {
    /// The unique identifier of the dataset.
    pub dataset_id: odf::DatasetID,

    /// The new name assigned to the dataset.
    pub new_dataset_name: odf::DatasetName,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Contains details about a dataset that has been deleted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetLifecycleMessageDeleted {
    /// The unique identifier of the dataset.
    pub dataset_id: odf::DatasetID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
