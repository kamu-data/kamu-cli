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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DatasetLifecycleMessage {
    Created(DatasetLifecycleMessageCreated),
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetLifecycleMessageCreated {
    pub dataset_id: odf::DatasetID,
    pub owner_account_id: odf::AccountID,
    #[serde(default)]
    pub dataset_visibility: odf::DatasetVisibility,
    pub dataset_name: odf::DatasetName,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetLifecycleMessageDeleted {
    pub dataset_id: odf::DatasetID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
