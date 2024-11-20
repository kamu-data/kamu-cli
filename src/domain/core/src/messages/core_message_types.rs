// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use messaging_outbox::Message;
use opendatafabric::{AccountID, DatasetID, DatasetName};
use serde::{Deserialize, Serialize};

use crate::DatasetVisibility;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const DATASET_LIFECYCLE_OUTBOX_VERSION: u32 = 1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DatasetLifecycleMessage {
    Created(DatasetLifecycleMessageCreated),
    DependenciesUpdated(DatasetLifecycleMessageDependenciesUpdated),
    Deleted(DatasetLifecycleMessageDeleted),
    Renamed(DatasetLifecycleMessageRenamed),
}

impl DatasetLifecycleMessage {
    pub fn created(
        dataset_id: DatasetID,
        owner_account_id: AccountID,
        dataset_visibility: DatasetVisibility,
        dataset_name: DatasetName,
    ) -> Self {
        Self::Created(DatasetLifecycleMessageCreated {
            dataset_id,
            owner_account_id,
            dataset_visibility,
            dataset_name,
        })
    }

    pub fn dependencies_updated(dataset_id: DatasetID, new_upstream_ids: Vec<DatasetID>) -> Self {
        Self::DependenciesUpdated(DatasetLifecycleMessageDependenciesUpdated {
            dataset_id,
            new_upstream_ids,
        })
    }

    pub fn deleted(dataset_id: DatasetID) -> Self {
        Self::Deleted(DatasetLifecycleMessageDeleted { dataset_id })
    }

    pub fn renamed(
        dataset_id: DatasetID,
        owner_account_id: AccountID,
        old_dataset_name: DatasetName,
        new_dataset_name: DatasetName,
    ) -> Self {
        Self::Renamed(DatasetLifecycleMessageRenamed {
            dataset_id,
            owner_account_id,
            old_dataset_name,
            new_dataset_name,
        })
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
    pub dataset_id: DatasetID,
    pub owner_account_id: AccountID,
    #[serde(default)]
    pub dataset_visibility: DatasetVisibility,
    pub dataset_name: DatasetName,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetLifecycleMessageDependenciesUpdated {
    pub dataset_id: DatasetID,
    pub new_upstream_ids: Vec<DatasetID>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetLifecycleMessageDeleted {
    pub dataset_id: DatasetID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetLifecycleMessageRenamed {
    pub dataset_id: DatasetID,
    pub owner_account_id: AccountID,
    pub old_dataset_name: DatasetName,
    pub new_dataset_name: DatasetName,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
