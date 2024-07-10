// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use messaging_outbox::Message;
use opendatafabric::{AccountID, DatasetID};
use serde::{Deserialize, Serialize};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DatasetLifecycleMessage {
    Created(DatasetLifecycleMessageCreated),
    DependenciesUpdated(DatasetLifecycleMessageDependenciesUpdated),
    Deleted(DatasetLifecycleMessageDeleted),
}

impl DatasetLifecycleMessage {
    pub fn created(dataset_id: DatasetID, owner_account_id: AccountID) -> Self {
        Self::Created(DatasetLifecycleMessageCreated {
            dataset_id,
            owner_account_id,
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
}

impl Message for DatasetLifecycleMessage {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetLifecycleMessageCreated {
    pub dataset_id: DatasetID,
    pub owner_account_id: AccountID,
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
