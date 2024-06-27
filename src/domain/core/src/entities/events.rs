// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric::{AccountID, DatasetID};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatasetEvent {
    Created(DatasetEventCreated),
    Deleted(DatasetEventDeleted),
    DependenciesUpdated(DatasetEventDependenciesUpdated),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetEventCreated {
    pub dataset_id: DatasetID,
    pub owner_account_id: AccountID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetEventDeleted {
    pub dataset_id: DatasetID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetEventDependenciesUpdated {
    pub dataset_id: DatasetID,
    pub new_upstream_ids: Vec<DatasetID>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
