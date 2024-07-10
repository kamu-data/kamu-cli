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

pub const MESSAGE_KAMU_CORE_DATASET_CREATED: &str = "dev.kamu.core.dataset.created";
pub const MESSAGE_KAMU_CORE_DATASET_DEPENDENCIES_UPDATED: &str =
    "dev.kamu.core.dataset.dependencies_updated";
pub const MESSAGE_KAMU_CORE_DATASET_DELETED: &str = "dev.kamu.core.dataset.deleted";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetCreatedMessage {
    pub dataset_id: DatasetID,
    pub owner_account_id: AccountID,
}

impl Message for DatasetCreatedMessage {
    fn type_name(&self) -> &'static str {
        MESSAGE_KAMU_CORE_DATASET_CREATED
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetDeletedMessage {
    pub dataset_id: DatasetID,
}

impl Message for DatasetDeletedMessage {
    fn type_name(&self) -> &'static str {
        MESSAGE_KAMU_CORE_DATASET_DELETED
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetDependenciesUpdatedMessage {
    pub dataset_id: DatasetID,
    pub new_upstream_ids: Vec<DatasetID>,
}

impl Message for DatasetDependenciesUpdatedMessage {
    fn type_name(&self) -> &'static str {
        MESSAGE_KAMU_CORE_DATASET_DEPENDENCIES_UPDATED
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
