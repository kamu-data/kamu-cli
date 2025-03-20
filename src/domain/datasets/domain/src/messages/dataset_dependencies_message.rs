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

const DATASET_DEPENDENCIES_OUTBOX_VERSION: u32 = 1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents messages related to dataset dependencies
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DatasetDependenciesMessage {
    Updated(DatasetDependenciesMessageUpdated),
}

impl DatasetDependenciesMessage {
    pub fn updated(
        dataset_id: &odf::DatasetID,
        added_upstream_ids: Vec<odf::DatasetID>,
        removed_upstream_ids: Vec<odf::DatasetID>,
    ) -> Self {
        Self::Updated(DatasetDependenciesMessageUpdated {
            dataset_id: dataset_id.clone(),
            added_upstream_ids,
            removed_upstream_ids,
        })
    }
}

impl Message for DatasetDependenciesMessage {
    fn version() -> u32 {
        DATASET_DEPENDENCIES_OUTBOX_VERSION
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Contains details about an update to dataset dependencies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetDependenciesMessageUpdated {
    /// The unique identifier of the dataset
    pub dataset_id: odf::DatasetID,

    /// List of newly added upstream dataset dependencies
    pub added_upstream_ids: Vec<odf::DatasetID>,

    /// List of removed upstream dataset dependencies
    pub removed_upstream_ids: Vec<odf::DatasetID>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
