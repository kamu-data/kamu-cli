// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric::DatasetID;

use crate::TaskOutcome;

/////////////////////////////////////////////////////////////////////////////////////////

/// Represents logical steps needed to carry out a task
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalPlan {
    /// Perform an update on a dataset like update from polling source or a
    /// derivative transform
    UpdateDataset(UpdateDataset),
    /// A task that can be used for testing the scheduling system
    Probe(Probe),
}

/////////////////////////////////////////////////////////////////////////////////////////

/// Perform an update on a dataset like update from polling source or a
/// derivative transform
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateDataset {
    /// ID of the dataset to update
    pub dataset_id: DatasetID,
}

/////////////////////////////////////////////////////////////////////////////////////////

/// A task that can be used for testing the scheduling system
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Probe {
    /// ID of the dataset this task should be associated with
    pub dataset_id: Option<DatasetID>,
    pub busy_time: Option<std::time::Duration>,
    pub end_with_outcome: Option<TaskOutcome>,
}
