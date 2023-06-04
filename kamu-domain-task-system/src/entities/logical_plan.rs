// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric::DatasetID;

/////////////////////////////////////////////////////////////////////////////////////////

/// Represents logical steps needed to carry out a task
#[derive(Debug, Clone)]
pub enum LogicalPlan {
    /// Perform an update on a dataset like update from polling source or a
    /// derivative transform
    UpdateDataset(UpdateDataset),
}

/////////////////////////////////////////////////////////////////////////////////////////

/// Perform an update on a dataset like update from polling source or a
/// derivative transform
#[derive(Debug, Clone)]
pub struct UpdateDataset {
    /// ID of the dataset to update
    pub dataset_id: DatasetID,
}

/////////////////////////////////////////////////////////////////////////////////////////
