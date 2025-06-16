// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A task to perform the resetting of a dataset
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LogicalPlanDatasetReset {
    pub dataset_id: odf::DatasetID,
    pub new_head_hash: Option<odf::Multihash>,
    pub old_head_hash: Option<odf::Multihash>,
    pub recursive: bool,
}

impl LogicalPlanDatasetReset {
    pub const TYPE_ID: &str = "ResetDataset";
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
