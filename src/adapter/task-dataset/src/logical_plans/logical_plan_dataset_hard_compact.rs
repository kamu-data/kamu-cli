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
pub struct LogicalPlanDatasetHardCompact {
    pub dataset_id: odf::DatasetID,
    pub max_slice_size: Option<u64>,
    pub max_slice_records: Option<u64>,
    pub keep_metadata_only: bool,
}

impl LogicalPlanDatasetHardCompact {
    pub const TYPE_ID: &str = "HardCompactDataset";
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
