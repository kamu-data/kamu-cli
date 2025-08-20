// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_task_system::logical_plan_struct! {
    /// A task to perform the resetting of a dataset to metadata only nodes
    pub struct LogicalPlanDatasetResetToMetadata {
        pub dataset_id: odf::DatasetID,
    }
    => "ResetDatasetToMetadata"
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
