// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::ResetPlan;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_task_system::task_definition_struct! {
    pub struct TaskDefinitionDatasetReset {
        pub dataset_handle: odf::DatasetHandle,
        pub reset_plan: ResetPlan,
    }
    => "dev.kamu.tasks.dataset.reset"
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
