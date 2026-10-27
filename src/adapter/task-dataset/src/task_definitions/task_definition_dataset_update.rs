// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::{PullOptions, PullPlanIterationJob, PullSyncItem};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_task_system::task_definition_struct! {
    pub struct TaskDefinitionDatasetUpdate {
        pub pull_options: PullOptions,
        pub job: DatasetUpdateJob,
    }
    => "dev.kamu.tasks.dataset.update"
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum DatasetUpdateJob {
    /// The root dataset to ingest. The ingest is planned when the task runs,
    /// in the transaction that also resolves its environment variables.
    Ingest(odf::DatasetHandle),
    /// The derivative to transform. The transform is planned when the task
    /// runs, in the transaction that elaborates it.
    Transform(odf::DatasetHandle),
    Sync(PullSyncItem),
}

impl From<PullPlanIterationJob> for DatasetUpdateJob {
    fn from(pull_job: PullPlanIterationJob) -> Self {
        match pull_job {
            PullPlanIterationJob::Ingest(pii) => Self::Ingest(pii.target.take_handle()),
            PullPlanIterationJob::Transform(pti) => Self::Transform(pti.target.take_handle()),
            PullPlanIterationJob::Sync(psi) => Self::Sync(psi),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
