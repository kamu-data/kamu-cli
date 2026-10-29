// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::{PollingIngestOptions, PullLocalTarget, PullPlanIterationJob, SyncOptions};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_task_system::task_definition_struct! {
    pub struct TaskDefinitionDatasetUpdate {
        pub ingest_options: PollingIngestOptions,
        pub sync_options: SyncOptions,
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
    /// The dataset to sync from a remote source. Its `HEAD` is read when the
    /// task runs, and the transfer resolves and opens the source.
    Sync {
        target: odf::DatasetHandle,
        remote_ref: odf::DatasetRefRemote,
    },
}

impl TryFrom<PullPlanIterationJob> for DatasetUpdateJob {
    type Error = UpdateTargetMissingError;

    fn try_from(pull_job: PullPlanIterationJob) -> Result<Self, Self::Error> {
        match pull_job {
            PullPlanIterationJob::Ingest(pii) => Ok(Self::Ingest(pii.target)),
            PullPlanIterationJob::Transform(pti) => Ok(Self::Transform(pti.target)),
            PullPlanIterationJob::Sync(psi) => match psi.local_target {
                PullLocalTarget::Existing(target) => Ok(Self::Sync {
                    target,
                    remote_ref: psi.remote_ref,
                }),
                PullLocalTarget::ToCreate(alias) => Err(UpdateTargetMissingError { alias }),
            },
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// An update task syncs into an existing dataset, so a pull that would create
/// its target is not an update task
#[derive(Debug, thiserror::Error)]
#[error("An update task syncs into an existing dataset, but {alias} does not exist")]
pub struct UpdateTargetMissingError {
    pub alias: odf::DatasetAlias,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
