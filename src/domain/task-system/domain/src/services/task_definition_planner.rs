// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use kamu_core::{CompactionOptions, PullOptions, PullPlanIterationJob, ResolvedDataset};
use opendatafabric::Multihash;

use crate::{LogicalPlan, LogicalPlanProbe};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait TaskDefinitionPlanner: Send + Sync {
    async fn prepare_task_definition(
        &self,
        logical_plan: &LogicalPlan,
    ) -> Result<TaskDefinition, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum TaskDefinition {
    Probe(TaskDefinitionProbe),
    Update(TaskDefinitionUpdate),
    Reset(TaskDefinitionReset),
    HardCompact(TaskDefinitionHardCompact),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct TaskDefinitionProbe {
    pub probe: LogicalPlanProbe,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct TaskDefinitionUpdate {
    pub pull_options: PullOptions,
    pub pull_job: PullPlanIterationJob,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct TaskDefinitionReset {
    pub target: ResolvedDataset,
    pub new_head_hash: Option<Multihash>,
    pub old_head_hash: Option<Multihash>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct TaskDefinitionHardCompact {
    pub target: ResolvedDataset,
    pub compaction_options: CompactionOptions,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
