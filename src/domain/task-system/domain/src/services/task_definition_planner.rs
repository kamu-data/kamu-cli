// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use kamu_core::{CompactionPlan, PullOptions, PullPlanIterationJob, ResetPlan, ResolvedDataset};

use crate::{LogicalPlan, LogicalPlanProbe, TaskID};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait TaskDefinitionPlanner: Send + Sync {
    async fn prepare_task_definition(
        &self,
        task_id: TaskID,
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
    DeliverWebhook(TaskDefinitionDeliverWebhook),
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
    pub dataset_handle: odf::DatasetHandle,
    pub reset_plan: ResetPlan,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct TaskDefinitionHardCompact {
    pub target: ResolvedDataset,
    pub compaction_plan: CompactionPlan,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct TaskDefinitionDeliverWebhook {
    pub task_id: TaskID,
    pub webhook_subscription_id: uuid::Uuid,
    pub webhook_event_id: uuid::Uuid,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
