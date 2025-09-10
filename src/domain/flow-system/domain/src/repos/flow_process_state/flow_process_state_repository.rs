// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use event_sourcing::EventID;
use internal_error::InternalError;
use thiserror::Error;

use crate::{FlowBinding, FlowTriggerStopPolicy};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FlowProcessStateRepository: Send + Sync {
    /// Insert a new row when a trigger is created.
    async fn insert_process(
        &self,
        flow_binding: FlowBinding,
        sort_key: String,
        paused_manual: bool,
        stop_policy: FlowTriggerStopPolicy,
        trigger_event_id: EventID,
    ) -> Result<(), FlowProcessInsertError>;

    /// Update trigger-related fields
    async fn update_trigger_state(
        &self,
        flow_binding: FlowBinding,
        paused_manual: Option<bool>,
        stop_policy: Option<FlowTriggerStopPolicy>,
        trigger_event_id: EventID,
    ) -> Result<(), FlowProcessUpdateError>;

    /// Apply a flow result (success or failure).
    async fn apply_flow_result(
        &self,
        flow_binding: FlowBinding,
        success: bool,
        event_time: DateTime<Utc>,
        flow_event_id: EventID,
    ) -> Result<(), FlowProcessUpdateError>;

    /// Schedule a flow to run at a specific time.
    async fn schedule_flow(
        &self,
        flow_binding: FlowBinding,
        planned_at: DateTime<Utc>,
        flow_event_id: EventID,
    ) -> Result<(), FlowProcessUpdateError>;

    /// Remove row when trigger is deleted.
    async fn delete_process(&self, flow_binding: FlowBinding)
    -> Result<(), FlowProcessDeleteError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum FlowProcessInsertError {
    #[error("Process already exists for the given flow binding: {flow_binding:?}")]
    AlreadyExists { flow_binding: FlowBinding },

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum FlowProcessUpdateError {
    #[error(transparent)]
    NotFound(FlowProcessNotFoundError),

    #[error(transparent)]
    ConcurrentModification(FlowProcessConcurrentModificationError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum FlowProcessLoadError {
    #[error(transparent)]
    NotFound(FlowProcessNotFoundError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum FlowProcessSaveError {
    #[error(transparent)]
    ConcurrentModification(FlowProcessConcurrentModificationError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum FlowProcessDeleteError {
    #[error(transparent)]
    NotFound(FlowProcessNotFoundError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("Concurrent modification for the given flow binding: {flow_binding:?}")]
pub struct FlowProcessConcurrentModificationError {
    pub flow_binding: FlowBinding,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("Process not found for the given flow binding: {flow_binding:?}")]
pub struct FlowProcessNotFoundError {
    pub flow_binding: FlowBinding,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
