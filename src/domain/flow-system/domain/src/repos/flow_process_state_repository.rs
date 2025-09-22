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

use crate::{FlowBinding, FlowOutcome, FlowScope, FlowTriggerStopPolicy};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FlowProcessStateRepository: Send + Sync {
    /// Upsert a row when a trigger is created or updated.
    async fn upsert_process_state_on_trigger_event(
        &self,
        trigger_event_id: EventID,
        flow_binding: FlowBinding,
        paused_manual: bool,
        stop_policy: FlowTriggerStopPolicy,
    ) -> Result<(), FlowProcessUpsertError>;

    /// Apply a flow result.
    async fn apply_flow_result(
        &self,
        flow_event_id: EventID,
        flow_binding: &FlowBinding,
        flow_outcome: &FlowOutcome,
        event_time: DateTime<Utc>,
    ) -> Result<(), FlowProcessFlowEventError>;

    /// React to flow being scheduled.
    async fn on_flow_scheduled(
        &self,
        flow_event_id: EventID,
        flow_binding: &FlowBinding,
        planned_at: DateTime<Utc>,
    ) -> Result<(), FlowProcessFlowEventError>;

    /// Remove all rows for the given scope.
    async fn delete_process_states_by_scope(
        &self,
        scope: &FlowScope,
    ) -> Result<(), FlowProcessDeleteError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum FlowProcessUpsertError {
    #[error(transparent)]
    ConcurrentModification(FlowProcessConcurrentModificationError),

    #[error(transparent)]
    Internal(#[from] InternalError),
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
pub enum FlowProcessFlowEventError {
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
