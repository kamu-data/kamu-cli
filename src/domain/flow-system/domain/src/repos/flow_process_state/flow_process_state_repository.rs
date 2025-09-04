// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::{FlowBinding, FlowTriggerStopPolicy};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FlowProcessStateRepository: Send + Sync {
    /// Insert a new row when a trigger is created.
    async fn insert_process(
        &self,
        flow_binding: FlowBinding,
        paused_manual: bool,
        stop_policy: FlowTriggerStopPolicy,
        trigger_event_id: i64,
    ) -> Result<(), InternalError>;

    /// Update trigger-related fields
    async fn update_trigger_state(
        &self,
        flow_binding: FlowBinding,
        paused_manual: Option<bool>,
        stop_policy: Option<FlowTriggerStopPolicy>,
        trigger_event_id: i64,
    ) -> Result<(), InternalError>;

    /// Apply a flow result (success or failure).
    async fn apply_flow_result(
        &self,
        flow_binding: FlowBinding,
        success: bool,
        event_time: chrono::DateTime<chrono::Utc>,
        flow_event_id: i64,
    ) -> Result<(), InternalError>;

    /// Remove row when trigger is deleted.
    async fn delete_process(&self, flow_binding: FlowBinding) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
