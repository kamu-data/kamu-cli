// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use internal_error::InternalError;
use kamu_flow_system::{FlowActivationCause, FlowBinding, FlowTriggerState};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[mockall::automock]
#[async_trait::async_trait]
pub trait FlowSchedulingService: Send + Sync {
    async fn try_schedule_auto_polling_flow_continuation_if_enabled(
        &self,
        flow_finish_time: DateTime<Utc>,
        flow_binding: &FlowBinding,
        trigger_state: &FlowTriggerState,
    ) -> Result<(), InternalError>;

    async fn schedule_late_flow_activations(
        &self,
        flow_success_time: DateTime<Utc>,
        flow_binding: &FlowBinding,
        late_activation_causes: &[FlowActivationCause],
    ) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
