// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use {/* kamu_adapter_task_webhook as atw, */ kamu_flow_system as fs, kamu_task_system as ts};

use crate::FLOW_TYPE_WEBHOOK_DELIVER;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn fs::FlowDispatcher)]
#[dill::meta(fs::FlowDispatcherMeta {
    flow_type: FLOW_TYPE_WEBHOOK_DELIVER,
})]
pub struct FlowDispatcherWebhookDeliver {}

#[async_trait::async_trait]
impl fs::FlowDispatcher for FlowDispatcherWebhookDeliver {
    async fn build_task_logical_plan(
        &self,
        _flow_binding: &fs::FlowBinding,
        _maybe_config_snapshot: Option<&fs::FlowConfigurationRule>,
    ) -> Result<ts::LogicalPlan, InternalError> {
        unimplemented!()
        /*Ok(atw::LogicalPlanWebhookDeliver {
            webhook_subscription_id: subscription_id.into_inner(),
            webhook_event_id: event_id.into_inner(),
        }
        .into_logical_plan())*/
    }

    async fn propagate_success(
        &self,
        _flow_binding: &fs::FlowBinding,
        _trigger_instance: fs::FlowTriggerInstance,
        _: Option<fs::FlowConfigurationRule>,
    ) -> Result<(), InternalError> {
        // No further actions triggered with a webhook delivery
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
