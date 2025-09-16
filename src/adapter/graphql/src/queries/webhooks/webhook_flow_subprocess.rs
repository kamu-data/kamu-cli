// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use {kamu_flow_system as fs, kamu_webhooks as wh};

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct WebhookFlowSubProcess {
    id: wh::WebhookSubscriptionID,
    name: String,
    flow_process_state: fs::FlowProcessState,
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl WebhookFlowSubProcess {
    #[graphql(skip)]
    #[allow(dead_code)]
    pub fn new(
        id: wh::WebhookSubscriptionID,
        name: String,
        flow_process_state: fs::FlowProcessState,
    ) -> Self {
        Self {
            id,
            name,
            flow_process_state,
        }
    }

    #[allow(clippy::unused_async)]
    pub async fn id(&self) -> WebhookSubscriptionID {
        self.id.into()
    }

    #[allow(clippy::unused_async)]
    pub async fn name(&self) -> String {
        self.name.clone()
    }

    #[allow(clippy::unused_async)]
    async fn summary(&self) -> Result<FlowProcessSummary> {
        Ok(self.flow_process_state.clone().into())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
