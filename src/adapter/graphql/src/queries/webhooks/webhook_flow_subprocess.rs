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
use crate::queries::{Dataset, DatasetRequestStateWithOwner};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct WebhookFlowSubProcess {
    id: wh::WebhookSubscriptionID,
    name: String,
    parent_dataset_request_state: Option<DatasetRequestStateWithOwner>,
    flow_process_state: fs::FlowProcessState,
}

#[Object]
impl WebhookFlowSubProcess {
    #[graphql(skip)]
    #[allow(dead_code)]
    pub fn new(
        subscription: &wh::WebhookSubscription,
        parent_dataset_request_state: Option<DatasetRequestStateWithOwner>,
        flow_process_state: fs::FlowProcessState,
    ) -> Self {
        // Decide on subprocess name
        let subprocess_name = if subscription.label().as_ref().is_empty() {
            subscription.target_url().to_string()
        } else {
            subscription.label().as_ref().to_string()
        };

        Self {
            id: subscription.id(),
            name: subprocess_name,
            parent_dataset_request_state,
            flow_process_state,
        }
    }

    pub async fn id(&self) -> WebhookSubscriptionID {
        self.id.into()
    }

    pub async fn name(&self) -> String {
        self.name.clone()
    }

    pub async fn parent_dataset(&self) -> Option<Dataset> {
        self.parent_dataset_request_state.as_ref().map(|state| {
            Dataset::new_access_checked(state.owner().clone(), state.dataset_handle().clone())
        })
    }

    #[allow(clippy::unused_async)]
    pub async fn summary(&self) -> Result<FlowProcessSummary> {
        Ok(self.flow_process_state.clone().into())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

page_based_connection!(
    WebhookFlowSubProcess,
    WebhookFlowSubProcessConnection,
    WebhookFlowSubProcessEdge
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
