// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::PaginationOpts;
use futures::TryStreamExt;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_adapter_flow_dataset::{DATASET_RESOURCE_TYPE, DatasetResourceUpdateDetails};
use kamu_flow_system as fs;
use kamu_webhooks::{WebhookSubscriptionQueryMode, WebhookSubscriptionQueryService};

use crate::{FLOW_TYPE_WEBHOOK_DELIVER, FlowScopeSubscription};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const FLOWS_PAGE_SIZE: usize = 20;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Finds the head of a dataset that the receiver of a webhook subscription
/// already knows about
#[dill::component]
pub struct WebhookDeliveryBaseResolver {
    flow_query_service: Arc<dyn fs::FlowQueryService>,
    webhook_subscription_query_service: Arc<dyn WebhookSubscriptionQueryService>,
}

impl WebhookDeliveryBaseResolver {
    /// The head sent in the last successful delivery, or else the head when the
    /// subscription was created. `None` when neither is known
    #[tracing::instrument(level = "debug", skip_all, fields(?webhook_flow_scope))]
    pub async fn resolve(
        &self,
        webhook_flow_scope: &fs::FlowScope,
    ) -> Result<Option<odf::Multihash>, InternalError> {
        if let Some(delivered_head) = self.find_last_delivered_head(webhook_flow_scope).await? {
            return Ok(Some(delivered_head));
        }

        let subscription_id = FlowScopeSubscription::new(webhook_flow_scope).subscription_id();
        let maybe_subscription = self
            .webhook_subscription_query_service
            .find_webhook_subscription(
                subscription_id,
                WebhookSubscriptionQueryMode::IncludingRemoved,
            )
            .await?;

        Ok(maybe_subscription.and_then(|s| s.dataset_head_at_creation().cloned()))
    }

    /// New head of the last successful delivery flow of the binding
    async fn find_last_delivered_head(
        &self,
        webhook_flow_scope: &fs::FlowScope,
    ) -> Result<Option<odf::Multihash>, InternalError> {
        let subscription_id = FlowScopeSubscription::new(webhook_flow_scope).subscription_id();
        let filters = fs::FlowFilters {
            by_flow_types: Some(vec![FLOW_TYPE_WEBHOOK_DELIVER.to_string()]),
            by_flow_statuses: Some(vec![fs::FlowStatus::Finished]),
            by_initiator: None,
        };

        // Newest first: only failed and aborted flows precede the last success, so it
        // is usually on the first page
        let mut page = 0;
        loop {
            let flows: Vec<_> = self
                .flow_query_service
                .list_scoped_flows(
                    FlowScopeSubscription::query_for_multiple_subscriptions(&[subscription_id]),
                    filters.clone(),
                    fs::FlowOrder::creation_time_desc(),
                    PaginationOpts::from_page(page, FLOWS_PAGE_SIZE),
                )
                .await?
                .matched_stream
                .try_collect()
                .await?;
            if flows.is_empty() {
                return Ok(None);
            }

            for flow in &flows {
                if flow.flow_binding.scope == *webhook_flow_scope
                    && let Some(fs::FlowOutcome::Success(_)) = &flow.outcome
                    && let Some(new_head) = Self::delivered_head(flow)?
                {
                    return Ok(Some(new_head));
                }
            }

            page += 1;
        }
    }

    /// New head of the last dataset update a delivery flow was activated by
    fn delivered_head(flow: &fs::FlowState) -> Result<Option<odf::Multihash>, InternalError> {
        let mut new_head = None;
        for activation_cause in &flow.activation_causes {
            if let fs::FlowActivationCause::ResourceUpdate(update) = activation_cause
                && update.resource_type == DATASET_RESOURCE_TYPE
            {
                let details =
                    serde_json::from_value::<DatasetResourceUpdateDetails>(update.details.clone())
                        .int_err()?;
                new_head = Some(details.new_head);
            }
        }
        Ok(new_head)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
