// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use kamu_adapter_flow_dataset as afs;
use kamu_adapter_flow_dataset::FLOW_SCOPE_TYPE_DATASET;
use kamu_adapter_flow_webhook::FLOW_SCOPE_TYPE_WEBHOOK_SUBSCRIPTION;
use kamu_datasets::DatasetAction;
use kamu_flow_system as fs;

use super::Flow;
use crate::prelude::*;
use crate::queries::{Dataset, DatasetAccessResult};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A flow that was activated by the completion of another flow
#[derive(SimpleObject)]
pub struct FlowDownstreamLink {
    /// Downstream flow ID
    flow_id: FlowID,
    /// When the downstream flow received the activation
    activated_at: DateTime<Utc>,
    /// Dataset the downstream flow belongs to, if any
    dataset_id: Option<DatasetID<'static>>,
    /// Null only when `datasetId` is null
    dataset: Option<DatasetAccessResult<'static>>,
    /// Null unless the caller may see the downstream flow: Read on its
    /// dataset, and Maintain for webhook deliveries
    flow: Option<Flow>,
}

impl FlowDownstreamLink {
    pub async fn build_list(ctx: &Context<'_>, upstream_flow_id: fs::FlowID) -> Result<Vec<Self>> {
        let flow_query_service = from_catalog_n!(ctx, dyn fs::FlowQueryService);

        let links = flow_query_service
            .get_downstream_links(upstream_flow_id)
            .await?;

        let downstream_flow_ids = links
            .iter()
            .map(|link| link.downstream_flow_id)
            .collect::<Vec<_>>();
        let mut flow_states_by_id = flow_query_service
            .get_flows(&downstream_flow_ids)
            .await?
            .into_iter()
            .map(|flow_state| (flow_state.flow_id, flow_state))
            .collect::<HashMap<_, _>>();

        let mut partial_links = Vec::with_capacity(links.len());
        let mut visible_flow_states = Vec::new();

        for link in links {
            // A missing flow is already logged by the query service
            let Some(flow_state) = flow_states_by_id.remove(&link.downstream_flow_id) else {
                continue;
            };

            let dataset_id =
                afs::FlowScopeDataset::maybe_dataset_id_in_scope(&flow_state.flow_binding.scope);

            let (dataset, is_flow_visible) = match &dataset_id {
                Some(dataset_id) => {
                    match Dataset::try_from_ref(ctx, &dataset_id.as_local_ref()).await? {
                        Some(dataset) => {
                            let is_flow_visible =
                                Self::is_flow_visible(ctx, &dataset, &flow_state).await?;
                            (
                                Some(DatasetAccessResult::accessible(dataset)),
                                is_flow_visible,
                            )
                        }
                        None => (
                            Some(DatasetAccessResult::not_accessible(dataset_id.clone())),
                            false,
                        ),
                    }
                }
                None => (None, false),
            };

            partial_links.push((link, dataset_id, dataset, is_flow_visible));
            if is_flow_visible {
                visible_flow_states.push(flow_state);
            }
        }

        let mut visible_flows = Flow::build_batch(visible_flow_states, ctx)
            .await?
            .into_iter();

        Ok(partial_links
            .into_iter()
            .map(|(link, dataset_id, dataset, is_flow_visible)| Self {
                flow_id: link.downstream_flow_id.into(),
                activated_at: link.activated_at,
                dataset_id: dataset_id.map(Into::into),
                dataset,
                flow: if is_flow_visible {
                    visible_flows.next()
                } else {
                    None
                },
            })
            .collect())
    }

    /// Webhook flows expose the delivery target, which only maintainers see
    async fn is_flow_visible(
        ctx: &Context<'_>,
        readable_dataset: &Dataset,
        flow_state: &fs::FlowState,
    ) -> Result<bool> {
        Ok(match flow_state.flow_binding.scope.scope_type() {
            FLOW_SCOPE_TYPE_DATASET => true,
            FLOW_SCOPE_TYPE_WEBHOOK_SUBSCRIPTION => readable_dataset
                .allowed_dataset_actions(ctx)
                .await?
                .contains(&DatasetAction::Maintain),
            _ => false,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
