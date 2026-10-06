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

        // A missing flow is already logged by the query service
        let linked_flow_states = links
            .into_iter()
            .filter_map(|link| {
                let flow_state = flow_states_by_id.remove(&link.downstream_flow_id)?;
                Some((link, flow_state))
            })
            .collect::<Vec<_>>();

        // Resolved together, so that the dataset loader batches the lookups
        let accesses = futures::future::try_join_all(
            linked_flow_states
                .iter()
                .map(|(_, flow_state)| Self::resolve_access(ctx, flow_state)),
        )
        .await?;

        let mut partial_links = Vec::with_capacity(linked_flow_states.len());
        let mut visible_flow_states = Vec::new();

        for ((link, flow_state), access) in linked_flow_states.into_iter().zip(accesses) {
            if access.is_flow_visible {
                visible_flow_states.push(flow_state);
            }
            partial_links.push((link, access));
        }

        let mut visible_flows = Flow::build_batch(visible_flow_states, ctx)
            .await?
            .into_iter();

        Ok(partial_links
            .into_iter()
            .map(|(link, access)| Self {
                flow_id: link.downstream_flow_id.into(),
                activated_at: link.activated_at,
                dataset_id: access.dataset_id.map(Into::into),
                dataset: access.dataset,
                flow: if access.is_flow_visible {
                    visible_flows.next()
                } else {
                    None
                },
            })
            .collect())
    }

    async fn resolve_access(
        ctx: &Context<'_>,
        flow_state: &fs::FlowState,
    ) -> Result<DownstreamFlowAccess> {
        let Some(dataset_id) =
            afs::FlowScopeDataset::maybe_dataset_id_in_scope(&flow_state.flow_binding.scope)
        else {
            return Ok(DownstreamFlowAccess {
                dataset_id: None,
                dataset: None,
                is_flow_visible: false,
            });
        };

        let (dataset, is_flow_visible) =
            match Dataset::try_from_ref(ctx, &dataset_id.as_local_ref()).await? {
                Some(dataset) => {
                    let is_flow_visible = Self::is_flow_visible(ctx, &dataset, flow_state).await?;
                    (DatasetAccessResult::accessible(dataset), is_flow_visible)
                }
                None => (
                    DatasetAccessResult::not_accessible(dataset_id.clone()),
                    false,
                ),
            };

        Ok(DownstreamFlowAccess {
            dataset_id: Some(dataset_id),
            dataset: Some(dataset),
            is_flow_visible,
        })
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

/// The downstream flow's dataset as the caller may see it, and whether the
/// caller may see the flow itself
struct DownstreamFlowAccess {
    dataset_id: Option<odf::DatasetID>,
    dataset: Option<DatasetAccessResult<'static>>,
    is_flow_visible: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
