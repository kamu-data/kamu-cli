// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use kamu_adapter_flow_webhook::{FLOW_TYPE_WEBHOOK_DELIVER, FlowScopeSubscription};
use kamu_flow_system as fs;

use crate::prelude::*;
use crate::queries::{FlowProcessTypeFilterInput, InitiatorFilterInput};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn flow_process_runtime_state(
    ctx: &Context<'_>,
    flow_trigger: &fs::FlowTriggerState,
) -> Result<FlowProcessRuntimeState> {
    let flow_event_store = from_catalog_n!(ctx, dyn fs::FlowEventStore);

    let flow_binding = &flow_trigger.flow_binding;

    let consecutive_failures = flow_event_store
        .get_current_consecutive_flow_failures_count(flow_binding)
        .await?;

    let effective_state = match flow_trigger.status {
        fs::FlowTriggerStatus::Active => {
            if consecutive_failures == 0 {
                FlowProcessRuntimeStateEnum::Active
            } else {
                FlowProcessRuntimeStateEnum::Failing
            }
        }
        fs::FlowTriggerStatus::PausedByUser => FlowProcessRuntimeStateEnum::PausedManual,
        fs::FlowTriggerStatus::StoppedAutomatically => FlowProcessRuntimeStateEnum::StoppedAuto,
        fs::FlowTriggerStatus::ScopeRemoved => {
            unreachable!("ScopeRemoved triggers are not expected here")
        }
    };

    let run_stats = flow_event_store.get_flow_run_stats(flow_binding).await?;

    let maybe_pending_flow_id = flow_event_store.try_get_pending_flow(flow_binding).await?;
    let next_planned_at = if let Some(pending_flow_id) = maybe_pending_flow_id {
        let pending_flow = fs::Flow::load(pending_flow_id, flow_event_store.as_ref())
            .await
            .int_err()?;
        pending_flow.timing.scheduled_for_activation_at
    } else {
        None
    };

    Ok(FlowProcessRuntimeState {
        effective_state,
        consecutive_failures,
        last_success_at: run_stats.last_success_time,
        last_attempt_at: run_stats.last_attempt_time,
        last_failure_at: run_stats.last_failure_time,
        next_planned_at,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn prepare_flows_filter_by_initiator(
    maybe_input: Option<InitiatorFilterInput>,
) -> Option<fs::InitiatorFilter> {
    match maybe_input {
        Some(initiator_filter) => match initiator_filter {
            InitiatorFilterInput::System(_) => Some(kamu_flow_system::InitiatorFilter::System),
            InitiatorFilterInput::Accounts(account_ids) => {
                Some(kamu_flow_system::InitiatorFilter::Account(
                    account_ids
                        .into_iter()
                        .map(Into::into)
                        .collect::<HashSet<_>>(),
                ))
            }
        },
        None => None,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn prepare_flows_filter_by_types(
    maybe_input: Option<&FlowProcessTypeFilterInput>,
) -> Option<Vec<String>> {
    match maybe_input {
        Some(FlowProcessTypeFilterInput::Primary(primary_filter)) => Some(
            primary_filter
                .by_flow_types
                .as_ref()
                .map(|flow_types| {
                    flow_types
                        .iter()
                        .map(|flow_type| encode_dataset_flow_type(*flow_type).to_string())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_else(unpack_all_dataset_flow_types),
        ),

        Some(FlowProcessTypeFilterInput::Webhooks(_)) => {
            Some(vec![FLOW_TYPE_WEBHOOK_DELIVER.to_string()])
        }

        None => None,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn prepare_flows_scope_query(
    maybe_input: Option<&FlowProcessTypeFilterInput>,
    dataset_id_refs: &[&odf::DatasetID],
) -> Option<fs::FlowScopeQuery> {
    match maybe_input {
        Some(FlowProcessTypeFilterInput::Webhooks(webhooks_filter)) => {
            // Particular subscriptions listed?
            if let Some(subscription_ids) = &webhooks_filter.subscription_ids
                && !subscription_ids.is_empty()
            {
                // Convert to domain-level webhook subscription IDs
                let subscription_ids = subscription_ids
                    .iter()
                    .copied()
                    .map(Into::into)
                    .collect::<Vec<_>>();

                // We want a flow scope query for these subscriptions
                // Note: we don't have to use "dataset_id_refs",
                // it would be a redundant condition at the database level
                Some(FlowScopeSubscription::query_for_multiple_subscriptions(
                    &subscription_ids,
                ))
            } else {
                // No particular channels listed, use all subscriptions
                // that belong to the datasets
                Some(
                    FlowScopeSubscription::query_for_subscriptions_of_multiple_datasets(
                        dataset_id_refs,
                    ),
                )
            }
        }
        Some(FlowProcessTypeFilterInput::Primary(_)) | None => None, // we'll use default query
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
