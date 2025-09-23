// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use kamu_adapter_flow_dataset::FlowScopeDataset;
use kamu_adapter_flow_webhook::{FLOW_TYPE_WEBHOOK_DELIVER, FlowScopeSubscription};
use kamu_flow_system as fs;

use crate::prelude::*;
use crate::queries::{FlowProcessTypeFilterInput, InitiatorFilterInput};

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
                    // TODO: empty list means "all_types"
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
) -> fs::FlowScopeQuery {
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
                FlowScopeSubscription::query_for_multiple_subscriptions(&subscription_ids)
            } else {
                // No particular channels listed, use all subscriptions
                // that belong to the datasets
                FlowScopeSubscription::query_for_subscriptions_of_multiple_datasets(dataset_id_refs)
            }
        }
        Some(FlowProcessTypeFilterInput::Primary(_)) => {
            // Primary flows only, use specific "type" field
            FlowScopeDataset::query_for_multiple_datasets_only(dataset_id_refs)
        }
        None => FlowScopeDataset::query_for_multiple_datasets(dataset_id_refs), /* we'll use default query */
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
