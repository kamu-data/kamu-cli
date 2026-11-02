// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::collections::HashSet;

use kamu_adapter_flow_dataset::FlowScopeDataset;
use kamu_datasets::{DatasetEntryService, DatasetEntryServiceExt, DatasetRegistry};
use kamu_flow_system as fs;

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn list_accessible_datasets_with_flows(
    ctx: &Context<'_>,
    account_id: &odf::AccountID,
    page: usize,
    per_page: usize,
) -> Result<(Vec<odf::DatasetHandle>, usize)> {
    let (flow_query_service, dataset_entry_service, dataset_registry) = from_catalog_n!(
        ctx,
        dyn fs::FlowQueryService,
        dyn DatasetEntryService,
        dyn DatasetRegistry
    );

    // Consider using ReBAC: not just owned, but perhaps "Maintain" too,
    // which allows launching flows
    let owned_dataset_ids = dataset_entry_service
        .get_owned_dataset_ids(account_id)
        .await
        .int_err()?;

    // Match flows by dataset across all scope types, as `listFlows` does,
    // so that datasets having only webhook delivery flows are included too
    let owned_dataset_id_refs = owned_dataset_ids.iter().collect::<Vec<_>>();
    let flow_scopes = flow_query_service
        .list_flow_scopes_matching_scope_query(FlowScopeDataset::query_for_multiple_datasets(
            &owned_dataset_id_refs,
        ))
        .await?;

    let dataset_ids_with_flows = flow_scopes
        .iter()
        .filter_map(FlowScopeDataset::maybe_dataset_id_in_scope)
        .collect::<HashSet<_>>();

    let filtered_dataset_ids = owned_dataset_ids
        .into_iter()
        .filter(|dataset_id| dataset_ids_with_flows.contains(dataset_id))
        .collect::<Vec<_>>();

    let total_count = filtered_dataset_ids.len();

    let dataset_ids = filtered_dataset_ids
        .into_iter()
        .skip(page * per_page)
        .take(per_page)
        .map(Cow::Owned)
        .collect::<Vec<_>>();

    let dataset_handles_resolution = dataset_registry
        .resolve_multiple_dataset_handles_by_ids(&dataset_ids)
        .await
        .int_err()?;

    for (dataset_id, _) in dataset_handles_resolution.unresolved_datasets {
        tracing::warn!(
            %dataset_id,
            "Ignoring point that refers to a dataset not present in the registry",
        );
    }

    Ok((dataset_handles_resolution.resolved_handles, total_count))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
