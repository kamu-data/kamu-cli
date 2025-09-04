// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;

use kamu_adapter_flow_dataset::FlowScopeDataset;
use kamu_core::DatasetRegistry;
use kamu_datasets::{DatasetEntryService, DatasetEntryServiceExt};
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

    let input_flow_scopes = owned_dataset_ids
        .iter()
        .map(FlowScopeDataset::make_scope)
        .collect::<Vec<_>>();

    let filtered_flow_scopes = flow_query_service
        .filter_flow_scopes_having_flows(&input_flow_scopes)
        .await?;

    let total_count = filtered_flow_scopes.len();

    let dataset_ids = filtered_flow_scopes
        .into_iter()
        .skip(page * per_page)
        .take(per_page)
        .filter_map(|flow_scope| FlowScopeDataset::maybe_dataset_id_in_scope(&flow_scope))
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
