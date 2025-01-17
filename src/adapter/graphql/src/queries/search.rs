// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::utils::datasets_filtering::filter_dataset_handle_stream;
use kamu_core::auth::{DatasetAction, DatasetActionAuthorizerExt};
use kamu_core::{self as domain, TryStreamExtExt};

use crate::prelude::*;
use crate::queries::{Account, Dataset};
use crate::utils::from_catalog_n;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Search
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct Search;

#[Object]
impl Search {
    const DEFAULT_RESULTS_PER_PAGE: usize = 15;

    /// Perform search across all resources
    async fn query(
        &self,
        ctx: &Context<'_>,
        query: String,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<SearchResultConnection> {
        use futures::TryStreamExt;

        let (dataset_registry, dataset_action_authorizer) = from_catalog_n!(
            ctx,
            dyn domain::DatasetRegistry,
            dyn domain::auth::DatasetActionAuthorizer
        );

        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_RESULTS_PER_PAGE);

        // TODO: Private Datasets: PERF: find a way to narrow down the number of records
        //       to filter, e.g.:
        //       - Anonymous: get all the public
        //       - Logged: all owned datasets and datasets with relations
        let filtered_all_dataset_handles_stream =
            filter_dataset_handle_stream(dataset_registry.all_dataset_handles(), |hdl| {
                hdl.alias.dataset_name.contains(&query)
            });
        let readable_dataset_handles_stream = dataset_action_authorizer
            .filtered_datasets_stream(filtered_all_dataset_handles_stream, DatasetAction::Read);
        let readable_dataset_handles = readable_dataset_handles_stream
            .filter_ok(|hdl| hdl.alias.dataset_name.contains(&query))
            .try_collect::<Vec<_>>()
            .await?;

        let total_count = readable_dataset_handles.len();

        let mut nodes: Vec<SearchResult> = Vec::new();
        for hdl in readable_dataset_handles
            .into_iter()
            .skip(page * per_page)
            .take(per_page)
        {
            let maybe_account = Account::from_dataset_alias(ctx, &hdl.alias).await?;
            if let Some(account) = maybe_account {
                nodes.push(SearchResult::Dataset(Dataset::new(account, hdl)));
            } else {
                tracing::warn!("Skipped dataset '{}' with unresolved account", hdl.alias);
            }
        }

        Ok(SearchResultConnection::new(
            nodes,
            page,
            per_page,
            total_count,
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union, Debug, Clone)]
pub enum SearchResult {
    Dataset(Dataset),
    // Account,
    // Organization,
    // Issue,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

page_based_connection!(SearchResult, SearchResultConnection, SearchResultEdge);
