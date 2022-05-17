// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::queries::*;
use crate::scalars::*;
use crate::utils::*;

use async_graphql::*;
use futures::TryStreamExt;
use kamu::domain;
use kamu::domain::TryStreamExtExt;

///////////////////////////////////////////////////////////////////////////////
// Search
///////////////////////////////////////////////////////////////////////////////

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
        let local_repo = from_catalog::<dyn domain::LocalDatasetRepository>(ctx).unwrap();

        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_RESULTS_PER_PAGE);

        let mut datasets: Vec<_> = local_repo
            .get_all_datasets()
            .filter_ok(|hdl| hdl.name.contains(&query))
            .try_collect()
            .await?;

        datasets.sort_by(|a, b| a.name.cmp(&b.name));
        let total_count = datasets.len();

        let nodes: Vec<_> = datasets
            .into_iter()
            .skip(page * per_page)
            .take(per_page)
            .map(|hdl| SearchResult::Dataset(Dataset::new(Account::mock(), hdl)))
            .collect();

        Ok(SearchResultConnection::new(
            nodes,
            page,
            per_page,
            Some(total_count),
        ))
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Union, Debug, Clone)]
pub enum SearchResult {
    Dataset(Dataset),
    // Account,
    // Organization,
    // Issue,
}

///////////////////////////////////////////////////////////////////////////////

page_based_connection!(SearchResult, SearchResultConnection, SearchResultEdge);
