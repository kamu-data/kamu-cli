// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use futures::TryStreamExt;
use kamu_core::{self as domain, TryStreamExtExt};

use crate::prelude::*;
use crate::queries::{Account, Dataset};

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
        let dataset_repo = from_catalog::<dyn domain::DatasetRepository>(ctx).unwrap();

        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_RESULTS_PER_PAGE);

        let mut datasets: Vec<_> = dataset_repo
            .get_all_datasets()
            .filter_ok(|hdl| hdl.alias.dataset_name.contains(&query))
            .try_collect()
            .await?;

        datasets.sort_by(|a, b| a.alias.cmp(&b.alias));
        let total_count = datasets.len();

        let nodes: Vec<_> = datasets
            .into_iter()
            .skip(page * per_page)
            .take(per_page)
            .map(|hdl| SearchResult::Dataset(Dataset::new(Account::fake(), hdl)))
            .collect();

        Ok(SearchResultConnection::new(
            nodes,
            page,
            per_page,
            total_count,
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
