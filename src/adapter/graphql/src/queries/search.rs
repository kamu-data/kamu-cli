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
use kamu_search::SearchLocalNatLangError;
use odf::dataset::TryStreamExtExt as _;

use crate::prelude::*;
use crate::queries::{Account, Dataset};
use crate::utils::from_catalog_n;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Search
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct Search;

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[Object]
impl Search {
    const DEFAULT_RESULTS_PER_PAGE: usize = 15;

    /// This endpoint uses heuristics to infer whether the query string is a DSL
    /// or a natural language query and is suitable to present the most
    /// versatile interface to the user consisting of just one input field.
    #[tracing::instrument(level = "info", name = Search_query, skip_all, fields(?page, ?per_page))]
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
            dyn kamu_core::DatasetRegistry,
            dyn kamu_core::auth::DatasetActionAuthorizer
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
        let mut readable_dataset_handles = readable_dataset_handles_stream
            .filter_ok(|hdl| hdl.alias.dataset_name.contains(&query))
            .try_collect::<Vec<_>>()
            .await?;
        // Sort first by dataset name and for stability after by account name
        readable_dataset_handles.sort_by(|a, b| {
            let dataset_name_cmp = a.alias.dataset_name.cmp(&b.alias.dataset_name);
            if dataset_name_cmp != std::cmp::Ordering::Equal {
                return dataset_name_cmp;
            }
            a.alias.account_name.cmp(&b.alias.account_name)
        });

        let total_count = readable_dataset_handles.len();

        let mut nodes: Vec<SearchResult> = Vec::new();
        for hdl in readable_dataset_handles
            .into_iter()
            .skip(page * per_page)
            .take(per_page)
        {
            let maybe_account = Account::from_dataset_alias(ctx, &hdl.alias).await?;
            if let Some(account) = maybe_account {
                nodes.push(SearchResult::Dataset(Dataset::new_access_checked(
                    account, hdl,
                )));
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

    /// Searches for datasets and other objects managed by the
    /// current node using a prompt in natural language
    #[tracing::instrument(level = "info", name = Search_query, skip_all, fields(?per_page))]
    async fn query_natural_language(
        &self,
        ctx: &Context<'_>,
        prompt: String,
        // page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<SearchResultExConnection> {
        let search_service = from_catalog_n!(ctx, dyn kamu_search::SearchServiceLocal);

        // TODO: Support "next page token" style pagination
        let page = 0;
        let per_page = per_page.unwrap_or(Self::DEFAULT_RESULTS_PER_PAGE);

        let limit = per_page;

        let res = search_service
            .search_natural_language(&prompt, kamu_search::SearchNatLangOpts { limit })
            .await
            .map_err(|e| match e {
                SearchLocalNatLangError::NotEnabled(e) => GqlError::Gql(e.into()),
                SearchLocalNatLangError::Internal(e) => GqlError::Internal(e),
            })?;

        let total_count = res.datasets.len();

        let mut nodes: Vec<SearchResultEx> = Vec::new();
        for hit in res.datasets {
            let Some(account) = Account::from_dataset_alias(ctx, &hit.handle.alias).await? else {
                tracing::warn!(
                    "Skipped dataset '{}' with unresolved account",
                    hit.handle.alias
                );
                continue;
            };

            nodes.push(SearchResultEx {
                item: SearchResult::Dataset(Dataset::new_access_checked(account, hit.handle)),
                score: hit.score,
            });
        }

        Ok(SearchResultExConnection::new(
            nodes,
            page,
            per_page,
            total_count,
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union, Debug)]
pub enum SearchResult {
    Dataset(Dataset),
    // Account,
    // Organization,
    // Issue,
}

page_based_connection!(SearchResult, SearchResultConnection, SearchResultEdge);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug)]
pub struct SearchResultEx {
    pub item: SearchResult,
    pub score: f32,
}

page_based_connection!(SearchResultEx, SearchResultExConnection, SearchResultExEdge);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
