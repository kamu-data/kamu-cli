// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;
use kamu::utils::datasets_filtering::filter_dataset_handle_stream;
use kamu_accounts::{AccountService, SearchAccountsByNamePatternFilters};
use kamu_core::auth::{DatasetAction, DatasetActionAuthorizerExt};

use crate::prelude::*;
use crate::queries::{Account, Dataset};
use crate::utils::from_catalog_n;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Search
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct Search;

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
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
        mut query: String,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<SearchResultConnection> {
        use futures::TryStreamExt;

        let (dataset_registry, dataset_action_authorizer) = from_catalog_n!(
            ctx,
            dyn kamu_datasets::DatasetRegistry,
            dyn kamu_core::auth::DatasetActionAuthorizer
        );

        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_RESULTS_PER_PAGE);

        // TODO: Private Datasets:
        // TODO: PERF: find a way to narrow down the number of records
        //       to filter, e.g.:
        //       - Anonymous: get all the public
        //       - Logged: all owned datasets and datasets with relations

        query.make_ascii_lowercase();
        let filtered_all_dataset_handles_stream =
            filter_dataset_handle_stream(dataset_registry.all_dataset_handles(), |hdl| {
                let mut lowercase_alias = hdl.alias.to_string();
                lowercase_alias.make_ascii_lowercase();

                if lowercase_alias.contains(&query) {
                    return true;
                }

                let mut id = hdl.id.as_did_str().to_stack_string();
                id.make_ascii_lowercase();

                id.contains(&query)
            });
        let mut readable_dataset_handles = dataset_action_authorizer
            .filtered_datasets_stream(filtered_all_dataset_handles_stream, DatasetAction::Read)
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

    // TODO: restrict to admin only for now, until it's fully ready for public use
    // (final API, ReBAC)
    #[tracing::instrument(level = "info", name = Search_query_full_text, skip_all)]
    #[graphql(guard = "AdminGuard::new()")]
    async fn query_full_text(
        &self,
        ctx: &Context<'_>,
        prompt: String,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<FullTextSearchResponse> {
        let search_service = from_catalog_n!(ctx, dyn kamu_search::SearchService);

        use kamu_accounts::account_search_schema as account_schema;
        use kamu_datasets::dataset_search_schema as dataset_schema;

        // TODO: max limit is 10,000 in ES, otherwise we need cursors
        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_RESULTS_PER_PAGE);

        let catalog = ctx.data::<dill::Catalog>().unwrap();
        let context = kamu_search::SearchContext { catalog };

        // Run actual search request
        let search_results = {
            use kamu_search::*;

            search_service
                .search(
                    context,
                    SearchRequest {
                        query: if prompt.is_empty() {
                            None
                        } else {
                            Some(prompt)
                        },
                        source: SearchRequestSourceSpec::All,
                        entity_schemas: vec![
                            account_schema::SCHEMA_NAME,
                            dataset_schema::SCHEMA_NAME,
                        ],
                        filter: None,
                        // sort: sort!(FULL_TEXT_SEARCH_ALIAS_TITLE),
                        sort: vec![],
                        page: SearchPaginationSpec {
                            limit: per_page,
                            offset: page * per_page,
                        },
                        options: SearchOptions {
                            enable_explain: false,
                            enable_highlighting: true,
                        },
                    },
                )
                .await
        }?;

        // Convert into GQL response
        Ok(FullTextSearchResponse {
            took_ms: search_results.took_ms,
            timeout: search_results.timeout,
            total_hits: search_results.total_hits,
            hits: search_results
                .hits
                .into_iter()
                .map(|hit| FullTextSearchHit {
                    id: hit.id,
                    schema_name: hit.schema_name.to_string(),
                    score: hit.score,
                    source: hit.source,
                    highlights: hit.highlights.map(|highlights| {
                        highlights
                            .into_iter()
                            .map(|h| FullTextSearchHighlight {
                                field: h.field,
                                best_fragment: h.best_fragment,
                            })
                            .collect()
                    }),
                })
                .collect(),
        })
    }

    /// Searches for datasets and other objects managed by the
    /// current node using a prompt in natural language
    #[tracing::instrument(level = "info", name = Search_query_natural_language, skip_all, fields(?per_page))]
    async fn query_natural_language(
        &self,
        ctx: &Context<'_>,
        prompt: String,
        // page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<SearchResultExConnection> {
        let datasets_search_service = from_catalog_n!(ctx, dyn kamu_datasets::DatasetSearchService);

        // TODO: max limit is 10,000 in ES, otherwise we need cursors
        let per_page = per_page.unwrap_or(Self::DEFAULT_RESULTS_PER_PAGE);

        let catalog = ctx.data::<dill::Catalog>().unwrap();
        let context = kamu_search::SearchContext { catalog };

        // Run vector search request
        let search_results = {
            use kamu_search::*;

            datasets_search_service
                .vector_search(
                    context,
                    prompt,
                    SearchPaginationSpec {
                        limit: per_page,
                        offset: 0, /* page * per_page, */
                    },
                )
                .await
                .int_err()
        }?;

        let total_count = search_results.total_hits;

        // Build final result with GQL dataset objects
        let mut nodes: Vec<SearchResultEx> = Vec::new();
        for hit in search_results.hits {
            let hdl = hit.handle;

            let Some(owner) = Account::from_dataset_alias(ctx, &hdl.alias).await? else {
                tracing::warn!("Skipped dataset '{}' with unresolved account", hdl.alias);
                continue;
            };

            // Note: we assume search will encapsulate ReBAC filtering in nearest future
            nodes.push(SearchResultEx {
                item: SearchResult::Dataset(Dataset::new_access_checked(owner, hdl)),
                score: hit.score,
            });
        }

        Ok(SearchResultExConnection::new(
            nodes,
            0, /* page */
            per_page,
            usize::try_from(total_count).unwrap(),
        ))
    }

    /// Perform lightweight search among resource names.
    /// Useful for autocomplete.
    #[tracing::instrument(level = "info", name = Search_name_lookup, skip_all, fields(%query, ?page, ?per_page))]
    async fn name_lookup(
        &self,
        ctx: &Context<'_>,
        query: String,
        filters: LookupFilters,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<NameLookupResultConnection> {
        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_RESULTS_PER_PAGE);

        let account_nodes = if let Some(filters) = filters.by_account {
            use futures::TryStreamExt;

            let account_service = from_catalog_n!(ctx, dyn AccountService);

            let exclude_accounts_by_ids = filters
                .exclude_accounts_by_ids
                .into_iter()
                .map(Into::into)
                .collect();
            let accounts = account_service
                .search_accounts_by_name_pattern(
                    &query,
                    SearchAccountsByNamePatternFilters {
                        exclude_accounts_by_ids,
                    },
                    PaginationOpts::from_page(page, per_page),
                )
                .try_collect::<Vec<_>>()
                .await?;

            accounts
                .into_iter()
                .map(Account::from_account)
                .map(NameLookupResult::Account)
                .collect()
        } else {
            Vec::new()
        };

        let nodes = account_nodes;
        let total = nodes.len();
        let page_nodes = nodes
            .into_iter()
            .skip(page * per_page)
            .take(per_page)
            .collect::<Vec<_>>();

        Ok(NameLookupResultConnection::new(
            page_nodes, page, per_page, total,
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug)]
pub struct FullTextSearchResponse {
    pub took_ms: u64,
    pub timeout: bool,
    pub total_hits: u64,
    pub hits: Vec<FullTextSearchHit>,
}

#[derive(SimpleObject, Debug)]
pub struct FullTextSearchHit {
    pub id: String,
    pub schema_name: String,
    pub score: Option<f64>,
    pub source: serde_json::Value,
    pub highlights: Option<Vec<FullTextSearchHighlight>>,
}

#[derive(SimpleObject, Debug)]
pub struct FullTextSearchHighlight {
    pub field: String,
    pub best_fragment: String,
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
    pub score: f64,
}

page_based_connection!(SearchResultEx, SearchResultExConnection, SearchResultExEdge);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union, Debug)]
pub enum NameLookupResult {
    Account(Account),
    // Dataset
    // Organization,
    // Issue,
}

#[derive(InputObject, Debug)]
pub struct LookupFilters {
    by_account: Option<AccountLookupFilter>,
}

#[derive(InputObject, Debug)]
pub struct AccountLookupFilter {
    exclude_accounts_by_ids: Vec<AccountID<'static>>,
}

page_based_connection!(
    NameLookupResult,
    NameLookupResultConnection,
    NameLookupResultEdge
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
