// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Mutex;

use async_graphql::*;
use kamu_accounts::CurrentAccountSubject;
use kamu_adapter_graphql::data_loader::account_entity_data_loader;
use kamu_core::*;
use kamu_datasets::*;
use kamu_search::{
    HybridSearchRequest,
    ListingSearchRequest,
    SearchContext,
    SearchEntityId,
    SearchEntitySchemaName,
    SearchError,
    SearchIndexUpdateOperation,
    SearchResponse,
    SearchSecurityContext,
    SearchService,
    TextSearchRequest,
    VectorSearchRequest,
};
use odf::metadata::testing::MetadataFactory;
use pretty_assertions::{assert_eq, assert_matches};

use crate::utils::{BaseGQLDatasetHarness, PredefinedAccountOpts, authentication_catalogs};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_single_dataset_search() {
    let harness = GqlSearchHarness::new().await;

    use odf::metadata::testing::alias;

    harness.create_datasets([alias(&"kamu", &"foo")]).await;

    assert_eq!(
        harness
            .search_authorized("bar", SearchOptions::default())
            .await
            .data,
        value!({
            "search": {
                "query": {
                    "nodes": [],
                    "totalCount": 0,
                    "pageInfo": {
                        "totalPages": 0,
                        "hasNextPage": false,
                        "hasPreviousPage": false,
                    }
                }
            }
        })
    );
    assert_eq!(
        harness
            .search_authorized("foo", SearchOptions::default())
            .await
            .data,
        value!({
            "search": {
                "query": {
                    "nodes": [{
                        "__typename": "Dataset",
                        "name": "foo",
                    }],
                    "totalCount": 1,
                    "pageInfo": {
                        "totalPages": 1,
                        "hasNextPage": false,
                        "hasPreviousPage": false,
                    }
                }
            }
        })
    );
    assert_eq!(
        harness
            .execute_authorized(indoc::indoc!(
                "
                {
                  search {
                    query(query: \"\") {
                      nodes {
                        ... on Dataset {
                          metadata {
                            chain {
                              blocks(page: 0, perPage: 0) {
                                totalCount
                                pageInfo {
                                  totalPages
                                  hasNextPage
                                  hasPreviousPage
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
                "
            ))
            .await
            .data,
        value!({
            "search": {
                "query": {
                    "nodes": [{
                        "metadata": {
                          "chain": {
                            "blocks": {
                              "totalCount": 2,
                              "pageInfo": {
                                "totalPages": 0,
                                "hasNextPage": false,
                                "hasPreviousPage": false
                              }
                            }
                          }
                        },
                    }],
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_case_insensitive_search() {
    let harness = GqlSearchHarness::new().await;

    use odf::metadata::testing::alias;

    harness.create_datasets([alias(&"kamu", &"dataset1")]).await;
    harness.create_datasets([alias(&"kAMu", &"dataset2")]).await;
    harness.create_datasets([alias(&"kAMu", &"dAtAsEt3")]).await;

    assert_eq!(
        value!({
            "search": {
                "query": {
                    "nodes": [
                        {
                            "__typename": "Dataset",
                            "name": "dataset1",
                        },
                        {
                            "__typename": "Dataset",
                            "name": "dataset2",
                        },
                        {
                            "__typename": "Dataset",
                            "name": "dAtAsEt3",
                        }
                    ],
                    "totalCount": 3,
                    "pageInfo": {
                        "totalPages": 1,
                        "hasNextPage": false,
                        "hasPreviousPage": false,
                    }
                }
            }
        }),
        harness
            .search_authorized("am", SearchOptions::default())
            .await
            .data,
    );
    assert_eq!(
        value!({
            "search": {
                "query": {
                    "nodes": [
                        {
                            "__typename": "Dataset",
                            "name": "dataset1",
                        },
                        {
                            "__typename": "Dataset",
                            "name": "dataset2",
                        },
                        {
                            "__typename": "Dataset",
                            "name": "dAtAsEt3",
                        }
                    ],
                    "totalCount": 3,
                    "pageInfo": {
                        "totalPages": 1,
                        "hasNextPage": false,
                        "hasPreviousPage": false,
                    }
                }
            }
        }),
        harness
            .search_authorized("tas", SearchOptions::default())
            .await
            .data,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_search_correct_dataset_order_in_response() {
    let harness = GqlSearchHarness::new().await;

    use odf::metadata::testing::alias;

    harness
        .create_datasets(
            // NOTE: purposely random order
            [
                alias(&"kamu", &"dataset5"),
                alias(&"kamu", &"dataset3"),
                alias(&"kamu", &"dataset4"),
                alias(&"kamu", &"dataset1"),
                alias(&"kamu", &"dataset2"),
            ],
        )
        .await;

    // Run queries multiple times to ensure the search results stability
    for _ in 0..5 {
        // Search without pagination
        assert_eq!(
            harness
                .search_authorized("dataset", SearchOptions::default())
                .await
                .data,
            value!({
                "search": {
                    "query": {
                        "nodes": [
                            {
                                "__typename": "Dataset",
                                "name": "dataset1",
                            },
                            {
                                "__typename": "Dataset",
                                "name": "dataset2",
                            },
                            {
                                "__typename": "Dataset",
                                "name": "dataset3",
                            },
                            {
                                "__typename": "Dataset",
                                "name": "dataset4",
                            },
                            {
                                "__typename": "Dataset",
                                "name": "dataset5",
                            },
                        ],
                        "totalCount": 5,
                        "pageInfo": {
                            "totalPages": 1,
                            "hasNextPage": false,
                            "hasPreviousPage": false,
                        }
                    }
                }
            })
        );

        // Checking with pagination
        assert_eq!(
            harness
                .search_authorized(
                    "dataset",
                    SearchOptions {
                        page: Some(0),
                        per_page: Some(3),
                    }
                )
                .await
                .data,
            value!({
                "search": {
                    "query": {
                        "nodes": [
                            {
                                "__typename": "Dataset",
                                "name": "dataset1",
                            },
                            {
                                "__typename": "Dataset",
                                "name": "dataset2",
                            },
                            {
                                "__typename": "Dataset",
                                "name": "dataset3",
                            },
                        ],
                        "totalCount": 5,
                        "pageInfo": {
                            "totalPages": 2,
                            "hasNextPage": true,
                            "hasPreviousPage": false,
                        }
                    }
                }
            })
        );
        assert_eq!(
            harness
                .search_authorized(
                    "dataset",
                    SearchOptions {
                        page: Some(1),
                        per_page: Some(3),
                    }
                )
                .await
                .data,
            value!({
                "search": {
                    "query": {
                        "nodes": [
                            {
                                "__typename": "Dataset",
                                "name": "dataset4",
                            },
                            {
                                "__typename": "Dataset",
                                "name": "dataset5",
                            },
                        ],
                        "totalCount": 5,
                        "pageInfo": {
                            "totalPages": 2,
                            "hasNextPage": false,
                            "hasPreviousPage": true,
                        }
                    }
                }
            })
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_name_lookup_accounts() {
    let harness = GqlSearchHarness::new().await;

    assert_eq!(
        harness
            .name_lookup_anonymous(
                "kA",
                value!({
                    "byAccount": {
                        "excludeAccountsByIds": [],
                    },
                })
            )
            .await
            .data,
        value!({
            "search": {
                "nameLookup": {
                    "nodes": [
                        {
                            "__typename": "Account",
                            "accountName": "kamu",
                        }
                    ],
                    "totalCount": 1,
                    "pageInfo": {
                        "totalPages": 1,
                        "hasNextPage": false,
                        "hasPreviousPage": false,
                    }
                }
            }
        }),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_search_by_id() {
    let harness = GqlSearchHarness::new().await;

    use odf::metadata::testing::alias;

    let created_datasets = harness.create_datasets([alias(&"kamu", &"foo")]).await;

    let dataset_id = created_datasets
        .first()
        .unwrap()
        .dataset_handle
        .id
        .as_did_str()
        .to_stack_string();

    assert_eq!(
        harness
            .search_authorized(&dataset_id, SearchOptions::default())
            .await
            .data,
        value!({
            "search": {
                "query": {
                    "nodes": [{
                        "__typename": "Dataset",
                        "name": "foo",
                    }],
                    "totalCount": 1,
                    "pageInfo": {
                        "totalPages": 1,
                        "hasNextPage": false,
                        "hasPreviousPage": false,
                    }
                }
            }
        })
    );

    let mut lowercase_dataset_id = dataset_id;
    lowercase_dataset_id.make_ascii_lowercase();

    assert_eq!(
        value!({
            "search": {
                "query": {
                    "nodes": [{
                        "__typename": "Dataset",
                        "name": "foo",
                    }],
                    "totalCount": 1,
                    "pageInfo": {
                        "totalPages": 1,
                        "hasNextPage": false,
                        "hasPreviousPage": false,
                    }
                }
            }
        }),
        harness
            .search_authorized(&lowercase_dataset_id, SearchOptions::default())
            .await
            .data,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_name_lookup_accounts_with_excluding() {
    let harness = GqlSearchHarness::new().await;

    let authorized_current_account_subject = harness
        .catalog_authorized
        .get_one::<CurrentAccountSubject>()
        .unwrap();
    let authorized_account_id = authorized_current_account_subject.account_id().to_string();

    assert_eq!(
        harness
            .name_lookup_anonymous(
                "kA",
                value!({
                    "byAccount": {
                        "excludeAccountsByIds": [authorized_account_id]
                    }
                })
            )
            .await
            .data,
        value!({
            "search": {
                "nameLookup": {
                    "nodes": [],
                    "totalCount": 0,
                    "pageInfo": {
                        "totalPages": 0,
                        "hasNextPage": false,
                        "hasPreviousPage": false,
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_search_security_context_of_anonymous() {
    let harness = SearchSecurityHarness::new(PredefinedAccountOpts::default()).await;

    for search in [SearchKind::FullText, SearchKind::FullTextListing] {
        assert_matches!(
            harness.security_of_anonymous_search(search).await,
            SearchSecurityContext::Anonymous,
            "{search:?}"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_search_security_context_of_user() {
    let harness = SearchSecurityHarness::new(PredefinedAccountOpts::default()).await;
    let account_id = harness.logged_account_id();

    for search in SearchKind::ALL {
        assert_matches!(
            harness.security_of_authorized_search(search).await,
            SearchSecurityContext::Restricted { current_principal_ids }
                if current_principal_ids == [account_id.clone()],
            "{search:?}"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_search_security_context_of_admin() {
    let harness = SearchSecurityHarness::new(PredefinedAccountOpts {
        is_admin: true,
        ..Default::default()
    })
    .await;

    for search in SearchKind::ALL {
        assert_matches!(
            harness.security_of_authorized_search(search).await,
            SearchSecurityContext::Unrestricted,
            "{search:?}"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseGQLDatasetHarness, base_gql_harness)]
struct GqlSearchHarness {
    base_gql_harness: BaseGQLDatasetHarness,
    catalog_anonymous: dill::Catalog,
    catalog_authorized: dill::Catalog,
    schema: kamu_adapter_graphql::Schema,
}

impl GqlSearchHarness {
    pub async fn new() -> Self {
        let base_gql_harness = BaseGQLDatasetHarness::builder()
            .tenancy_config(TenancyConfig::MultiTenant)
            .build();

        let (catalog_anonymous, catalog_authorized) =
            authentication_catalogs(base_gql_harness.catalog(), PredefinedAccountOpts::default())
                .await;
        let schema = kamu_adapter_graphql::schema_quiet();

        Self {
            base_gql_harness,
            catalog_anonymous,
            catalog_authorized,
            schema,
        }
    }

    pub async fn create_datasets(
        &self,
        dataset_aliases: impl IntoIterator<Item = odf::DatasetAlias>,
    ) -> Vec<CreateDatasetResult> {
        let create_dataset = self
            .catalog_authorized
            .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
            .unwrap();

        let mut results = Vec::new();
        for dataset_alias in dataset_aliases {
            results.push(
                create_dataset
                    .execute(
                        MetadataFactory::dataset_snapshot()
                            .name(dataset_alias)
                            .kind(odf::DatasetKind::Root)
                            .push_event(MetadataFactory::set_polling_source().build())
                            .build(),
                        Default::default(),
                    )
                    .await
                    .unwrap(),
            );
        }

        results
    }

    pub async fn execute_anonymous(&self, request: &str) -> Response {
        let res = self
            .schema
            .execute(Request::new(request).data(self.catalog_anonymous.clone()))
            .await;

        assert!(res.is_ok(), "{res:?}");

        res
    }

    pub async fn execute_authorized(&self, request: &str) -> Response {
        let res = self
            .schema
            .execute(
                Request::new(request)
                    .data(account_entity_data_loader(&self.catalog_authorized))
                    .data(self.catalog_authorized.clone()),
            )
            .await;

        assert!(res.is_ok(), "{res:?}");

        res
    }

    pub async fn search_authorized(&self, query: &str, options: SearchOptions) -> Response {
        use std::fmt::Write;

        let extra_arguments = {
            let mut s = String::new();
            if let Some(value) = options.page {
                write!(&mut s, ", page: {value}").unwrap();
            }
            if let Some(value) = options.per_page {
                write!(&mut s, ", perPage: {value}").unwrap();
            }
            s
        };

        self.execute_authorized(
            &indoc::indoc!(
                "
                {
                  search {
                    query(query: \"<query>\" <extra_arguments>) {
                      nodes {
                        __typename
                        ... on Dataset {
                          name
                        }
                      }
                      totalCount
                      pageInfo {
                        totalPages
                        hasNextPage
                        hasPreviousPage
                      }
                    }
                  }
                }
                "
            )
            .replace("<query>", query)
            .replace("<extra_arguments>", &extra_arguments),
        )
        .await
    }

    pub async fn name_lookup_anonymous(
        &self,
        query: &str,
        filters: async_graphql::Value,
    ) -> Response {
        self.execute_anonymous(
            &indoc::indoc!(
                "
                {
                  search {
                    nameLookup(
                      query: \"<query>\",
                      filters: <filters>
                    ) {
                      nodes {
                        __typename
                        ... on Account {
                          accountName
                        }
                      }
                      totalCount
                      pageInfo {
                        totalPages
                        hasNextPage
                        hasPreviousPage
                      }
                    }
                  }
                }
                "
            )
            .replace("<query>", query)
            .replace("<filters>", &format!("{filters}")),
        )
        .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct SearchOptions {
    page: Option<usize>,
    per_page: Option<usize>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseGQLDatasetHarness, base_gql_harness)]
struct SearchSecurityHarness {
    base_gql_harness: BaseGQLDatasetHarness,
    catalog_anonymous: dill::Catalog,
    catalog_authorized: dill::Catalog,
}

impl SearchSecurityHarness {
    async fn new(predefined_account_opts: PredefinedAccountOpts) -> Self {
        let base_gql_harness = BaseGQLDatasetHarness::builder()
            .tenancy_config(TenancyConfig::MultiTenant)
            .build();

        let base_catalog = dill::CatalogBuilder::new_chained(base_gql_harness.catalog())
            .add_value(SecurityRecordingSearchService::default())
            .bind::<dyn SearchService, SecurityRecordingSearchService>()
            .bind::<dyn DatasetSearchService, SecurityRecordingSearchService>()
            .build();

        let (catalog_anonymous, catalog_authorized) =
            authentication_catalogs(&base_catalog, predefined_account_opts).await;

        Self {
            base_gql_harness,
            catalog_anonymous,
            catalog_authorized,
        }
    }

    fn logged_account_id(&self) -> String {
        self.logged_account_from_catalog(&self.catalog_authorized)
            .account_handle
            .id
            .to_string()
    }

    async fn security_of_anonymous_search(&self, search: SearchKind) -> SearchSecurityContext {
        self.security_of_search(search, &self.catalog_anonymous)
            .await
    }

    async fn security_of_authorized_search(&self, search: SearchKind) -> SearchSecurityContext {
        self.security_of_search(search, &self.catalog_authorized)
            .await
    }

    /// Runs the search and returns the security context the search service
    /// received
    async fn security_of_search(
        &self,
        search: SearchKind,
        catalog: &dill::Catalog,
    ) -> SearchSecurityContext {
        let res = self.execute_query(search.request_code(), catalog).await;
        assert!(res.is_ok(), "{res:?}");

        catalog
            .get_one::<SecurityRecordingSearchService>()
            .unwrap()
            .take_single_recorded()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy)]
enum SearchKind {
    FullText,
    FullTextListing,
    NaturalLanguage,
    Hybrid,
}

impl SearchKind {
    const ALL: [Self; 4] = [
        Self::FullText,
        Self::FullTextListing,
        Self::NaturalLanguage,
        Self::Hybrid,
    ];

    fn request_code(self) -> &'static str {
        match self {
            Self::FullText => r#"{ search { queryFullText(prompt: "foo") { totalHits } } }"#,
            Self::FullTextListing => r#"{ search { queryFullText(prompt: "") { totalHits } } }"#,
            Self::NaturalLanguage => {
                r#"{ search { queryNaturalLanguage(prompt: "foo") { totalCount } } }"#
            }
            Self::Hybrid => r#"{ search { queryHybrid(prompt: "foo") { totalCount } } }"#,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Answers every search with no hits, recording the security context of each
#[derive(Default)]
struct SecurityRecordingSearchService {
    recorded: Mutex<Vec<SearchSecurityContext>>,
}

impl SecurityRecordingSearchService {
    fn record(&self, ctx: &SearchContext<'_>) {
        self.recorded.lock().unwrap().push(ctx.security.clone());
    }

    fn take_single_recorded(&self) -> SearchSecurityContext {
        let mut recorded = std::mem::take(&mut *self.recorded.lock().unwrap());
        assert_eq!(recorded.len(), 1, "{recorded:?}");
        recorded.pop().unwrap()
    }

    fn empty_response() -> SearchResponse {
        SearchResponse {
            took_ms: 0,
            timeout: false,
            total_hits: Some(0),
            hits: Vec::new(),
        }
    }
}

#[async_trait::async_trait]
impl SearchService for SecurityRecordingSearchService {
    async fn health(&self, _ctx: SearchContext<'_>) -> Result<serde_json::Value, SearchError> {
        unimplemented!()
    }

    async fn find_document_by_id(
        &self,
        _ctx: SearchContext<'_>,
        _schema_name: SearchEntitySchemaName,
        _id: &SearchEntityId,
    ) -> Result<Option<serde_json::Value>, SearchError> {
        unimplemented!()
    }

    async fn bulk_update(
        &self,
        _ctx: SearchContext<'_>,
        _schema_name: SearchEntitySchemaName,
        _operations: Vec<SearchIndexUpdateOperation>,
    ) -> Result<(), SearchError> {
        unimplemented!()
    }

    async fn listing_search(
        &self,
        ctx: SearchContext<'_>,
        _req: ListingSearchRequest,
    ) -> Result<SearchResponse, SearchError> {
        self.record(&ctx);
        Ok(Self::empty_response())
    }

    async fn text_search(
        &self,
        ctx: SearchContext<'_>,
        _req: TextSearchRequest,
    ) -> Result<SearchResponse, SearchError> {
        self.record(&ctx);
        Ok(Self::empty_response())
    }

    async fn vector_search(
        &self,
        _ctx: SearchContext<'_>,
        _req: VectorSearchRequest,
    ) -> Result<SearchResponse, SearchError> {
        unimplemented!()
    }

    async fn hybrid_search(
        &self,
        _ctx: SearchContext<'_>,
        _req: HybridSearchRequest,
    ) -> Result<SearchResponse, SearchError> {
        unimplemented!()
    }
}

#[async_trait::async_trait]
impl DatasetSearchService for SecurityRecordingSearchService {
    async fn vector_search(
        &self,
        ctx: SearchContext<'_>,
        _prompt: String,
        _limit: usize,
    ) -> Result<DatasetSearchResponse, DatasetSearchError> {
        self.record(&ctx);
        Ok(DatasetSearchResponse::default())
    }

    async fn hybrid_search(
        &self,
        ctx: SearchContext<'_>,
        _prompt: String,
        _limit: usize,
    ) -> Result<DatasetSearchResponse, DatasetSearchError> {
        self.record(&ctx);
        Ok(DatasetSearchResponse::default())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
