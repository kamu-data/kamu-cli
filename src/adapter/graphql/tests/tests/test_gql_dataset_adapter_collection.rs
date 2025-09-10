// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bon::bon;
use indoc::indoc;
use kamu::testing::MockDatasetActionAuthorizer;
use kamu_core::*;
use kamu_datasets_services::*;
use odf::dataset::MetadataChainExt;
use serde_json::json;

use crate::utils::{BaseGQLDatasetHarness, PredefinedAccountOpts, authentication_catalogs};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_collection_operations() {
    let harness = GraphQLDatasetsHarness::builder()
        .tenancy_config(TenancyConfig::MultiTenant)
        .build()
        .await;

    // Create empty datasets to link to
    let foo = harness.create_root_dataset("foo").await;
    let bar = harness.create_root_dataset("bar").await;
    let baz = harness.create_root_dataset("baz").await;

    // Create collection dataset
    let did = harness.create_collection("x", None).await;

    // Entries are empty
    pretty_assertions::assert_eq!(harness.list_entries(&did).await, json!([]));

    // No-op update is ok
    harness.update_entries(&did, json!([])).await;
    pretty_assertions::assert_eq!(harness.list_entries(&did).await, json!([]));

    // Add first entry
    harness
        .update_entries(
            &did,
            json!([{
                "add": {
                    "entry": {
                        "path": "/foo",
                        "ref": foo.dataset_handle.id,
                    }
                }
            }]),
        )
        .await;

    pretty_assertions::assert_eq!(
        harness.list_entries(&did).await,
        json!([{
            "path": "/foo",
            "ref": foo.dataset_handle.id,
            "extraData": {},
        }])
    );

    assert_eq!(harness.get_metadata_chain_len(&did).await, 4);

    // Add duplicate entry - no new events should be generated
    harness
        .update_entries(
            &did,
            json!([{
                "add": {
                    "entry": {
                        "path": "/foo",
                        "ref": foo.dataset_handle.id,
                    }
                }
            }]),
        )
        .await;

    pretty_assertions::assert_eq!(
        harness.list_entries(&did).await,
        json!([{
            "path": "/foo",
            "ref": foo.dataset_handle.id,
            "extraData": {},
        }])
    );

    assert_eq!(harness.get_metadata_chain_len(&did).await, 4);

    // Add second entry
    harness
        .update_entries(
            &did,
            json![{
                "add": {
                    "entry": {
                        "path": "/bar",
                        "ref": bar.dataset_handle.id,
                    }
                }
            }],
        )
        .await;

    // Entries are alphabetically sorted
    pretty_assertions::assert_eq!(
        harness.list_entries(&did).await,
        json!([
            {
                "path": "/bar",
                "ref": bar.dataset_handle.id,
                "extraData": {},
            },
            {
                "path": "/foo",
                "ref": foo.dataset_handle.id,
                "extraData": {},
            },
        ])
    );

    // Add third entry, nested
    harness
        .update_entries(
            &did,
            json![{
                "add": {
                    "entry": {
                        "path": "/dir/baz",
                        "ref": baz.dataset_handle.id,
                    }
                }
            }],
        )
        .await;

    pretty_assertions::assert_eq!(
        harness.list_entries(&did).await,
        json!([
            {
                "path": "/bar",
                "ref": bar.dataset_handle.id,
                "extraData": {},
            },
            {
                "path": "/dir/baz",
                "ref": baz.dataset_handle.id,
                "extraData": {},
            },
            {
                "path": "/foo",
                "ref": foo.dataset_handle.id,
                "extraData": {},
            },
        ])
    );

    // Move entry
    harness
        .update_entries(
            &did,
            json![{
                "move": {
                    "pathFrom": "/dir/baz",
                    "pathTo": "/baz",
                }
            }],
        )
        .await;

    pretty_assertions::assert_eq!(
        harness.list_entries(&did).await,
        json!([
            {
                "path": "/bar",
                "ref": bar.dataset_handle.id,
                "extraData": {},
            },
            {
                "path": "/baz",
                "ref": baz.dataset_handle.id,
                "extraData": {},
            },
            {
                "path": "/foo",
                "ref": foo.dataset_handle.id,
                "extraData": {},
            },
        ])
    );

    // Remove entry
    harness
        .update_entries(
            &did,
            json![{
                "remove": {
                    "path": "/foo",
                }
            }],
        )
        .await;

    pretty_assertions::assert_eq!(
        harness.list_entries(&did).await,
        json!([
            {
                "path": "/bar",
                "ref": bar.dataset_handle.id,
                "extraData": {},
            },
            {
                "path": "/baz",
                "ref": baz.dataset_handle.id,
                "extraData": {},
            },
        ])
    );

    // Move to replace
    harness
        .update_entries(
            &did,
            json![{
                "move": {
                    "pathFrom": "/baz",
                    "pathTo": "/bar",
                }
            }],
        )
        .await;

    pretty_assertions::assert_eq!(
        harness.list_entries(&did).await,
        json!([
            {
                "path": "/bar",
                "ref": baz.dataset_handle.id,
                "extraData": {},
            },
        ])
    );

    // Add to replace
    harness
        .update_entries(
            &did,
            json![{
                "add": {
                    "entry": {
                        "path": "/bar",
                        "ref": foo.dataset_handle.id,
                    }
                }
            }],
        )
        .await;

    pretty_assertions::assert_eq!(
        harness.list_entries(&did).await,
        json!([
            {
                "path": "/bar",
                "ref": foo.dataset_handle.id,
                "extraData": {},
            },
        ])
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_collection_extra_data() {
    let harness = GraphQLDatasetsHarness::builder()
        .tenancy_config(TenancyConfig::MultiTenant)
        .build()
        .await;

    // Create empty datasets to link to
    let linked = harness.create_root_dataset("foo").await.dataset_handle.id;

    // Create collection dataset
    let did = harness
        .create_collection(
            "x",
            Some(json!([{
                "name": "foo",
                "type": {
                    "ddl": "string",
                }
            }, {
                "name": "bar",
                "type": {
                    "ddl": "int",
                }
            }])),
        )
        .await;

    // Entries are empty
    pretty_assertions::assert_eq!(harness.list_entries(&did).await, json!([]));

    // Add first entry
    harness
        .update_entries(
            &did,
            json![{
                "add": {
                    "entry": {
                        "path": "/foo",
                        "ref": linked,
                        "extraData": {"foo": "aaa", "bar": 111},
                    }
                }
            }],
        )
        .await;

    pretty_assertions::assert_eq!(
        harness.list_entries(&did).await,
        json!([{
            "path": "/foo",
            "ref": linked,
            "extraData": {"foo": "aaa", "bar": 111}
        }])
    );

    // Preserved after move
    harness
        .update_entries(
            &did,
            json![{
                "move": {
                    "pathFrom": "/foo",
                    "pathTo": "/bar",
                }
            }],
        )
        .await;

    pretty_assertions::assert_eq!(
        harness.list_entries(&did).await,
        json!([{
            "path": "/bar",
            "ref": linked,
            "extraData": {"foo": "aaa", "bar": 111}
        }])
    );

    // Update via move
    harness
        .update_entries(
            &did,
            json![{
                "move": {
                    "pathFrom": "/bar",
                    "pathTo": "/bar",
                    "extraData": {"foo": "bbb", "bar": 222}
                }
            }],
        )
        .await;

    pretty_assertions::assert_eq!(
        harness.list_entries(&did).await,
        json!([{
            "path": "/bar",
            "ref": linked,
            "extraData": {"foo": "bbb", "bar": 222}
        }])
    );

    assert_eq!(harness.get_metadata_chain_len(&did).await, 8);

    // Redundant add does not produce a new event
    harness
        .update_entries(
            &did,
            json![{
                "add": {
                    "entry": {
                        "path": "/bar",
                        "ref": linked,
                        "extraData": {"foo": "bbb", "bar": 222}
                    }
                }
            }],
        )
        .await;

    assert_eq!(harness.get_metadata_chain_len(&did).await, 8);

    // Move without changing anything does not produce a new event
    harness
        .update_entries(
            &did,
            json![{
                "move": {
                    "pathFrom": "/bar",
                    "pathTo": "/bar",
                    "extraData": {"foo": "bbb", "bar": 222}
                }
            }],
        )
        .await;

    assert_eq!(harness.get_metadata_chain_len(&did).await, 8);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_collection_path_prefix_and_max_depth() {
    let harness = GraphQLDatasetsHarness::builder()
        .tenancy_config(TenancyConfig::MultiTenant)
        .build()
        .await;

    // Create empty dataset to link to
    let linked = harness.create_root_dataset("foo").await.dataset_handle.id;

    // Create collection dataset
    let did = harness.create_collection("x", None).await;

    harness
        .update_entries(
            &did,
            json!([
                {
                    "add": {
                        "entry": {
                            "path": "/a",
                            "ref": linked,
                        }
                    }
                },
                {
                    "add": {
                        "entry": {
                            "path": "/1/b",
                            "ref": linked,
                        }
                    }
                },
                {
                    "add": {
                        "entry": {
                            "path": "/1/2/c",
                            "ref": linked,
                        }
                    }
                }
            ]),
        )
        .await;

    pretty_assertions::assert_eq!(
        harness.list_entries(&did).await,
        json!([
            {
                "path": "/1/2/c",
                "ref": linked,
                "extraData": {},
            },
            {
                "path": "/1/b",
                "ref": linked,
                "extraData": {},
            },
            {
                "path": "/a",
                "ref": linked,
                "extraData": {},
            },
        ])
    );

    // With prefix
    pretty_assertions::assert_eq!(
        harness.list_entries_ext(&did, Some("/1/"), None).await,
        json!([
            {
                "path": "/1/2/c",
                "ref": linked,
                "extraData": {},
            },
            {
                "path": "/1/b",
                "ref": linked,
                "extraData": {},
            },
        ])
    );

    pretty_assertions::assert_eq!(
        harness.list_entries_ext(&did, Some("/1/2/"), None).await,
        json!([
            {
                "path": "/1/2/c",
                "ref": linked,
                "extraData": {},
            },
        ])
    );

    pretty_assertions::assert_eq!(
        harness.list_entries_ext(&did, Some("/1/2/c"), None).await,
        json!([
            {
                "path": "/1/2/c",
                "ref": linked,
                "extraData": {},
            },
        ])
    );

    pretty_assertions::assert_eq!(
        harness.list_entries_ext(&did, Some("/1/2/3/"), None).await,
        json!([])
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_collection_entry_search() {
    let harness = GraphQLDatasetsHarness::builder()
        .tenancy_config(TenancyConfig::MultiTenant)
        .build()
        .await;

    // Create empty dataset to link to
    let foo = harness.create_root_dataset("foo").await.dataset_handle.id;
    let bar = harness.create_root_dataset("bar").await.dataset_handle.id;
    let baz = harness.create_root_dataset("baz").await.dataset_handle.id;

    // Create collection dataset
    let did = harness.create_collection("x", None).await;

    harness
        .update_entries(
            &did,
            json!([
                {
                    "add": {
                        "entry": {
                            "path": "/foo",
                            "ref": foo,
                        }
                    }
                },
                {
                    "add": {
                        "entry": {
                            "path": "/bar",
                            "ref": bar,
                        }
                    }
                },
                {
                    "add": {
                        "entry": {
                            "path": "/baz/foo",
                            "ref": foo,
                        }
                    }
                }
            ]),
        )
        .await;

    // Path does not exist
    pretty_assertions::assert_eq!(harness.get_entry(&did, "/barz").await, json!(null));

    // Get single entry by path
    pretty_assertions::assert_eq!(
        harness.get_entry(&did, "/bar").await,
        json!({
            "path": "/bar",
            "ref": bar,
            "extraData": {},
        })
    );

    // Reverse search by refs
    pretty_assertions::assert_eq!(
        harness.entries_by_ref(&did, &[&foo]).await,
        json!([
            {
                "path": "/baz/foo",
                "ref": foo,
                "extraData": {},
            },
            {
                "path": "/foo",
                "ref": foo,
                "extraData": {},
            },
        ])
    );

    pretty_assertions::assert_eq!(harness.entries_by_ref(&did, &[&baz]).await, json!([]));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_collection_resolve_ref_to_dataset() {
    let harness = GraphQLDatasetsHarness::builder()
        .tenancy_config(TenancyConfig::MultiTenant)
        .build()
        .await;

    // Create empty datasets to link to
    let foo = harness.create_root_dataset("foo").await;

    // Create collection dataset
    let did = harness.create_collection("x", None).await;

    // Add first entry
    harness
        .update_entries(
            &did,
            json!([{
                "add": {
                    "entry": {
                        "path": "/foo",
                        "ref": foo.dataset_handle.id,
                    }
                }
            }]),
        )
        .await;

    pretty_assertions::assert_eq!(
        harness.list_entries(&did).await,
        json!([{
            "path": "/foo",
            "ref": foo.dataset_handle.id,
            "extraData": {},
        }])
    );

    // List and resolve entries
    let res = harness
        .execute_authorized_query(
            async_graphql::Request::new(indoc!(
                r#"
                    query ($datasetId: DatasetID!) {
                        datasets {
                            byId(datasetId: $datasetId) {
                                asCollection {
                                    latest {
                                        entries {
                                            nodes {
                                                path
                                                ref
                                                extraData
                                                asDataset {
                                                    alias
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    "#
            ))
            .variables(async_graphql::Variables::from_json(json!({
                "datasetId": did,
            }))),
        )
        .await;

    assert!(res.is_ok(), "{res:#?}");
    pretty_assertions::assert_eq!(
        res.data.into_json().unwrap()["datasets"]["byId"]["asCollection"]["latest"]["entries"]
            ["nodes"]
            .clone(),
        json!([{
            "path": "/foo",
            "ref": foo.dataset_handle.id,
            "extraData": {},
            "asDataset": {
                "alias": "kamu/foo",
            },
        }]),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_add_entry_errors() {
    let harness = GraphQLDatasetsHarness::builder()
        .tenancy_config(TenancyConfig::MultiTenant)
        .build()
        .await;

    let foo = harness.create_root_dataset("foo").await.dataset_handle.id;
    let did = harness.create_collection("x", None).await;

    // CAS error
    let expected_head = odf::Multihash::from_digest_sha3_256(b"hello");
    let res = harness
        .execute_authorized_query(harness.add_entry_request(
            &did,
            &json!({
                "path": "/foo",
                "ref": foo,
            }),
            Some(&expected_head),
        ))
        .await;

    assert!(res.is_ok(), "{res:#?}");

    pretty_assertions::assert_eq!(
        res.data.into_json().unwrap()["datasets"]["byId"]["asCollection"]["addEntry"].clone(),
        json!({
            "expectedHead": expected_head,
            "actualHead": null,
            "isSuccess": false,
            "message": "Expected head didn't match, dataset was likely updated concurrently"
        }),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseGQLDatasetHarness, base_gql_harness)]
struct GraphQLDatasetsHarness {
    base_gql_harness: BaseGQLDatasetHarness,
    catalog_authorized: dill::Catalog,
}

#[bon]
impl GraphQLDatasetsHarness {
    #[builder]
    pub async fn new(
        tenancy_config: TenancyConfig,
        mock_dataset_action_authorizer: Option<MockDatasetActionAuthorizer>,
    ) -> Self {
        let base_gql_harness = BaseGQLDatasetHarness::builder()
            .tenancy_config(tenancy_config)
            .maybe_mock_dataset_action_authorizer(mock_dataset_action_authorizer)
            .build();

        let cache_dir = base_gql_harness.temp_dir().join("cache");
        std::fs::create_dir(&cache_dir).unwrap();

        let base_catalog = dill::CatalogBuilder::new_chained(base_gql_harness.catalog())
            .add::<RenameDatasetUseCaseImpl>()
            .add::<DeleteDatasetUseCaseImpl>()
            .add_value(kamu::EngineConfigDatafusionEmbeddedBatchQuery::default())
            .add::<kamu::QueryServiceImpl>()
            .add::<kamu::ObjectStoreRegistryImpl>()
            .add::<kamu::ObjectStoreBuilderLocalFs>()
            .add_value(kamu::EngineConfigDatafusionEmbeddedIngest::default())
            .add::<kamu::EngineProvisionerNull>()
            .add::<kamu::DataFormatRegistryImpl>()
            .add::<kamu::PushIngestPlannerImpl>()
            .add::<kamu::PushIngestExecutorImpl>()
            .add::<kamu::PushIngestDataUseCaseImpl>()
            .build();

        let (_catalog_anonymous, catalog_authorized) =
            authentication_catalogs(&base_catalog, PredefinedAccountOpts::default()).await;

        Self {
            base_gql_harness,
            catalog_authorized,
        }
    }

    pub async fn execute_authorized_query(
        &self,
        query: impl Into<async_graphql::Request>,
    ) -> async_graphql::Response {
        kamu_adapter_graphql::schema_quiet()
            .execute(query.into().data(self.catalog_authorized.clone()))
            .await
    }

    pub async fn create_root_dataset(&self, name: &str) -> kamu_datasets::CreateDatasetResult {
        let create_dataset = self
            .catalog_authorized
            .get_one::<dyn kamu_datasets::CreateDatasetFromSnapshotUseCase>()
            .unwrap();

        create_dataset
            .execute(
                odf::metadata::testing::MetadataFactory::dataset_snapshot()
                    .name(odf::DatasetAlias::new(
                        None,
                        odf::DatasetName::new_unchecked(name),
                    ))
                    .kind(odf::DatasetKind::Root)
                    .push_event(
                        odf::metadata::testing::MetadataFactory::set_polling_source().build(),
                    )
                    .build(),
                Default::default(),
            )
            .await
            .unwrap()
    }

    pub async fn create_collection(
        &self,
        alias: &str,
        extra_columns: Option<serde_json::Value>,
    ) -> odf::DatasetID {
        let res = self
            .execute_authorized_query(
                async_graphql::Request::new(indoc!(
                    r#"
                    mutation ($datasetAlias: DatasetAlias!, $extraColumns: [ColumnInput]) {
                        datasets {
                            createCollection(
                                datasetAlias: $datasetAlias,
                                extraColumns: $extraColumns,
                                datasetVisibility: PRIVATE,
                            ) {
                                message
                                ... on CreateDatasetResultSuccess {
                                    dataset {
                                        id
                                    }
                                }
                            }
                        }
                    }
                    "#
                ))
                .variables(async_graphql::Variables::from_json(json!({
                    "datasetAlias": alias,
                    "extraColumns": extra_columns,
                }))),
            )
            .await;

        assert!(res.is_ok(), "{res:#?}");
        let id = res.data.into_json().unwrap()["datasets"]["createCollection"]["dataset"]["id"]
            .as_str()
            .unwrap()
            .to_string();

        odf::DatasetID::from_did_str(&id).unwrap()
    }

    pub async fn get_dataset(&self, dataset_id: &odf::DatasetID) -> ResolvedDataset {
        let dataset_reg = self
            .catalog_authorized
            .get_one::<dyn DatasetRegistry>()
            .unwrap();

        dataset_reg.get_dataset_by_id(dataset_id).await.unwrap()
    }

    pub async fn get_metadata_chain_len(&self, dataset_id: &odf::DatasetID) -> usize {
        let dataset = self.get_dataset(dataset_id).await;
        let last_block = dataset
            .as_metadata_chain()
            .get_block_by_ref(&odf::BlockRef::Head)
            .await
            .unwrap();
        usize::try_from(last_block.sequence_number).unwrap() + 1
    }

    pub async fn list_entries(&self, did: &odf::DatasetID) -> serde_json::Value {
        let res = self
            .execute_authorized_query(
                async_graphql::Request::new(indoc!(
                    r#"
                    query ($datasetId: DatasetID!) {
                        datasets {
                            byId(datasetId: $datasetId) {
                                asCollection {
                                    latest {
                                        entries {
                                            nodes {
                                                path
                                                ref
                                                extraData
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    "#
                ))
                .variables(async_graphql::Variables::from_json(json!({
                    "datasetId": did,
                }))),
            )
            .await;

        assert!(res.is_ok(), "{res:#?}");
        res.data.into_json().unwrap()["datasets"]["byId"]["asCollection"]["latest"]["entries"]
            ["nodes"]
            .clone()
    }

    pub async fn list_entries_ext(
        &self,
        did: &odf::DatasetID,
        path_prefix: Option<&str>,
        max_depth: Option<usize>,
    ) -> serde_json::Value {
        let res = self
            .execute_authorized_query(
                async_graphql::Request::new(indoc!(
                    r#"
                    query ($datasetId: DatasetID!, $pathPrefix: String, $maxDepth: Int) {
                        datasets {
                            byId(datasetId: $datasetId) {
                                asCollection {
                                    latest {
                                        entries(pathPrefix: $pathPrefix, maxDepth: $maxDepth) {
                                            nodes {
                                                path
                                                ref
                                                extraData
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    "#
                ))
                .variables(async_graphql::Variables::from_json(json!({
                    "datasetId": did,
                    "pathPrefix": path_prefix.unwrap_or(""),
                    "maxDepth": max_depth,
                }))),
            )
            .await;

        assert!(res.is_ok(), "{res:#?}");
        res.data.into_json().unwrap()["datasets"]["byId"]["asCollection"]["latest"]["entries"]
            ["nodes"]
            .clone()
    }

    pub fn add_entry_request(
        &self,
        did: &odf::DatasetID,
        entry: &serde_json::Value,
        expected_head: Option<&odf::Multihash>,
    ) -> async_graphql::Request {
        async_graphql::Request::new(indoc!(
            r#"
            mutation ($datasetId: DatasetID!, $entry: [CollectionEntryInput!], $expectedHead: Multihash) {
                datasets {
                    byId(datasetId: $datasetId) {
                        asCollection {
                            addEntry(entry: $entry, expectedHead: $expectedHead) {
                                isSuccess
                                message
                                ... on CollectionUpdateErrorCasFailed {
                                    expectedHead
                                    actualHead
                                    isSuccess
                                    message
                                }
                            }
                        }
                    }
                }
            }
            "#
        ))
        .variables(async_graphql::Variables::from_json(json!({
            "datasetId": did,
            "entry": entry,
            "expectedHead": expected_head
        })))
    }

    pub async fn update_entries(&self, did: &odf::DatasetID, operations: serde_json::Value) {
        let res = self
            .execute_authorized_query(
                async_graphql::Request::new(indoc!(
                    r#"
                    mutation ($datasetId: DatasetID!, $operations: [CollectionUpdateInput!]) {
                        datasets {
                            byId(datasetId: $datasetId) {
                                asCollection {
                                    updateEntries(operations: $operations) {
                                        isSuccess
                                        message
                                    }
                                }
                            }
                        }
                    }
                    "#
                ))
                .variables(async_graphql::Variables::from_json(json!({
                    "datasetId": did,
                    "operations": operations,
                }))),
            )
            .await;

        assert!(res.is_ok(), "{res:#?}");
        pretty_assertions::assert_eq!(
            res.data.into_json().unwrap()["datasets"]["byId"]["asCollection"]["updateEntries"],
            json!({
                "isSuccess": true,
                "message": "",
            })
        );
    }

    pub async fn get_entry(&self, did: &odf::DatasetID, path: &str) -> serde_json::Value {
        let res = self
            .execute_authorized_query(
                async_graphql::Request::new(indoc!(
                    r#"
                    query ($datasetId: DatasetID!, $path: String!) {
                        datasets {
                            byId(datasetId: $datasetId) {
                                asCollection {
                                    latest {
                                        entry(path: $path) {
                                            path
                                            ref
                                            extraData
                                        }
                                    }
                                }
                            }
                        }
                    }
                    "#
                ))
                .variables(async_graphql::Variables::from_json(json!({
                    "datasetId": did,
                    "path": path,
                }))),
            )
            .await;

        assert!(res.is_ok(), "{res:#?}");
        res.data.into_json().unwrap()["datasets"]["byId"]["asCollection"]["latest"]["entry"].clone()
    }

    pub async fn entries_by_ref(
        &self,
        did: &odf::DatasetID,
        refs: &[&odf::DatasetID],
    ) -> serde_json::Value {
        let res = self
            .execute_authorized_query(
                async_graphql::Request::new(indoc!(
                    r#"
                    query ($datasetId: DatasetID!, $refs: [DatasetID!]!) {
                        datasets {
                            byId(datasetId: $datasetId) {
                                asCollection {
                                    latest {
                                        entriesByRef(refs: $refs) {
                                            path
                                            ref
                                            extraData
                                        }
                                    }
                                }
                            }
                        }
                    }
                    "#
                ))
                .variables(async_graphql::Variables::from_json(json!({
                    "datasetId": did,
                    "refs": refs,
                }))),
            )
            .await;

        assert!(res.is_ok(), "{res:#?}");
        res.data.into_json().unwrap()["datasets"]["byId"]["asCollection"]["latest"]["entriesByRef"]
            .clone()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
