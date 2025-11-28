// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use base64::Engine;
use bon::bon;
use indoc::indoc;
use kamu::testing::MockDatasetActionAuthorizer;
use kamu_adapter_http::platform::UploadServiceLocal;
use kamu_core::*;
use kamu_datasets_services::*;
use serde_json::json;

use crate::utils::{BaseGQLDatasetHarness, PredefinedAccountOpts, authentication_catalogs};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn base64usnp_encode(data: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(data)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_versioned_file_create_in_band() {
    let harness = GraphQLDatasetsHarness::builder()
        .tenancy_config(TenancyConfig::MultiTenant)
        .build()
        .await;

    // Create versioned file dataset
    let did = harness
        .create_versioned_file(&odf::DatasetAlias::new(
            None,
            odf::DatasetName::new_unchecked("x"),
        ))
        .await;

    // Check no versions
    let res = harness
        .execute_authorized_query(
            async_graphql::Request::new(indoc!(
                r#"
                query ($datasetId: DatasetID!) {
                    datasets {
                        byId(datasetId: $datasetId) {
                            asVersionedFile {
                                versions {
                                    nodes {
                                        version
                                    }
                                }
                            }
                        }
                    }
                }
                "#
            ))
            .variables(async_graphql::Variables::from_json(json!({
                "datasetId": &did,
            }))),
        )
        .await;

    assert!(res.is_ok(), "{res:#?}");
    let nodes = res.data.into_json().unwrap()["datasets"]["byId"]["asVersionedFile"]["versions"]
        ["nodes"]
        .clone();
    pretty_assertions::assert_eq!(nodes, json!([]));

    // Check no latest version
    let res = harness
        .execute_authorized_query(
            async_graphql::Request::new(indoc!(
                r#"
                query ($datasetId: DatasetID!) {
                    datasets {
                        byId(datasetId: $datasetId) {
                            asVersionedFile {
                                latest {
                                    version
                                }
                            }
                        }
                    }
                }
                "#
            ))
            .variables(async_graphql::Variables::from_json(json!({
                "datasetId": &did,
            }))),
        )
        .await;

    assert!(res.is_ok(), "{res:#?}");
    let latest = &res.data.into_json().unwrap()["datasets"]["byId"]["asVersionedFile"]["latest"];
    assert_eq!(*latest, serde_json::Value::Null);

    // Upload first version
    let res = harness
        .in_band_upload_versioned_file(&did, base64usnp_encode(b"hello").as_str())
        .await;

    let upload_result = res.data.into_json().unwrap()["datasets"]["byId"]["asVersionedFile"]
        ["uploadNewVersion"]
        .clone();
    let head_v1 = &upload_result["newHead"];
    pretty_assertions::assert_eq!(
        upload_result,
        json!({
            "newVersion": 1,
            "contentHash": odf::Multihash::from_digest_sha3_256(b"hello"),
            "newHead": head_v1,
        })
    );

    // Read back latest content
    let res = harness
        .execute_authorized_query(
            async_graphql::Request::new(indoc!(
                r#"
                query ($datasetId: DatasetID!) {
                    datasets {
                        byId(datasetId: $datasetId) {
                            head
                            asVersionedFile {
                                latest {
                                    version
                                    contentLength
                                    contentType
                                    contentHash
                                    content
                                    extraData
                                }
                            }
                        }
                    }
                }
                "#
            ))
            .variables(async_graphql::Variables::from_json(json!({
                "datasetId": &did,
            }))),
        )
        .await;

    assert!(res.is_ok(), "{res:#?}");
    let dataset = res.data.into_json().unwrap()["datasets"]["byId"].clone();
    let latest = dataset["asVersionedFile"]["latest"].clone();
    pretty_assertions::assert_eq!(dataset["head"], *head_v1);
    pretty_assertions::assert_eq!(
        latest,
        json!({
            "version": 1,
            "contentLength": b"hello".len(),
            "contentHash": odf::Multihash::from_digest_sha3_256(b"hello"),
            "contentType": "application/octet-stream",
            "extraData": {},
            "content": base64usnp_encode(b"hello"),
        })
    );

    // Upload second version
    let res = harness
        .execute_authorized_query(
            async_graphql::Request::new(indoc!(
                r#"
                mutation ($datasetId: DatasetID!, $content: Base64Usnp!, $expectedHead: Multihash!) {
                    datasets {
                        byId(datasetId: $datasetId) {
                            asVersionedFile {
                                uploadNewVersion(content: $content, expectedHead: $expectedHead) {
                                    ... on UpdateVersionSuccess {
                                        newVersion
                                        contentHash
                                        newHead
                                    }
                                }
                            }
                        }
                    }
                }
                "#
            ))
            .variables(async_graphql::Variables::from_json(json!({
                "datasetId": &did,
                "content": base64usnp_encode(b"bye"),
                "expectedHead": head_v1,
            }))),
        )
        .await;

    assert!(res.is_ok(), "{res:#?}");
    let upload_result = res.data.into_json().unwrap()["datasets"]["byId"]["asVersionedFile"]
        ["uploadNewVersion"]
        .clone();
    let head_v2 = &upload_result["newHead"];
    pretty_assertions::assert_eq!(
        upload_result,
        json!({
            "newVersion": 2,
            "contentHash": odf::Multihash::from_digest_sha3_256(b"bye"),
            "newHead": head_v2,
        })
    );

    // Read back latest content
    let res = harness
        .execute_authorized_query(
            async_graphql::Request::new(indoc!(
                r#"
                query ($datasetId: DatasetID!) {
                    datasets {
                        byId(datasetId: $datasetId) {
                            head
                            asVersionedFile {
                                latest {
                                    version
                                    contentLength
                                    contentType
                                    contentHash
                                    content
                                    extraData
                                }
                            }
                        }
                    }
                }
                "#
            ))
            .variables(async_graphql::Variables::from_json(json!({
                "datasetId": &did,
            }))),
        )
        .await;

    assert!(res.is_ok(), "{res:#?}");
    let dataset = res.data.into_json().unwrap()["datasets"]["byId"].clone();
    let latest = dataset["asVersionedFile"]["latest"].clone();
    pretty_assertions::assert_eq!(dataset["head"], *head_v2);
    pretty_assertions::assert_eq!(
        latest,
        json!({
            "version": 2,
            "contentLength": b"bye".len(),
            "contentHash": odf::Multihash::from_digest_sha3_256(b"bye"),
            "contentType": "application/octet-stream",
            "extraData": {},
            "content": base64usnp_encode(b"bye"),
        })
    );

    // Read back first version (via block hash)
    let res = harness
        .execute_authorized_query(
            async_graphql::Request::new(indoc!(
                r#"
                query ($datasetId: DatasetID!, $blockHash: Multihash!) {
                    datasets {
                        byId(datasetId: $datasetId) {
                            asVersionedFile {
                                asOf(blockHash: $blockHash) {
                                    version
                                    contentLength
                                    contentType
                                    contentHash
                                    content
                                    extraData
                                }
                            }
                        }
                    }
                }
                "#
            ))
            .variables(async_graphql::Variables::from_json(json!({
                "datasetId": &did,
                "blockHash": head_v1,
            }))),
        )
        .await;

    assert!(res.is_ok(), "{res:#?}");
    let entry =
        res.data.into_json().unwrap()["datasets"]["byId"]["asVersionedFile"]["asOf"].clone();
    pretty_assertions::assert_eq!(
        entry,
        json!({
            "version": 1,
            "contentLength": b"hello".len(),
            "contentHash": odf::Multihash::from_digest_sha3_256(b"hello"),
            "contentType": "application/octet-stream",
            "extraData": {},
            "content": base64usnp_encode(b"hello"),
        })
    );

    // Read back first version (via version number)
    let res = harness
        .execute_authorized_query(
            async_graphql::Request::new(indoc!(
                r#"
                query ($datasetId: DatasetID!) {
                    datasets {
                        byId(datasetId: $datasetId) {
                            asVersionedFile {
                                asOf(version: 1) {
                                    version
                                    contentLength
                                    contentType
                                    contentHash
                                    content
                                    extraData
                                }
                            }
                        }
                    }
                }
                "#
            ))
            .variables(async_graphql::Variables::from_json(json!({
                "datasetId": &did,
            }))),
        )
        .await;

    assert!(res.is_ok(), "{res:#?}");
    let entry =
        res.data.into_json().unwrap()["datasets"]["byId"]["asVersionedFile"]["asOf"].clone();
    pretty_assertions::assert_eq!(
        entry,
        json!({
            "version": 1,
            "contentLength": b"hello".len(),
            "contentHash": odf::Multihash::from_digest_sha3_256(b"hello"),
            "contentType": "application/octet-stream",
            "extraData": {},
            "content": base64usnp_encode(b"hello"),
        })
    );

    // Try upload with non-latest head to simulate concurrency
    let res = harness
        .execute_authorized_query(
            async_graphql::Request::new(indoc!(
                r#"
                mutation ($datasetId: DatasetID!, $content: Base64Usnp!, $expectedHead: Multihash!) {
                    datasets {
                        byId(datasetId: $datasetId) {
                            asVersionedFile {
                                uploadNewVersion(content: $content, expectedHead: $expectedHead) {
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
                "datasetId": &did,
                "content": base64usnp_encode(b"boo"),
                "expectedHead": head_v1,
            }))),
        )
        .await;

    assert!(res.is_ok(), "{res:#?}");
    let upload_result = res.data.into_json().unwrap()["datasets"]["byId"]["asVersionedFile"]
        ["uploadNewVersion"]
        .clone();
    pretty_assertions::assert_eq!(
        upload_result,
        json!({
            "isSuccess": false,
            "message": "Expected head didn't match, dataset was likely updated concurrently",
        })
    );

    // Check list versions
    let res = harness
        .execute_authorized_query(
            async_graphql::Request::new(indoc!(
                r#"
                query ($datasetId: DatasetID!) {
                    datasets {
                        byId(datasetId: $datasetId) {
                            asVersionedFile {
                                versions {
                                    nodes {
                                        version
                                        contentHash
                                    }
                                }
                            }
                        }
                    }
                }
                "#
            ))
            .variables(async_graphql::Variables::from_json(json!({
                "datasetId": &did,
            }))),
        )
        .await;

    assert!(res.is_ok(), "{res:#?}");
    let nodes = res.data.into_json().unwrap()["datasets"]["byId"]["asVersionedFile"]["versions"]
        ["nodes"]
        .clone();
    pretty_assertions::assert_eq!(
        nodes,
        json!([
            {
                "version": 2,
                "contentHash": odf::Multihash::from_digest_sha3_256(b"bye"),
            },
            {
                "version": 1,
                "contentHash": odf::Multihash::from_digest_sha3_256(b"hello"),
            }
        ])
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_versioned_file_extra_data() {
    let harness = GraphQLDatasetsHarness::builder()
        .tenancy_config(TenancyConfig::MultiTenant)
        .build()
        .await;

    // Create a versioned file dataset
    let res = harness
        .execute_authorized_query(
            async_graphql::Request::new(indoc!(
                r#"
                mutation ($datasetAlias: DatasetAlias!, $extraColumns: [ColumnInput]) {
                    datasets {
                        createVersionedFile(
                            datasetAlias: $datasetAlias,
                            extraColumns: $extraColumns,
                            datasetVisibility: PUBLIC,
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
                "datasetAlias": "x",
                "extraColumns": [{
                    "name": "foo",
                    "type": {
                        "ddl": "string",
                    }
                }, {
                    "name": "bar",
                    "type": {
                        "ddl": "int",
                    }
                }]
            }))),
        )
        .await;

    assert!(res.is_ok(), "{res:#?}");
    let did = res.data.into_json().unwrap()["datasets"]["createVersionedFile"]["dataset"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Upload the first version
    let res = harness
        .execute_authorized_query(
            async_graphql::Request::new(indoc!(
                r#"
                mutation ($datasetId: DatasetID!, $content: Base64Usnp!, $extraData: ExtraData!) {
                    datasets {
                        byId(datasetId: $datasetId) {
                            asVersionedFile {
                                uploadNewVersion(content: $content, extraData: $extraData) {
                                    isSuccess
                                    message
                                    ... on UpdateVersionSuccess {
                                        newVersion
                                        contentHash
                                        newHead
                                    }
                                }
                            }
                        }
                    }
                }
                "#
            ))
            .variables(async_graphql::Variables::from_json(json!({
                "datasetId": &did,
                "content": base64usnp_encode(b"hello"),
                "extraData": {
                    "foo": "extra",
                    "bar": 123,
                }
            }))),
        )
        .await;

    assert!(res.is_ok(), "{res:#?}");
    let upload_result = res.data.into_json().unwrap()["datasets"]["byId"]["asVersionedFile"]
        ["uploadNewVersion"]
        .clone();
    let head_v1 = &upload_result["newHead"];
    pretty_assertions::assert_eq!(
        upload_result,
        json!({
            "isSuccess": true,
            "message": "",
            "newVersion": 1,
            "contentHash": odf::Multihash::from_digest_sha3_256(b"hello"),
            "newHead": head_v1,
        })
    );

    // Read back the latest content
    let res = harness
        .execute_authorized_query(
            async_graphql::Request::new(indoc!(
                r#"
                query ($datasetId: DatasetID!) {
                    datasets {
                        byId(datasetId: $datasetId) {
                            asVersionedFile {
                                latest {
                                    version
                                    contentType
                                    contentHash
                                    content
                                    extraData
                                }
                            }
                        }
                    }
                }
                "#
            ))
            .variables(async_graphql::Variables::from_json(json!({
                "datasetId": &did,
            }))),
        )
        .await;

    assert!(res.is_ok(), "{res:#?}");
    let entry =
        res.data.into_json().unwrap()["datasets"]["byId"]["asVersionedFile"]["latest"].clone();
    pretty_assertions::assert_eq!(
        entry,
        json!({
            "version": 1,
            "contentHash": odf::Multihash::from_digest_sha3_256(b"hello"),
            "contentType": "application/octet-stream",
            "content": base64usnp_encode(b"hello"),
            "extraData": {
                "foo": "extra",
                "bar": 123,
            },
        })
    );

    // Update just the extra data
    let res = harness
        .execute_authorized_query(
            async_graphql::Request::new(indoc!(
                r#"
                mutation ($datasetId: DatasetID!, $extraData: ExtraData!) {
                    datasets {
                        byId(datasetId: $datasetId) {
                            asVersionedFile {
                                updateExtraData(extraData: $extraData) {
                                    ... on UpdateVersionSuccess {
                                        newVersion
                                        contentHash
                                        newHead
                                    }
                                }
                            }
                        }
                    }
                }
                "#
            ))
            .variables(async_graphql::Variables::from_json(json!({
                "datasetId": &did,
                "extraData": {
                    "foo": "mega",
                    "bar": 321,
                }
            }))),
        )
        .await;

    assert!(res.is_ok(), "{res:#?}");
    let upload_result = res.data.into_json().unwrap()["datasets"]["byId"]["asVersionedFile"]
        ["updateExtraData"]
        .clone();
    let head_v2 = &upload_result["newHead"];
    pretty_assertions::assert_eq!(
        upload_result,
        json!({
            "newVersion": 2,
            "contentHash": odf::Multihash::from_digest_sha3_256(b"hello"),
            "newHead": head_v2,
        })
    );

    // Read back the latest content
    let res = harness
        .execute_authorized_query(
            async_graphql::Request::new(indoc!(
                r#"
                query ($datasetId: DatasetID!) {
                    datasets {
                        byId(datasetId: $datasetId) {
                            asVersionedFile {
                                latest {
                                    version
                                    contentType
                                    contentHash
                                    content
                                    extraData
                                }
                            }
                        }
                    }
                }
                "#
            ))
            .variables(async_graphql::Variables::from_json(json!({
                "datasetId": &did,
            }))),
        )
        .await;

    assert!(res.is_ok(), "{res:#?}");
    let entry =
        res.data.into_json().unwrap()["datasets"]["byId"]["asVersionedFile"]["latest"].clone();
    pretty_assertions::assert_eq!(
        entry,
        json!({
            "version": 2,
            "contentHash": odf::Multihash::from_digest_sha3_256(b"hello"),
            "contentType": "application/octet-stream",
            "content": base64usnp_encode(b"hello"),
            "extraData": {
                "foo": "mega",
                "bar": 321,
            },
        })
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
            .add::<kamu::SessionContextBuilder>()
            .add::<kamu::ObjectStoreRegistryImpl>()
            .add::<kamu::ObjectStoreBuilderLocalFs>()
            .add_value(FileUploadLimitConfig::new_in_bytes(24))
            .add_value(kamu::EngineConfigDatafusionEmbeddedIngest::default())
            .add::<kamu::EngineProvisionerNull>()
            .add::<kamu::DataFormatRegistryImpl>()
            .add::<kamu::PushIngestPlannerImpl>()
            .add::<kamu::PushIngestExecutorImpl>()
            .add::<kamu::PushIngestDataUseCaseImpl>()
            .add::<UpdateVersionedFileUseCaseImpl>()
            .add::<UploadServiceLocal>()
            .add_value(ServerUrlConfig::new_test(None))
            .add_value(CacheDir::new(cache_dir))
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
        self.execute_query(query, &self.catalog_authorized).await
    }

    pub async fn create_versioned_file(&self, dataset_alias: &odf::DatasetAlias) -> String {
        let res = self
            .execute_authorized_query(
                async_graphql::Request::new(indoc!(
                    r#"
                    mutation ($datasetAlias: DatasetAlias!) {
                        datasets {
                            createVersionedFile(
                                datasetAlias: $datasetAlias,
                                datasetVisibility: PUBLIC,
                            ) {
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
                    "datasetAlias": dataset_alias,
                }))),
            )
            .await;

        assert!(res.is_ok(), "{res:#?}");

        // Return created did
        res.data.into_json().unwrap()["datasets"]["createVersionedFile"]["dataset"]["id"]
            .as_str()
            .unwrap()
            .to_string()
    }

    pub async fn in_band_upload_versioned_file(
        &self,
        dataset_did_str: &str,
        content: &str,
    ) -> async_graphql::Response {
        let res = self
            .execute_authorized_query(
                async_graphql::Request::new(indoc!(
                    r#"
                    mutation ($datasetId: DatasetID!, $content: Base64Usnp!) {
                        datasets {
                            byId(datasetId: $datasetId) {
                                asVersionedFile {
                                    uploadNewVersion(content: $content) {
                                        ... on UpdateVersionSuccess {
                                            newVersion
                                            contentHash
                                            newHead
                                        }
                                    }
                                }
                            }
                        }
                    }
                    "#
                ))
                .variables(async_graphql::Variables::from_json(json!({
                    "datasetId": dataset_did_str,
                    "content": content,
                }))),
            )
            .await;

        assert!(res.is_ok(), "{res:#?}");
        res
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
