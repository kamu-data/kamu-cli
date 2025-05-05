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
use kamu_auth_rebac_inmem::InMemoryRebacRepository;
use kamu_auth_rebac_services::RebacServiceImpl;
use kamu_core::*;
use kamu_datasets_services::*;
use serde_json::json;

use crate::utils::{authentication_catalogs, BaseGQLDatasetHarness};

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
    let res = harness
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
                "datasetAlias": "x",
            }))),
        )
        .await;

    assert!(res.is_ok(), "{res:#?}");
    let did = res.data.into_json().unwrap()["datasets"]["createVersionedFile"]["dataset"]["id"]
        .as_str()
        .unwrap()
        .to_string();

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
                "datasetId": &did,
                "content": base64usnp_encode(b"hello"),
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

    // Create versioned file dataset
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

    // Upload first version
    let res = harness
        .execute_authorized_query(
            async_graphql::Request::new(indoc!(
                r#"
                mutation ($datasetId: DatasetID!, $content: Base64Usnp!, $extraData: JSON!) {
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

    // Read back latest content
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
                mutation ($datasetId: DatasetID!, $extraData: JSON!) {
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

    // Read back latest content
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

#[test_log::test(tokio::test)]
async fn test_versioned_file_direct_upload_download() {
    let harness = GraphQLDatasetsHarness::builder()
        .tenancy_config(TenancyConfig::MultiTenant)
        .build()
        .await;

    // Create versioned file dataset
    let res = harness
        .execute_authorized_query(
            async_graphql::Request::new(indoc!(
                r#"
                mutation ($datasetAlias: DatasetAlias!) {
                    datasets {
                        createVersionedFile(
                            datasetAlias: $datasetAlias,
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
            }))),
        )
        .await;

    assert!(res.is_ok(), "{res:#?}");
    let did = res.data.into_json().unwrap()["datasets"]["createVersionedFile"]["dataset"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Initiate direct upload
    let res = harness
        .execute_authorized_query(
            async_graphql::Request::new(indoc!(
                r#"
                mutation ($datasetId: DatasetID!, $contentLength: Int!) {
                    datasets {
                        byId(datasetId: $datasetId) {
                            asVersionedFile {
                                startUploadNewVersion(contentLength: $contentLength) {
                                    isSuccess
                                    message
                                    ... on StartUploadVersionSuccess {
                                        method
                                        url
                                        headers { key value }
                                        useMultipart
                                        uploadToken
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
                "contentLength": b"hello".len(),

            }))),
        )
        .await;

    assert!(res.is_ok(), "{res:#?}");
    let upload_result = res.data.into_json().unwrap()["datasets"]["byId"]["asVersionedFile"]
        ["startUploadNewVersion"]
        .clone();
    let method = upload_result["method"].as_str().unwrap();
    let use_multipart = upload_result["useMultipart"].as_bool().unwrap();
    // let headers = upload_result["headers"].as_array().unwrap();
    // let upload_url = upload_result["url"].as_str().unwrap();
    let upload_token = upload_result["uploadToken"].as_str().unwrap();
    assert_eq!(method, "POST");
    assert!(use_multipart);

    // Use direct service access to simulate upload without spawning HTTP server
    let upload_token: kamu::domain::UploadTokenBase64Json = upload_token.parse().unwrap();

    let upload_service = harness
        .catalog_authorized
        .get_one::<kamu_adapter_http::platform::UploadServiceLocal>()
        .unwrap();

    upload_service
        .save_upload(
            &upload_token.0,
            b"hello".len(),
            bytes::Bytes::from_static(b"hello"),
        )
        .await
        .unwrap();

    // Finalize direct upload
    let res = harness
        .execute_authorized_query(
            async_graphql::Request::new(indoc!(
                r#"
                mutation ($datasetId: DatasetID!, $uploadToken: String!) {
                    datasets {
                        byId(datasetId: $datasetId) {
                            asVersionedFile {
                                finishUploadNewVersion(uploadToken: $uploadToken) {
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
                "uploadToken": upload_token,

            }))),
        )
        .await;

    assert!(res.is_ok(), "{res:#?}");
    let upload_result = res.data.into_json().unwrap()["datasets"]["byId"]["asVersionedFile"]
        ["finishUploadNewVersion"]
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

    // // Read back latest content
    // let res = harness
    //     .execute_authorized_query(
    //         async_graphql::Request::new(indoc!(
    //             r#"
    //             query ($datasetId: DatasetID!) {
    //                 datasets {
    //                     byId(datasetId: $datasetId) {
    //                         asVersionedFile {
    //                             latest {
    //                                 version
    //                                 blockHash
    //                                 contentType
    //                                 contentHash
    //                                 extraData
    //                                 contentDownload {
    //                                     url
    //                                     headers { key value }
    //                                     expiresAt
    //                                 }
    //                             }
    //                         }
    //                     }
    //                 }
    //             }
    //             "#
    //         ))
    //         .variables(async_graphql::Variables::from_json(json!({
    //             "datasetId": &did,
    //         }))),
    //     )
    //     .await;

    // assert!(res.is_ok(), "{res:#?}");
    // let get_content_url =
    // res.data.into_json().unwrap()["datasets"]["byId"]["asVersionedFile"]
    //     ["getContentUrl"]
    //     .clone();
    // let url = get_content_url["url"].as_str().unwrap();
    // let headers = &get_content_url["headers"];
    // let expires_at = get_content_url["expiresAt"].as_str().unwrap();
    // pretty_assertions::assert_eq!(
    //     get_content_url,
    //     json!({
    //         "isSuccess": true,
    //         "message": "",
    //         "version": 2,
    //         "blockHash": head_v1,
    //         "contentHash": odf::Multihash::from_digest_sha3_256(b"hello"),
    //         "contentType": "application/octet-stream",
    //         "extraData": {},
    //         "url": url,
    //         "headers": headers,
    //         "expiresAt": expires_at,
    //     })
    // );

    // assert_eq!(url, "");
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
            .add::<RebacServiceImpl>()
            .add_value(kamu_auth_rebac_services::DefaultAccountProperties { is_admin: false })
            .add_value(kamu_auth_rebac_services::DefaultDatasetProperties {
                allows_anonymous_read: false,
                allows_public_read: false,
            })
            .add::<InMemoryRebacRepository>()
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
            .add::<kamu_adapter_http::platform::UploadServiceLocal>()
            .add_value(kamu::domain::ServerUrlConfig::new_test(None))
            .add_value(kamu::domain::FileUploadLimitConfig::new_in_bytes(100))
            .add_value(kamu::domain::CacheDir::new(cache_dir))
            .build();

        let (_catalog_anonymous, catalog_authorized) = authentication_catalogs(&base_catalog).await;

        // TODO: Yuck
        let catalog_authorized = catalog_authorized
            .builder_chained()
            .add_value(kamu_adapter_http::AccessToken {
                token: "<secret>".into(),
            })
            .build();

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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
