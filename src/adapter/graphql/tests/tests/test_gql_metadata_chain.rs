// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use async_graphql::*;
use dill::Component;
use event_bus::EventBus;
use indoc::indoc;
use kamu::testing::MetadataFactory;
use kamu::*;
use kamu_core::*;
use opendatafabric::serde::yaml::YamlMetadataEventSerializer;
use opendatafabric::*;

use crate::utils::{authentication_catalogs, expect_anonymous_access_error};

////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_metadata_chain_events() {
    let tempdir = tempfile::tempdir().unwrap();

    let base_catalog = dill::CatalogBuilder::new()
        .add::<EventBus>()
        .add::<DependencyGraphServiceInMemory>()
        .add_builder(
            DatasetRepositoryLocalFs::builder()
                .with_root(tempdir.path().join("datasets"))
                .with_multi_tenant(false),
        )
        .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
        .add::<auth::AlwaysHappyDatasetActionAuthorizer>()
        .build();

    // Init dataset
    let (_, catalog_authorized) = authentication_catalogs(&base_catalog);
    let dataset_repo = catalog_authorized
        .get_one::<dyn DatasetRepository>()
        .unwrap();
    let create_result = dataset_repo
        .create_dataset_from_seed(
            &"foo".try_into().unwrap(),
            MetadataFactory::seed(DatasetKind::Root).build(),
        )
        .await
        .unwrap();
    create_result
        .dataset
        .commit_event(
            MetadataFactory::set_data_schema().build().into(),
            CommitOpts::default(),
        )
        .await
        .unwrap();
    create_result
        .dataset
        .commit_event(
            MetadataFactory::add_data()
                .some_new_data_with_offset(0, 9)
                .some_new_checkpoint()
                .some_new_watermark()
                .some_new_source_state()
                .build()
                .into(),
            CommitOpts {
                check_object_refs: false, // Cheating a little
                ..CommitOpts::default()
            },
        )
        .await
        .unwrap();

    let request_code = indoc!(
        r#"
        {
            datasets {
                byId (datasetId: "<id>") {
                    metadata {
                        chain {
                            blocks (
                                page: 0,
                                perPage: 10,
                            ) {
                                nodes {
                                    event {
                                        __typename
                                        ... on Seed {
                                            datasetId
                                            datasetKind
                                        }
                                        ... on SetDataSchema {
                                            schema {
                                                format
                                                content
                                            }
                                        }
                                        ... on AddData {
                                            prevOffset
                                            newData {
                                                offsetInterval {
                                                    start
                                                    end
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        "#
    )
    .replace("<id>", &create_result.dataset_handle.id.to_string());

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(async_graphql::Request::new(request_code.clone()).data(catalog_authorized))
        .await;
    assert!(res.is_ok(), "{res:?}");

    let expected_schema = r#"{"name": "arrow_schema", "type": "struct", "fields": [{"name": "city", "repetition": "REQUIRED", "type": "BYTE_ARRAY", "logicalType": "STRING"}, {"name": "population", "repetition": "REQUIRED", "type": "INT64"}]}"#;

    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "metadata": {
                        "chain": {
                            "blocks": {
                                "nodes": [{
                                    "event": {
                                        "__typename": "AddData",
                                        "prevOffset": null,
                                        "newData": {
                                            "offsetInterval": {
                                                "start": 0,
                                                "end": 9,
                                            }
                                        }
                                    }
                                }, {
                                    "event": {
                                        "__typename": "SetDataSchema",
                                        "schema": {
                                            "format": "PARQUET_JSON",
                                            "content": expected_schema,
                                        }
                                    }
                                }, {
                                    "event": {
                                        "__typename": "Seed",
                                        "datasetId": create_result.dataset_handle.id.to_string(),
                                        "datasetKind": "ROOT",
                                    }
                                }]
                            }
                        }
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn metadata_chain_append_event() {
    let tempdir = tempfile::tempdir().unwrap();

    let base_catalog = dill::CatalogBuilder::new()
        .add::<EventBus>()
        .add::<DependencyGraphServiceInMemory>()
        .add_builder(
            DatasetRepositoryLocalFs::builder()
                .with_root(tempdir.path().join("datasets"))
                .with_multi_tenant(false),
        )
        .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
        .add::<auth::AlwaysHappyDatasetActionAuthorizer>()
        .build();

    let (catalog_anonymous, catalog_authorized) = authentication_catalogs(&base_catalog);

    let dataset_repo = catalog_authorized
        .get_one::<dyn DatasetRepository>()
        .unwrap();
    let create_result = dataset_repo
        .create_dataset_from_snapshot(
            MetadataFactory::dataset_snapshot()
                .name("foo")
                .kind(DatasetKind::Root)
                .build(),
        )
        .await
        .unwrap();

    let event = MetadataFactory::set_polling_source().build();

    let event_yaml = String::from_utf8_lossy(
        &YamlMetadataEventSerializer
            .write_manifest(&MetadataEvent::SetPollingSource(event))
            .unwrap(),
    )
    .to_string();

    let request_code = indoc!(
        r#"
        mutation {
            datasets {
                byId (datasetId: "<id>") {
                    metadata {
                        chain {
                            commitEvent (
                                event: "<content>",
                                eventFormat: YAML,
                            ) {
                                ... on CommitResultSuccess {
                                    oldHead
                                }
                            }
                        }
                    }
                }
            }
        }
        "#
    )
    .replace("<id>", &create_result.dataset_handle.id.to_string())
    .replace("<content>", &event_yaml.escape_default().to_string());

    let schema = kamu_adapter_graphql::schema_quiet();

    let res = schema
        .execute(async_graphql::Request::new(request_code.clone()).data(catalog_anonymous))
        .await;
    expect_anonymous_access_error(res);

    let res = schema
        .execute(async_graphql::Request::new(request_code.clone()).data(catalog_authorized))
        .await;
    assert!(res.is_ok(), "{res:?}");
    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "metadata": {
                        "chain": {
                            "commitEvent": {
                                "oldHead": create_result.head.to_string(),
                            }
                        }
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn metadata_update_readme_new() {
    let tempdir = tempfile::tempdir().unwrap();

    let base_catalog = dill::CatalogBuilder::new()
        .add::<EventBus>()
        .add::<DependencyGraphServiceInMemory>()
        .add_builder(
            DatasetRepositoryLocalFs::builder()
                .with_root(tempdir.path().join("datasets"))
                .with_multi_tenant(false),
        )
        .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
        .add::<auth::AlwaysHappyDatasetActionAuthorizer>()
        .build();

    let (catalog_anonymous, catalog_authorized) = authentication_catalogs(&base_catalog);

    let dataset_repo = catalog_authorized
        .get_one::<dyn DatasetRepository>()
        .unwrap();
    let create_result = dataset_repo
        .create_dataset_from_snapshot(
            MetadataFactory::dataset_snapshot()
                .name("foo")
                .kind(DatasetKind::Root)
                .build(),
        )
        .await
        .unwrap();

    let dataset = create_result.dataset.clone();

    let schema = kamu_adapter_graphql::schema_quiet();

    /////////////////////////////////////
    // Add new readme
    /////////////////////////////////////

    let new_readme_request_code = indoc!(
        r#"
        mutation {
            datasets {
                byId (datasetId: "<id>") {
                    metadata {
                        updateReadme(content: "new readme") {
                            __typename
                        }
                    }
                }
            }
        }
        "#
    )
    .replace("<id>", &create_result.dataset_handle.id.to_string());

    let res = schema
        .execute(
            async_graphql::Request::new(new_readme_request_code.clone()).data(catalog_anonymous),
        )
        .await;
    expect_anonymous_access_error(res);

    let res = schema
        .execute(
            async_graphql::Request::new(new_readme_request_code).data(catalog_authorized.clone()),
        )
        .await;

    let assert_result = |res: async_graphql::Response, expected: &str| {
        assert!(res.is_ok(), "{res:?}");
        assert_eq!(
            res.data,
            value!({
                "datasets": {
                    "byId": {
                        "metadata": {
                            "updateReadme": {
                                "__typename": expected.to_string(),
                            }
                        }
                    }
                }
            })
        );
    };

    assert_result(res, "CommitResultSuccess");

    assert_attachments_eq(
        dataset.clone(),
        SetAttachments {
            attachments: Attachments::Embedded(AttachmentsEmbedded {
                items: vec![AttachmentEmbedded {
                    path: "README.md".to_string(),
                    content: "new readme".to_string(),
                }],
            }),
        },
    )
    .await;

    /////////////////////////////////////
    // Removes readme
    /////////////////////////////////////

    let res = schema
        .execute(
            async_graphql::Request::new(
                indoc!(
                    r#"
                    mutation {
                        datasets {
                            byId (datasetId: "<id>") {
                                metadata {
                                    updateReadme(content: null) {
                                        __typename
                                    }
                                }
                            }
                        }
                    }
                    "#
                )
                .replace("<id>", &create_result.dataset_handle.id.to_string()),
            )
            .data(catalog_authorized.clone()),
        )
        .await;

    assert_result(res, "CommitResultSuccess");

    assert_attachments_eq(
        dataset.clone(),
        SetAttachments {
            attachments: Attachments::Embedded(AttachmentsEmbedded { items: vec![] }),
        },
    )
    .await;

    /////////////////////////////////////
    // Detects no-op changes
    /////////////////////////////////////

    let res = schema
        .execute(
            async_graphql::Request::new(
                indoc!(
                    r#"
                    mutation {
                        datasets {
                            byId (datasetId: "<id>") {
                                metadata {
                                    updateReadme(content: null) {
                                        __typename
                                    }
                                }
                            }
                        }
                    }
                    "#
                )
                .replace("<id>", &create_result.dataset_handle.id.to_string()),
            )
            .data(catalog_authorized.clone()),
        )
        .await;

    assert_result(res, "NoChanges");

    /////////////////////////////////////
    // Preserves other attachments
    /////////////////////////////////////

    create_result
        .dataset
        .commit_event(
            SetAttachments {
                attachments: Attachments::Embedded(AttachmentsEmbedded {
                    items: vec![
                        AttachmentEmbedded {
                            path: "LICENSE.md".to_string(),
                            content: "my license".to_string(),
                        },
                        AttachmentEmbedded {
                            path: "README.md".to_string(),
                            content: "my readme".to_string(),
                        },
                    ],
                }),
            }
            .into(),
            CommitOpts::default(),
        )
        .await
        .unwrap();

    let res = schema
        .execute(
            async_graphql::Request::new(
                indoc!(
                    r#"
                    mutation {
                        datasets {
                            byId (datasetId: "<id>") {
                                metadata {
                                    updateReadme(content: "new readme") {
                                        __typename
                                    }
                                }
                            }
                        }
                    }
                    "#
                )
                .replace("<id>", &create_result.dataset_handle.id.to_string()),
            )
            .data(catalog_authorized),
        )
        .await;

    assert_result(res, "CommitResultSuccess");

    assert_attachments_eq(
        dataset.clone(),
        SetAttachments {
            attachments: Attachments::Embedded(AttachmentsEmbedded {
                items: vec![
                    AttachmentEmbedded {
                        path: "LICENSE.md".to_string(),
                        content: "my license".to_string(),
                    },
                    AttachmentEmbedded {
                        path: "README.md".to_string(),
                        content: "new readme".to_string(),
                    },
                ],
            }),
        },
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////

async fn assert_attachments_eq(dataset: Arc<dyn Dataset>, expected: SetAttachments) {
    let actual = dataset
        .as_metadata_chain()
        .iter_blocks()
        .try_first()
        .await
        .unwrap()
        .unwrap()
        .1
        .event
        .into_variant::<SetAttachments>()
        .unwrap();

    assert_eq!(actual, expected);
}

////////////////////////////////////////////////////////////////////////////////////////
