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
use indoc::indoc;
use kamu::testing::MetadataFactory;
use kamu::*;
use kamu_adapter_graphql::AccessToken;
use kamu_core::*;
use opendatafabric::serde::yaml::YamlMetadataEventSerializer;
use opendatafabric::*;

#[test_log::test(tokio::test)]
async fn metadata_chain_append_event() {
    let tempdir = tempfile::tempdir().unwrap();

    let cat = dill::CatalogBuilder::new()
        .add_builder(
            dill::builder_for::<DatasetRepositoryLocalFs>()
                .with_root(tempdir.path().join("datasets"))
                .with_current_account_subject(Arc::new(CurrentAccountSubject::new_test()))
                .with_multi_tenant(false),
        )
        .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
        .add::<auth::AlwaysHappyDatasetActionAuthorizer>()
        .bind::<dyn auth::DatasetActionAuthorizer, auth::AlwaysHappyDatasetActionAuthorizer>()
        .add_value(kamu::testing::MockAuthenticationService::built_in())
        .bind::<dyn auth::AuthenticationService, kamu::testing::MockAuthenticationService>()
        .build();

    let dataset_repo = cat.get_one::<dyn DatasetRepository>().unwrap();
    let create_result = dataset_repo
        .create_dataset_from_snapshot(
            None,
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

    let authentication_svc = cat.get_one::<dyn auth::AuthenticationService>().unwrap();
    let access_token = authentication_svc
        .login("test", String::from("<dummy>"))
        .await
        .unwrap()
        .access_token;

    let schema = kamu_adapter_graphql::schema();
    let res = kamu_adapter_graphql::execute_query(
        schema,
        cat,
        Some(AccessToken::new(access_token)),
        indoc!(
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
        .replace("<content>", &event_yaml.escape_default().to_string()),
    )
    .await;
    assert!(res.is_ok(), "{:?}", res);
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

#[test_log::test(tokio::test)]
async fn metadata_update_readme_new() {
    let tempdir = tempfile::tempdir().unwrap();

    let cat = dill::CatalogBuilder::new()
        .add_builder(
            dill::builder_for::<DatasetRepositoryLocalFs>()
                .with_root(tempdir.path().join("datasets"))
                .with_current_account_subject(Arc::new(CurrentAccountSubject::new_test()))
                .with_multi_tenant(false),
        )
        .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
        .add::<auth::AlwaysHappyDatasetActionAuthorizer>()
        .bind::<dyn auth::DatasetActionAuthorizer, auth::AlwaysHappyDatasetActionAuthorizer>()
        .add_value(kamu::testing::MockAuthenticationService::built_in())
        .bind::<dyn auth::AuthenticationService, kamu::testing::MockAuthenticationService>()
        .build();

    let dataset_repo = cat.get_one::<dyn DatasetRepository>().unwrap();
    let create_result = dataset_repo
        .create_dataset_from_snapshot(
            None,
            MetadataFactory::dataset_snapshot()
                .name("foo")
                .kind(DatasetKind::Root)
                .build(),
        )
        .await
        .unwrap();

    let dataset = create_result.dataset.clone();

    let authentication_svc = cat.get_one::<dyn auth::AuthenticationService>().unwrap();
    let access_token = authentication_svc
        .login("test", String::from("<dummy>"))
        .await
        .unwrap()
        .access_token;

    let schema = kamu_adapter_graphql::schema();

    /////////////////////////////////////
    // Add new readme
    /////////////////////////////////////

    let res = kamu_adapter_graphql::execute_query(
        schema.clone(),
        cat.clone(),
        Some(AccessToken::new(access_token.clone())),
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
    .await;

    let assert_result = |res: async_graphql::Response, expected: &str| {
        assert!(res.is_ok(), "{:?}", res);
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

    let res = kamu_adapter_graphql::execute_query(
        schema.clone(),
        cat.clone(),
        Some(AccessToken::new(access_token.clone())),
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

    let res = kamu_adapter_graphql::execute_query(
        schema.clone(),
        cat.clone(),
        Some(AccessToken::new(access_token.clone())),
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

    let res = kamu_adapter_graphql::execute_query(
        schema,
        cat,
        Some(AccessToken::new(access_token)),
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
