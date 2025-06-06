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
use chrono::Utc;
use indoc::indoc;
use kamu_accounts::DEFAULT_ACCOUNT_NAME;
use kamu_core::*;
use kamu_datasets::*;
use kamu_datasets_services::*;
use odf::metadata::testing::MetadataFactory;

use crate::utils::{
    BaseGQLDatasetHarness,
    PredefinedAccountOpts,
    authentication_catalogs,
    expect_anonymous_access_error,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_metadata_chain_events() {
    let harness = GraphQLMetadataChainHarness::new(TenancyConfig::SingleTenant).await;

    // Prepare phase
    let dataset_id = {
        let create_dataset = harness
            .catalog_authorized
            .get_one::<dyn CreateDatasetUseCase>()
            .unwrap();

        let create_result = create_dataset
            .execute(
                &"foo".try_into().unwrap(),
                odf::dataset::make_seed_block(
                    harness.did_generator.generate_dataset_id().0,
                    odf::DatasetKind::Root,
                    Utc::now(),
                ),
                Default::default(),
            )
            .await
            .unwrap();

        create_result
            .dataset
            .commit_event(
                MetadataFactory::set_data_schema().build().into(),
                odf::dataset::CommitOpts::default(),
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
                odf::dataset::CommitOpts {
                    check_object_refs: false, // Cheating a little
                    ..odf::dataset::CommitOpts::default()
                },
            )
            .await
            .unwrap();

        create_result.dataset_handle.id
    };

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
    .replace("<id>", &dataset_id.to_string());

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(async_graphql::Request::new(request_code.clone()).data(harness.catalog_authorized))
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
                                        "datasetId": dataset_id.to_string(),
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn metadata_chain_append_event() {
    let harness = GraphQLMetadataChainHarness::new(TenancyConfig::SingleTenant).await;

    let create_dataset = harness
        .catalog_authorized
        .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
        .unwrap();

    let (dataset_id, original_head) = {
        let create_dataset_result = create_dataset
            .execute(
                MetadataFactory::dataset_snapshot()
                    .name("foo")
                    .kind(odf::DatasetKind::Root)
                    .build(),
                Default::default(),
            )
            .await
            .unwrap();

        (
            create_dataset_result.dataset_handle.id,
            create_dataset_result.head,
        )
    };

    let event = MetadataFactory::set_polling_source().build();

    use odf::metadata::serde::yaml::YamlMetadataEventSerializer;
    let event_yaml = String::from_utf8_lossy(
        &YamlMetadataEventSerializer
            .write_manifest(&odf::MetadataEvent::SetPollingSource(event))
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
    .replace("<id>", &dataset_id.to_string())
    .replace("<content>", &event_yaml.escape_default().to_string());

    let schema = kamu_adapter_graphql::schema_quiet();

    let res = schema
        .execute(async_graphql::Request::new(request_code.clone()).data(harness.catalog_anonymous))
        .await;
    expect_anonymous_access_error(res);

    let res = schema
        .execute(async_graphql::Request::new(request_code.clone()).data(harness.catalog_authorized))
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
                                "oldHead": original_head.to_string(),
                            }
                        }
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn metadata_update_readme_new() {
    let harness = GraphQLMetadataChainHarness::new(TenancyConfig::SingleTenant).await;

    let create_dataset = harness
        .catalog_authorized
        .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
        .unwrap();

    let foo_handle = create_dataset
        .execute(
            MetadataFactory::dataset_snapshot()
                .name("foo")
                .kind(odf::DatasetKind::Root)
                .build(),
            Default::default(),
        )
        .await
        .unwrap()
        .dataset_handle;

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
    .replace("<id>", &foo_handle.id.to_string());

    let res = schema
        .execute(
            async_graphql::Request::new(new_readme_request_code.clone())
                .data(harness.catalog_anonymous.clone()),
        )
        .await;
    expect_anonymous_access_error(res);

    let res = schema
        .execute(
            async_graphql::Request::new(new_readme_request_code)
                .data(harness.catalog_authorized.clone()),
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
        harness.get_dataset(&foo_handle).await,
        odf::metadata::SetAttachments {
            attachments: odf::metadata::Attachments::Embedded(odf::metadata::AttachmentsEmbedded {
                items: vec![odf::metadata::AttachmentEmbedded {
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
                .replace("<id>", &foo_handle.id.to_string()),
            )
            .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert_result(res, "CommitResultSuccess");

    assert_attachments_eq(
        harness.get_dataset(&foo_handle).await,
        odf::metadata::SetAttachments {
            attachments: odf::metadata::Attachments::Embedded(odf::metadata::AttachmentsEmbedded {
                items: vec![],
            }),
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
                .replace("<id>", &foo_handle.id.to_string()),
            )
            .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert_result(res, "NoChanges");

    /////////////////////////////////////
    // Preserves other attachments
    /////////////////////////////////////

    harness
        .get_dataset(&foo_handle)
        .await
        .commit_event(
            odf::metadata::SetAttachments {
                attachments: odf::metadata::Attachments::Embedded(
                    odf::metadata::AttachmentsEmbedded {
                        items: vec![
                            odf::metadata::AttachmentEmbedded {
                                path: "LICENSE.md".to_string(),
                                content: "my license".to_string(),
                            },
                            odf::metadata::AttachmentEmbedded {
                                path: "README.md".to_string(),
                                content: "my readme".to_string(),
                            },
                        ],
                    },
                ),
            }
            .into(),
            odf::dataset::CommitOpts::default(),
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
                .replace("<id>", &foo_handle.id.to_string()),
            )
            .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert_result(res, "CommitResultSuccess");

    assert_attachments_eq(
        harness.get_dataset(&foo_handle).await,
        odf::metadata::SetAttachments {
            attachments: odf::metadata::Attachments::Embedded(odf::metadata::AttachmentsEmbedded {
                items: vec![
                    odf::metadata::AttachmentEmbedded {
                        path: "LICENSE.md".to_string(),
                        content: "my license".to_string(),
                    },
                    odf::metadata::AttachmentEmbedded {
                        path: "README.md".to_string(),
                        content: "new readme".to_string(),
                    },
                ],
            }),
        },
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn assert_attachments_eq(dataset: ResolvedDataset, expected: odf::metadata::SetAttachments) {
    use odf::dataset::{MetadataChainExt as _, TryStreamExtExt as _};
    use odf::metadata::EnumWithVariants;

    let actual = dataset
        .as_metadata_chain()
        .iter_blocks()
        .try_first()
        .await
        .unwrap()
        .unwrap()
        .1
        .event
        .into_variant::<odf::metadata::SetAttachments>()
        .unwrap();

    assert_eq!(actual, expected);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_metadata_chain_set_transform_event() {
    let harness = GraphQLMetadataChainHarness::new(TenancyConfig::MultiTenant).await;

    let root_id = harness
        .create_root_dataset(Some(DEFAULT_ACCOUNT_NAME.clone()))
        .await
        .dataset_handle
        .id;

    let derived_id = harness
        .create_derived_dataset(Some(DEFAULT_ACCOUNT_NAME.clone()))
        .await
        .dataset_handle
        .id;

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
                                        ... on SetTransform {
                                            inputs {
                                                datasetRef
                                                alias
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
    .replace("<id>", &derived_id.to_string());

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(async_graphql::Request::new(request_code.clone()).data(harness.catalog_authorized))
        .await;
    assert!(res.is_ok(), "{res:?}");
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
                                        "__typename": "SetTransform",
                                        "inputs": [{
                                            "datasetRef": root_id.to_string(),
                                            "alias": "kamu/foo",
                                        }]
                                    }
                                }, {
                                    "event": {
                                        "__typename": "Seed",
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseGQLDatasetHarness, base_gql_harness)]
struct GraphQLMetadataChainHarness {
    base_gql_harness: BaseGQLDatasetHarness,
    catalog_anonymous: dill::Catalog,
    catalog_authorized: dill::Catalog,
    did_generator: Arc<dyn DidGenerator>,
}

impl GraphQLMetadataChainHarness {
    async fn new(tenancy_config: TenancyConfig) -> Self {
        let base_gql_harness = BaseGQLDatasetHarness::builder()
            .tenancy_config(tenancy_config)
            .build();

        let base_catalog = {
            let mut b = dill::CatalogBuilder::new_chained(base_gql_harness.catalog());

            b.add::<CommitDatasetEventUseCaseImpl>();

            b.build()
        };

        let (catalog_anonymous, catalog_authorized) =
            authentication_catalogs(&base_catalog, PredefinedAccountOpts::default()).await;

        let did_generator = base_catalog.get_one::<dyn DidGenerator>().unwrap();

        Self {
            base_gql_harness,
            catalog_anonymous,
            catalog_authorized,
            did_generator,
        }
    }

    async fn create_root_dataset(
        &self,
        account_name: Option<odf::AccountName>,
    ) -> CreateDatasetResult {
        let create_dataset_from_snapshot = self
            .catalog_authorized
            .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
            .unwrap();

        create_dataset_from_snapshot
            .execute(
                MetadataFactory::dataset_snapshot()
                    .kind(odf::DatasetKind::Root)
                    .name(odf::DatasetAlias::new(
                        account_name,
                        odf::DatasetName::new_unchecked("foo"),
                    ))
                    .build(),
                Default::default(),
            )
            .await
            .unwrap()
    }

    async fn create_derived_dataset(
        &self,
        account_name: Option<odf::AccountName>,
    ) -> CreateDatasetResult {
        let create_dataset_from_snapshot = self
            .catalog_authorized
            .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
            .unwrap();

        create_dataset_from_snapshot
            .execute(
                MetadataFactory::dataset_snapshot()
                    .name(odf::DatasetAlias::new(
                        account_name,
                        odf::DatasetName::new_unchecked("bar"),
                    ))
                    .kind(odf::DatasetKind::Derivative)
                    .push_event(
                        MetadataFactory::set_transform()
                            .inputs_from_refs([odf::DatasetAlias::new(
                                Some(DEFAULT_ACCOUNT_NAME.clone()),
                                odf::DatasetName::new_unchecked("foo"),
                            )])
                            .build(),
                    )
                    .build(),
                Default::default(),
            )
            .await
            .unwrap()
    }

    pub async fn get_dataset(&self, hdl: &odf::DatasetHandle) -> ResolvedDataset {
        let dataset_registry = self
            .catalog_authorized
            .get_one::<dyn DatasetRegistry>()
            .unwrap();
        dataset_registry.get_dataset_by_handle(hdl).await
    }
}
