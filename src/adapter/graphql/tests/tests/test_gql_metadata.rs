// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::*;
use indoc::indoc;
use kamu::*;
use kamu_accounts::testing::MockAuthenticationService;
use kamu_accounts::{AuthenticationService, DEFAULT_ACCOUNT_NAME};
use kamu_core::*;
use kamu_datasets::*;
use odf::metadata::testing::MetadataFactory;
use serde_json::json;

use crate::utils::{BaseGQLDatasetHarness, PredefinedAccountOpts, authentication_catalogs};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_current_push_sources() {
    let harness = DatasetMetadataHarness::new().await;
    let create_result = harness.create_root_dataset().await;

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(
            DatasetMetadataHarness::get_dataset_push_sources_request(
                &create_result.dataset_handle.id.to_string(),
            )
            .data(harness.catalog_authorized.clone()),
        )
        .await;
    assert!(res.is_ok(), "{res:?}");

    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "metadata": {
                        "currentPushSources": []
                    }
                }
            }
        })
    );

    // Add two push sources
    create_result
        .dataset
        .commit_event(
            MetadataFactory::add_push_source()
                .source_name("source1")
                .read(odf::metadata::ReadStepCsv {
                    schema: Some(vec!["foo STRING".to_string()]),
                    ..Default::default()
                })
                .build()
                .into(),
            odf::dataset::CommitOpts::default(),
        )
        .await
        .unwrap();
    create_result
        .dataset
        .commit_event(
            MetadataFactory::add_push_source()
                .source_name("source2")
                .read(odf::metadata::ReadStepNdJson {
                    schema: Some(vec!["foo STRING".to_string()]),
                    ..Default::default()
                })
                .build()
                .into(),
            odf::dataset::CommitOpts::default(),
        )
        .await
        .unwrap();

    let res = schema
        .execute(
            DatasetMetadataHarness::get_dataset_push_sources_request(
                &create_result.dataset_handle.id.to_string(),
            )
            .data(harness.catalog_authorized.clone()),
        )
        .await;
    assert!(res.is_ok(), "{res:?}");

    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "metadata": {
                        "currentPushSources": [{
                            "sourceName": "source1",
                            "read": {
                                "__typename": "ReadStepCsv",
                            }
                        }, {
                            "sourceName": "source2",
                            "read": {
                                "__typename": "ReadStepNdJson",
                            }
                         }]
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_current_push_source_blocks() {
    let harness = DatasetMetadataHarness::new().await;
    let create_result = harness.create_root_dataset().await;

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(
            DatasetMetadataHarness::get_dataset_metadata_blocks_request(
                &create_result.dataset_handle.id.to_string(),
                "ADD_PUSH_SOURCE",
            )
            .data(harness.catalog_authorized.clone()),
        )
        .await;
    assert!(res.is_ok(), "{res:?}");

    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "metadata": {
                        "extendedBlocksByEventType": []
                    }
                }
            }
        })
    );

    // Add two push sources
    create_result
        .dataset
        .commit_event(
            MetadataFactory::add_push_source()
                .source_name("source1")
                .read(odf::metadata::ReadStepCsv {
                    schema: Some(vec!["foo STRING".to_string()]),
                    ..Default::default()
                })
                .build()
                .into(),
            odf::dataset::CommitOpts::default(),
        )
        .await
        .unwrap();
    create_result
        .dataset
        .commit_event(
            MetadataFactory::add_push_source()
                .source_name("source2")
                .read(odf::metadata::ReadStepNdJson {
                    schema: Some(vec!["foo STRING".to_string()]),
                    ..Default::default()
                })
                .build()
                .into(),
            odf::dataset::CommitOpts::default(),
        )
        .await
        .unwrap();

    let res = schema
        .execute(
            DatasetMetadataHarness::get_dataset_metadata_blocks_request(
                &create_result.dataset_handle.id.to_string(),
                "ADD_PUSH_SOURCE",
            )
            .data(harness.catalog_authorized.clone()),
        )
        .await;
    assert!(res.is_ok(), "{res:?}");

    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "metadata": {
                        "extendedBlocksByEventType": [ {
                            "__typename": "MetadataBlockExtended",
                            "event": {
                                "__typename": "AddPushSource",
                                "sourceName": "source2",
                                "read": {
                                    "__typename": "ReadStepNdJson",
                                }
                            }
                         }, {
                            "__typename": "MetadataBlockExtended",
                            "event": {
                                "__typename": "AddPushSource",
                                "sourceName": "source1",
                                "read": {
                                    "__typename": "ReadStepCsv",
                                }
                            }
                        }]
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_current_polling_source() {
    let harness = DatasetMetadataHarness::new().await;

    let create_result = harness.create_root_dataset().await;

    // Add polling source
    create_result
        .dataset
        .commit_event(
            MetadataFactory::set_polling_source()
                .read(odf::metadata::ReadStepCsv {
                    schema: Some(vec!["foo STRING".to_string()]),
                    ..Default::default()
                })
                .build()
                .into(),
            odf::dataset::CommitOpts::default(),
        )
        .await
        .unwrap();

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(
            DatasetMetadataHarness::get_dataset_polling_source_metadata_request(
                create_result.dataset_handle.id.to_string().as_str(),
            )
            .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "metadata": {
                        "currentPollingSource": {
                            "read": {
                                "__typename": "ReadStepCsv",
                            },
                        }
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_current_polling_source_block() {
    let harness = DatasetMetadataHarness::new().await;

    let create_result = harness.create_root_dataset().await;

    // Add polling source
    create_result
        .dataset
        .commit_event(
            MetadataFactory::set_polling_source()
                .read(odf::metadata::ReadStepCsv {
                    schema: Some(vec!["foo STRING".to_string()]),
                    ..Default::default()
                })
                .build()
                .into(),
            odf::dataset::CommitOpts::default(),
        )
        .await
        .unwrap();

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(
            DatasetMetadataHarness::get_dataset_metadata_blocks_request(
                create_result.dataset_handle.id.to_string().as_str(),
                "SET_POLLING_SOURCE",
            )
            .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "metadata": {
                        "extendedBlocksByEventType": [{
                            "__typename": "MetadataBlockExtended",
                            "event": {
                                "__typename": "SetPollingSource",
                                "read": {
                                    "__typename": "ReadStepCsv",
                                },
                            }
                        }]
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_current_set_transform() {
    let harness = DatasetMetadataHarness::new().await;

    let create_root_result = harness.create_root_dataset().await;
    let create_derived_result = harness.create_derived_dataset().await;

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(
            DatasetMetadataHarness::get_dataset_set_transform_metadata_request(
                create_derived_result.dataset_handle.id.to_string().as_str(),
            )
            .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "metadata": {
                        "currentTransform": {
                            "inputs": [{
                                "datasetRef": create_root_result.dataset_handle.id.to_string(),
                                "alias": create_root_result.dataset_handle.alias.to_string(),
                            }],
                        }
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_current_set_transform_block() {
    let harness = DatasetMetadataHarness::new().await;

    let create_root_result = harness.create_root_dataset().await;
    let create_derived_result = harness.create_derived_dataset().await;

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(
            DatasetMetadataHarness::get_dataset_metadata_blocks_request(
                create_derived_result.dataset_handle.id.to_string().as_str(),
                "SET_TRANSFORM",
            )
            .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "metadata": {
                        "extendedBlocksByEventType": [{
                            "__typename": "MetadataBlockExtended",
                            "event": {
                                "__typename": "SetTransform",
                                "inputs": [{
                                    "datasetRef": create_root_result.dataset_handle.id.to_string(),
                                    "alias": create_root_result.dataset_handle.alias.to_string(),
                                }],
                            }
                        }]
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseGQLDatasetHarness, base_gql_harness)]
struct DatasetMetadataHarness {
    base_gql_harness: BaseGQLDatasetHarness,
    catalog_authorized: dill::Catalog,
}

impl DatasetMetadataHarness {
    async fn new() -> Self {
        let base_gql_harness = BaseGQLDatasetHarness::builder()
            .tenancy_config(TenancyConfig::MultiTenant)
            .build();

        let catalog_base = {
            let mut b = dill::CatalogBuilder::new_chained(base_gql_harness.catalog());

            b.add::<MetadataQueryServiceImpl>()
                .add::<EngineProvisionerNull>()
                .add::<ObjectStoreRegistryImpl>()
                .add::<DataFormatRegistryImpl>()
                .add_value(MockAuthenticationService::built_in())
                .bind::<dyn AuthenticationService, MockAuthenticationService>();

            b.build()
        };

        let (_, catalog_authorized) =
            authentication_catalogs(&catalog_base, PredefinedAccountOpts::default()).await;

        Self {
            base_gql_harness,
            catalog_authorized,
        }
    }

    async fn create_root_dataset(&self) -> CreateDatasetResult {
        let create_dataset_from_snapshot = self
            .catalog_authorized
            .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
            .unwrap();

        create_dataset_from_snapshot
            .execute(
                MetadataFactory::dataset_snapshot()
                    .kind(odf::DatasetKind::Root)
                    .name(odf::DatasetAlias::new(
                        Some(DEFAULT_ACCOUNT_NAME.clone()),
                        odf::DatasetName::new_unchecked("foo"),
                    ))
                    .build(),
                Default::default(),
            )
            .await
            .unwrap()
    }

    async fn create_derived_dataset(&self) -> CreateDatasetResult {
        let create_dataset_from_snapshot = self
            .catalog_authorized
            .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
            .unwrap();

        create_dataset_from_snapshot
            .execute(
                MetadataFactory::dataset_snapshot()
                    .name(odf::DatasetAlias::new(
                        Some(DEFAULT_ACCOUNT_NAME.clone()),
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

    fn get_dataset_set_transform_metadata_request(dataset_id: &str) -> async_graphql::Request {
        async_graphql::Request::new(indoc!(
            r#"
                    query ($datasetId: DatasetID!) {
                        datasets {
                            byId(
                                datasetId: $datasetId
                            ) {
                                metadata {
                                    currentTransform {
                                        inputs {
                                            datasetRef
                                            alias
                                        }
                                    }
                                }
                            }
                        }
                    }
                    "#
        ))
        .variables(async_graphql::Variables::from_json(json!({
            "datasetId": dataset_id,
        })))
    }

    fn get_dataset_push_sources_request(dataset_id: &str) -> async_graphql::Request {
        async_graphql::Request::new(indoc!(
            r#"
            query ($datasetId: DatasetID!) {
                datasets {
                    byId(
                        datasetId: $datasetId
                    ) {
                        metadata {
                            currentPushSources {
                                sourceName
                                read {
                                    __typename
                                }
                            }
                    }
                    }
                }
            }
            "#
        ))
        .variables(async_graphql::Variables::from_json(json!({
            "datasetId": dataset_id,
        })))
    }

    fn get_dataset_polling_source_metadata_request(dataset_id: &str) -> async_graphql::Request {
        async_graphql::Request::new(indoc!(
            r#"
            query ($datasetId: DatasetID!) {
                datasets {
                    byId(
                        datasetId: $datasetId
                    ) {
                        metadata {
                            currentPollingSource {
                                read {
                                    __typename
                                }
                            }
                        }
                    }
                }
            }
            "#
        ))
        .variables(async_graphql::Variables::from_json(json!({
            "datasetId": dataset_id,
        })))
    }

    fn get_dataset_metadata_blocks_request(
        dataset_id: &str,
        event_type: &str,
    ) -> async_graphql::Request {
        async_graphql::Request::new(indoc!(
            r#"
            query ($datasetId: DatasetID!, $eventType: MetadataEventType!) {
                datasets {
                    byId(
                        datasetId: $datasetId
                    ) {
                        metadata {
                            extendedBlocksByEventType (eventType: $eventType) {
                                __typename
                                event {
                                    __typename
                                    ... on SetPollingSource {
                                        read {
                                            __typename
                                        }
                                    }
                                    ... on SetTransform {
                                        inputs {
                                            datasetRef
                                            alias
                                        }
                                    }
                                    ...on AddPushSource {
                                        sourceName
                                        read {
                                            __typename
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
            "datasetId": dataset_id,
            "eventType": event_type,
        })))
    }
}
