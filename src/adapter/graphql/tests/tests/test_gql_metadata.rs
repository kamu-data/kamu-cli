// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::value;
use indoc::indoc;
use kamu::*;
use kamu_accounts::{
    AuthenticationService,
    CurrentAccountSubject,
    DEFAULT_ACCOUNT_NAME,
    MockAuthenticationService,
};
use kamu_core::*;
use kamu_datasets::*;
use kamu_datasets_services::GetDatasetDownstreamDependenciesUseCaseImpl;
use messaging_outbox::OutboxProvider;
use odf::metadata::testing::MetadataFactory;
use pretty_assertions::assert_eq;
use serde_json::json;

use crate::utils::{
    AuthenticationCatalogsResult,
    BaseGQLDatasetHarness,
    PredefinedAccountOpts,
    authentication_catalogs_ext,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_current_push_sources() {
    let harness = DatasetMetadataHarness::new().await;
    let create_result = harness.create_root_dataset().await;

    let res = harness
        .execute_authorized_query(DatasetMetadataHarness::get_dataset_push_sources_request(
            &create_result.dataset_handle.id.to_string(),
        ))
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
                    schema: Some(odf::schema::DataSchema::new(vec![
                        odf::schema::DataField::string("foo"),
                    ])),
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
                    schema: Some(odf::schema::DataSchema::new(vec![
                        odf::schema::DataField::string("foo"),
                    ])),
                    ..Default::default()
                })
                .build()
                .into(),
            odf::dataset::CommitOpts::default(),
        )
        .await
        .unwrap();

    let res = harness
        .execute_authorized_query(DatasetMetadataHarness::get_dataset_push_sources_request(
            &create_result.dataset_handle.id.to_string(),
        ))
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

    let res = harness
        .execute_authorized_query(DatasetMetadataHarness::get_dataset_metadata_blocks_request(
            &create_result.dataset_handle.id.to_string(),
            &["ADD_PUSH_SOURCE"],
            None,
        ))
        .await;
    assert!(res.is_ok(), "{res:?}");

    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "metadata": {
                        "metadataProjection": []
                    },
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
                    schema: Some(odf::schema::DataSchema::new(vec![
                        odf::schema::DataField::string("foo"),
                    ])),
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
                    schema: Some(odf::schema::DataSchema::new(vec![
                        odf::schema::DataField::string("foo"),
                    ])),
                    ..Default::default()
                })
                .build()
                .into(),
            odf::dataset::CommitOpts::default(),
        )
        .await
        .unwrap();

    let res = harness
        .execute_authorized_query(DatasetMetadataHarness::get_dataset_metadata_blocks_request(
            &create_result.dataset_handle.id.to_string(),
            &["ADD_PUSH_SOURCE"],
            None,
        ))
        .await;
    assert!(res.is_ok(), "{res:?}");

    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "metadata": {
                        "metadataProjection": [
                            {
                                "__typename": "MetadataBlockExtended",
                                "event": {
                                    "__typename": "AddPushSource",
                                    "sourceName": "source1",
                                    "read": {
                                        "__typename": "ReadStepCsv",
                                    }
                                }

                            },
                            {
                                "__typename": "MetadataBlockExtended",
                                "event": {
                                    "__typename": "AddPushSource",
                                    "sourceName": "source2",
                                    "read": {
                                        "__typename": "ReadStepNdJson",
                                    }
                                }
                            }
                        ]
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
                    schema: Some(odf::schema::DataSchema::new(vec![
                        odf::schema::DataField::string("foo"),
                    ])),
                    ..Default::default()
                })
                .build()
                .into(),
            odf::dataset::CommitOpts::default(),
        )
        .await
        .unwrap();

    let res = harness
        .execute_authorized_query(
            DatasetMetadataHarness::get_dataset_polling_source_metadata_request(
                create_result.dataset_handle.id.to_string().as_str(),
            ),
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
                    schema: Some(odf::schema::DataSchema::new(vec![
                        odf::schema::DataField::string("foo"),
                    ])),
                    ..Default::default()
                })
                .build()
                .into(),
            odf::dataset::CommitOpts::default(),
        )
        .await
        .unwrap();

    let res = harness
        .execute_authorized_query(DatasetMetadataHarness::get_dataset_metadata_blocks_request(
            create_result.dataset_handle.id.to_string().as_str(),
            &["SET_POLLING_SOURCE"],
            None,
        ))
        .await;

    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "metadata": {
                        "metadataProjection": [{
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

    let res = harness
        .execute_authorized_query(
            DatasetMetadataHarness::get_dataset_set_transform_metadata_request(
                create_derived_result.dataset_handle.id.to_string().as_str(),
            ),
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

    let res = harness
        .execute_authorized_query(DatasetMetadataHarness::get_dataset_metadata_blocks_request(
            create_derived_result.dataset_handle.id.to_string().as_str(),
            &["SET_TRANSFORM"],
            None,
        ))
        .await;

    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "metadata": {
                        "metadataProjection": [{
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

#[test_log::test(tokio::test)]
async fn test_multiple_metadata_block() {
    let harness = DatasetMetadataHarness::new().await;

    let create_result = harness.create_root_dataset().await;

    // Add two push sources
    create_result
        .dataset
        .commit_event(
            MetadataFactory::add_push_source()
                .source_name("source1")
                .read(odf::metadata::ReadStepCsv {
                    schema: Some(odf::schema::DataSchema::new(vec![
                        odf::schema::DataField::string("foo"),
                    ])),
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
                    schema: Some(odf::schema::DataSchema::new(vec![
                        odf::schema::DataField::string("foo"),
                    ])),
                    ..Default::default()
                })
                .build()
                .into(),
            odf::dataset::CommitOpts::default(),
        )
        .await
        .unwrap();

    let res = harness
        .execute_authorized_query(DatasetMetadataHarness::get_dataset_metadata_blocks_request(
            create_result.dataset_handle.id.to_string().as_str(),
            &["SEED", "ADD_PUSH_SOURCE"],
            None,
        ))
        .await;

    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "metadata": {
                        "metadataProjection": [
                            {
                                "__typename": "MetadataBlockExtended",
                                "event": {
                                    "__typename": "Seed",
                                    "datasetId": create_result.dataset_handle.id.to_string(),
                                }
                            },
                            {
                                "__typename": "MetadataBlockExtended",
                                "event": {
                                    "__typename": "AddPushSource",
                                    "sourceName": "source1",
                                    "read": {
                                        "__typename": "ReadStepCsv",
                                    }
                                }
                            },
                            {
                                "__typename": "MetadataBlockExtended",
                                "event": {
                                    "__typename": "AddPushSource",
                                    "sourceName": "source2",
                                    "read": {
                                        "__typename": "ReadStepNdJson",
                                    }
                                }
                            }
                        ]
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_metadata_blocks_in_range() {
    let harness = DatasetMetadataHarness::new().await;

    let create_result = harness.create_root_dataset().await;

    // Add two push sources
    create_result
        .dataset
        .commit_event(
            MetadataFactory::add_push_source()
                .source_name("source1")
                .read(odf::metadata::ReadStepCsv {
                    schema: Some(odf::schema::DataSchema::new(vec![
                        odf::schema::DataField::string("foo"),
                    ])),
                    ..Default::default()
                })
                .build()
                .into(),
            odf::dataset::CommitOpts::default(),
        )
        .await
        .unwrap();
    let last_commit_result = create_result
        .dataset
        .commit_event(
            MetadataFactory::add_push_source()
                .source_name("source2")
                .read(odf::metadata::ReadStepNdJson {
                    schema: Some(odf::schema::DataSchema::new(vec![
                        odf::schema::DataField::string("foo"),
                    ])),
                    ..Default::default()
                })
                .build()
                .into(),
            odf::dataset::CommitOpts::default(),
        )
        .await
        .unwrap();

    let res = harness
        .execute_authorized_query(DatasetMetadataHarness::get_dataset_metadata_blocks_request(
            create_result.dataset_handle.id.to_string().as_str(),
            &["SEED", "ADD_PUSH_SOURCE"],
            Some(&last_commit_result.old_head.unwrap().to_string()),
        ))
        .await;

    // It will return seed and only the first push source
    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "metadata": {
                        "metadataProjection": [{
                            "__typename": "MetadataBlockExtended",
                            "event": {
                                "__typename": "Seed",
                                "datasetId": create_result.dataset_handle.id.to_string(),
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
async fn test_current_downstream_dependencies_count_hides_private_from_readers() {
    let harness = DatasetMetadataHarness::new().await;

    let create_root_result = harness
        .create_root_dataset_with_visibility(odf::DatasetVisibility::Public)
        .await;
    harness
        .create_derived_dataset_with_visibility("bar", odf::DatasetVisibility::Public)
        .await;
    harness
        .create_derived_dataset_with_visibility("baz", odf::DatasetVisibility::Private)
        .await;

    let root_id = &create_root_result.dataset_handle.id;

    // Another account sees neither the private downstream nor its count
    assert_eq!(
        harness
            .get_downstream_dependencies(&harness.catalog_other_account, root_id)
            .await,
        (vec!["bar".to_string()], 1)
    );
    assert_eq!(
        harness
            .get_downstream_dependencies(&harness.catalog_authorized, root_id)
            .await,
        (vec!["bar".to_string(), "baz".to_string()], 2)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_current_downstream_dependencies_count_includes_unreadable_for_maintainer() {
    let harness = DatasetMetadataHarness::new().await;

    let create_root_result = harness
        .create_root_dataset_with_visibility(odf::DatasetVisibility::Public)
        .await;
    harness
        .create_derived_dataset_by(
            &harness.catalog_other_account,
            "baz",
            odf::DatasetVisibility::Private,
        )
        .await;

    let root_id = &create_root_result.dataset_handle.id;

    // The root owner maintains it, so the count includes the private downstream
    // of another account, which the owner cannot list
    assert_eq!(
        harness
            .get_downstream_dependencies(&harness.catalog_authorized, root_id)
            .await,
        (vec![], 1)
    );
    // A reader of the root who cannot read the downstream sees no count of it
    assert_eq!(
        harness
            .get_downstream_dependencies(&harness.catalog_third_account, root_id)
            .await,
        (vec![], 0)
    );
    // The downstream owner reads it, so it is both listed and counted
    assert_eq!(
        harness
            .get_downstream_dependencies(&harness.catalog_other_account, root_id)
            .await,
        (vec!["baz".to_string()], 1)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseGQLDatasetHarness, base_gql_harness)]
struct DatasetMetadataHarness {
    base_gql_harness: BaseGQLDatasetHarness,
    catalog_authorized: dill::Catalog,
    catalog_other_account: dill::Catalog,
    catalog_third_account: dill::Catalog,
}

impl DatasetMetadataHarness {
    async fn new() -> Self {
        let base_gql_harness = BaseGQLDatasetHarness::builder()
            .tenancy_config(TenancyConfig::MultiTenant)
            .outbox_provider(OutboxProvider::Immediate {
                force_immediate: true,
            })
            .build();

        let catalog_base = {
            let mut b = dill::CatalogBuilder::new_chained(base_gql_harness.catalog());

            b.add::<MetadataQueryServiceImpl>()
                .add::<GetDatasetDownstreamDependenciesUseCaseImpl>()
                .add::<EngineProvisionerNull>()
                .add::<ObjectStoreRegistryImpl>()
                .add::<DataFormatRegistryImpl>()
                .add_value(MockAuthenticationService::built_in())
                .bind::<dyn AuthenticationService, MockAuthenticationService>();

            b.build()
        };

        let AuthenticationCatalogsResult {
            catalog_no_subject,
            catalog_authorized,
            ..
        } = authentication_catalogs_ext(&catalog_base, None, PredefinedAccountOpts::default())
            .await;

        let catalog_other_account = dill::CatalogBuilder::new_chained(&catalog_no_subject)
            .add_value(CurrentAccountSubject::new_test_with(&"other-account"))
            .build();

        let catalog_third_account = dill::CatalogBuilder::new_chained(&catalog_no_subject)
            .add_value(CurrentAccountSubject::new_test_with(&"third-account"))
            .build();

        Self {
            base_gql_harness,
            catalog_authorized,
            catalog_other_account,
            catalog_third_account,
        }
    }

    pub async fn execute_authorized_query(
        &self,
        query: impl Into<async_graphql::Request>,
    ) -> async_graphql::Response {
        self.execute_query(query, &self.catalog_authorized).await
    }

    async fn create_root_dataset(&self) -> CreateDatasetResult {
        self.create_root_dataset_with_visibility(odf::DatasetVisibility::default())
            .await
    }

    async fn create_root_dataset_with_visibility(
        &self,
        visibility: odf::DatasetVisibility,
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
                        Some(DEFAULT_ACCOUNT_NAME.clone()),
                        odf::DatasetName::new_unchecked("foo"),
                    ))
                    .build(),
                CreateDatasetUseCaseOptions {
                    dataset_visibility: visibility,
                    ..Default::default()
                },
            )
            .await
            .unwrap()
    }

    async fn create_derived_dataset(&self) -> CreateDatasetResult {
        self.create_derived_dataset_with_visibility("bar", odf::DatasetVisibility::default())
            .await
    }

    async fn create_derived_dataset_with_visibility(
        &self,
        name: &str,
        visibility: odf::DatasetVisibility,
    ) -> CreateDatasetResult {
        self.create_derived_dataset_by(&self.catalog_authorized, name, visibility)
            .await
    }

    /// Creates a derivative of `foo` owned by the subject of the given catalog
    async fn create_derived_dataset_by(
        &self,
        catalog: &dill::Catalog,
        name: &str,
        visibility: odf::DatasetVisibility,
    ) -> CreateDatasetResult {
        let owner_name = catalog
            .get_one::<CurrentAccountSubject>()
            .unwrap()
            .account_name()
            .clone();

        let create_dataset_from_snapshot = catalog
            .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
            .unwrap();

        create_dataset_from_snapshot
            .execute(
                MetadataFactory::dataset_snapshot()
                    .name(odf::DatasetAlias::new(
                        Some(owner_name),
                        odf::DatasetName::new_unchecked(name),
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
                CreateDatasetUseCaseOptions {
                    dataset_visibility: visibility,
                    ..Default::default()
                },
            )
            .await
            .unwrap()
    }

    /// Returns sorted names of the listed downstreams and the downstreams count
    async fn get_downstream_dependencies(
        &self,
        catalog: &dill::Catalog,
        dataset_id: &odf::DatasetID,
    ) -> (Vec<String>, u64) {
        let request = async_graphql::Request::new(indoc!(
            r#"
            query ($datasetId: DatasetID!) {
                datasets {
                    byId(datasetId: $datasetId) {
                        metadata {
                            currentDownstreamDependencies {
                                ... on DatasetAccessResultAccessible {
                                    dataset {
                                        name
                                    }
                                }
                            }
                            currentDownstreamDependenciesCount
                        }
                    }
                }
            }
            "#
        ))
        .variables(async_graphql::Variables::from_json(json!({
            "datasetId": dataset_id.to_string(),
        })));

        let res = self.execute_query(request, catalog).await;
        assert!(res.is_ok(), "{res:?}");

        let json = res.data.into_json().unwrap();
        let metadata = &json["datasets"]["byId"]["metadata"];

        let mut names = metadata["currentDownstreamDependencies"]
            .as_array()
            .unwrap()
            .iter()
            .map(|dependency| dependency["dataset"]["name"].as_str().unwrap().to_string())
            .collect::<Vec<_>>();
        names.sort();

        let count = metadata["currentDownstreamDependenciesCount"]
            .as_u64()
            .unwrap();

        (names, count)
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
        event_type: &[&str],
        head: Option<&str>,
    ) -> async_graphql::Request {
        async_graphql::Request::new(indoc!(
            r#"
            query ($datasetId: DatasetID!, $eventTypes: [MetadataEventType!], $head: Multihash) {
                datasets {
                    byId(
                        datasetId: $datasetId
                    ) {
                        metadata {
                            metadataProjection (eventTypes: $eventTypes, head: $head) {
                                __typename
                                event {
                                    __typename
                                    ... on SetPollingSource {
                                        read {
                                            __typename
                                        }
                                    }
                                     ... on Seed {
                                        datasetId
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
            "eventTypes": event_type,
            "head": head
        })))
    }
}
