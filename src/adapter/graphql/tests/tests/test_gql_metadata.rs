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
use database_common::NoOpDatabasePlugin;
use dill::*;
use indoc::indoc;
use internal_error::InternalError;
use kamu::testing::MetadataFactory;
use kamu::*;
use kamu_core::utils::metadata_chain_comparator::CompareChainsResult;
use kamu_core::*;
use messaging_outbox::DummyOutboxImpl;
use opendatafabric::*;
use time_source::SystemTimeSourceDefault;
use url::Url;

use crate::utils::authentication_catalogs;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MockRemoteStatusService {}

#[async_trait::async_trait]
impl RemoteStatusService for MockRemoteStatusService {
    async fn check_remotes_status(
        &self,
        _dataset_handle: &DatasetHandle,
    ) -> std::result::Result<DatasetPushStatuses, InternalError> {
        Ok(DatasetPushStatuses {
            statuses: vec![
                PushStatus {
                    remote: remote("https://example.com/ahead"),
                    check_result: Ok(CompareChainsResult::LhsBehind {
                        rhs_ahead_blocks: vec![],
                    }),
                },
                PushStatus {
                    remote: remote("https://example.com/behind"),
                    check_result: Ok(CompareChainsResult::LhsAhead {
                        lhs_ahead_blocks: vec![],
                    }),
                },
                PushStatus {
                    remote: remote("https://example.com/diverged"),
                    check_result: Ok(CompareChainsResult::Divergence {
                        uncommon_blocks_in_lhs: 0,
                        uncommon_blocks_in_rhs: 0,
                    }),
                },
                PushStatus {
                    remote: remote("https://example.com/equal"),
                    check_result: Ok(CompareChainsResult::Equal),
                },
                PushStatus {
                    remote: remote("https://example.com/not-found"),
                    check_result: Err(StatusCheckError::RemoteDatasetNotFound),
                },
            ],
        })
    }
}

fn remote(url: &str) -> DatasetRefRemote {
    DatasetRefRemote::Url(Arc::new(Url::parse(url).unwrap()))
}

#[test_log::test(tokio::test)]
async fn test_push_statuses() {
    let tempdir = tempfile::tempdir().unwrap();
    let datasets_dir = tempdir.path().join("datasets");
    std::fs::create_dir(&datasets_dir).unwrap();

    let base_catalog = {
        let mut b = CatalogBuilder::new();

        b.add_value(RunInfoDir::new(tempdir.path().join("run")))
            .add::<DummyOutboxImpl>()
            .add_builder(DatasetRepositoryLocalFs::builder().with_root(datasets_dir))
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .bind::<dyn DatasetRepositoryWriter, DatasetRepositoryLocalFs>()
            .add::<CreateDatasetFromSnapshotUseCaseImpl>()
            .add::<SystemTimeSourceDefault>()
            .add::<DatasetRegistryRepoBridge>()
            .add_value(TenancyConfig::SingleTenant)
            .add_value(MockRemoteStatusService {})
            .bind::<dyn RemoteStatusService, MockRemoteStatusService>();

        NoOpDatabasePlugin::init_database_components(&mut b);

        b.build()
    };

    // Init dataset with no sources
    let (_, catalog_authorized) = authentication_catalogs(&base_catalog).await;

    let create_dataset_from_snapshot = catalog_authorized
        .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
        .unwrap();
    let create_result = create_dataset_from_snapshot
        .execute(
            MetadataFactory::dataset_snapshot()
                .kind(DatasetKind::Root)
                .name("foo")
                .build(),
            Default::default(),
        )
        .await
        .unwrap();

    let request_code = indoc!(
        r#"
        {
            datasets {
                byId (datasetId: "<id>") {
                    metadata {
                        pushSyncStatuses {
                            statuses {
                                remote,
                                result {
                                    ... on CompareChainsResultStatus { message }
                                    ... on CompareChainsResultError { reason { message } }
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
        .execute(Request::new(request_code.clone()).data(catalog_authorized.clone()))
        .await;

    assert!(res.is_ok(), "{res:?}");

    let expected = value!({
        "datasets": {
            "byId": {
                "metadata": {
                    "pushSyncStatuses": {
                        "statuses": [
                            {
                                "remote": "https://example.com/ahead",
                                "result": {
                                    "message": "AHEAD"
                                }
                            },
                            {
                                "remote": "https://example.com/behind",
                                "result": {
                                    "message": "BEHIND"
                                }
                            },
                            {
                                "remote": "https://example.com/diverged",
                                "result": {
                                    "message": "DIVERGED"
                                }
                            },
                            {
                                "remote": "https://example.com/equal",
                                "result": {
                                    "message": "EQUAL"
                                }
                            },
                            {
                                "remote": "https://example.com/not-found",
                                "result": {
                                    "reason": {
                                        "message": "Remote dataset not found"
                                    }
                                }
                            }
                        ]
                    }
                }
            }
        }
    });
    assert_eq!(res.data, expected);
}

#[test_log::test(tokio::test)]
async fn test_current_push_sources() {
    let tempdir = tempfile::tempdir().unwrap();
    let datasets_dir = tempdir.path().join("datasets");
    std::fs::create_dir(&datasets_dir).unwrap();

    let base_catalog = {
        let mut b = CatalogBuilder::new();

        b.add_value(RunInfoDir::new(tempdir.path().join("run")))
            .add::<DummyOutboxImpl>()
            .add_value(TenancyConfig::SingleTenant)
            .add_builder(DatasetRepositoryLocalFs::builder().with_root(datasets_dir))
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .bind::<dyn DatasetRepositoryWriter, DatasetRepositoryLocalFs>()
            .add::<DatasetRegistryRepoBridge>()
            .add::<CreateDatasetFromSnapshotUseCaseImpl>()
            .add::<PushIngestServiceImpl>()
            .add::<SystemTimeSourceDefault>()
            .add::<EngineProvisionerNull>()
            .add::<ObjectStoreRegistryImpl>()
            .add::<DataFormatRegistryImpl>()
            .add::<auth::AlwaysHappyDatasetActionAuthorizer>()
            .add::<DependencyGraphServiceInMemory>();

        NoOpDatabasePlugin::init_database_components(&mut b);

        b.build()
    };

    // Init dataset with no sources
    let (_, catalog_authorized) = authentication_catalogs(&base_catalog).await;

    let create_dataset_from_snapshot = catalog_authorized
        .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
        .unwrap();
    let create_result = create_dataset_from_snapshot
        .execute(
            MetadataFactory::dataset_snapshot()
                .kind(DatasetKind::Root)
                .name("foo")
                .build(),
            Default::default(),
        )
        .await
        .unwrap();

    let request_code = indoc!(
        r#"
        {
            datasets {
                byId (datasetId: "<id>") {
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
    )
    .replace("<id>", &create_result.dataset_handle.id.to_string());

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(Request::new(request_code.clone()).data(catalog_authorized.clone()))
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
                .read(ReadStepCsv {
                    schema: Some(vec!["foo STRING".to_string()]),
                    ..Default::default()
                })
                .build()
                .into(),
            CommitOpts::default(),
        )
        .await
        .unwrap();
    create_result
        .dataset
        .commit_event(
            MetadataFactory::add_push_source()
                .source_name("source2")
                .read(ReadStepNdJson {
                    schema: Some(vec!["foo STRING".to_string()]),
                    ..Default::default()
                })
                .build()
                .into(),
            CommitOpts::default(),
        )
        .await
        .unwrap();

    let res = schema
        .execute(Request::new(request_code.clone()).data(catalog_authorized))
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
