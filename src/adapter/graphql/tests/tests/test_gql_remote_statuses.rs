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
use dill::*;
use indoc::indoc;
use internal_error::InternalError;
use kamu_auth_rebac_services::RebacDatasetRegistryFacadeImpl;
use kamu_core::utils::metadata_chain_comparator::CompareChainsResult;
use kamu_core::*;
use kamu_datasets::*;
use odf::metadata::testing::MetadataFactory;
use url::Url;

use crate::utils::{authentication_catalogs, BaseGQLDatasetHarness};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_remote_push_statuses() {
    let harness = PushStatusesTestHarness::new();

    // Init dataset with no sources
    let (_, catalog_authorized) = authentication_catalogs(&harness.catalog).await;

    let create_dataset_from_snapshot = catalog_authorized
        .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
        .unwrap();
    let create_result = create_dataset_from_snapshot
        .execute(
            MetadataFactory::dataset_snapshot()
                .kind(odf::DatasetKind::Root)
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseGQLDatasetHarness, base_gql_harness)]
struct PushStatusesTestHarness {
    base_gql_harness: BaseGQLDatasetHarness,
    catalog: Catalog,
}

impl PushStatusesTestHarness {
    fn new() -> Self {
        let base_gql_harness = BaseGQLDatasetHarness::builder()
            .tenancy_config(TenancyConfig::SingleTenant)
            .build();

        let catalog = {
            let mut b = CatalogBuilder::new_chained(base_gql_harness.catalog());

            b.add_value(FakeRemoteStatusService {})
                .bind::<dyn RemoteStatusService, FakeRemoteStatusService>();
            b.add::<RebacDatasetRegistryFacadeImpl>();

            b.build()
        };

        Self {
            base_gql_harness,
            catalog,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FakeRemoteStatusService {}

#[async_trait::async_trait]
impl RemoteStatusService for FakeRemoteStatusService {
    async fn check_remotes_status(
        &self,
        _dataset_handle: &odf::DatasetHandle,
    ) -> std::result::Result<DatasetPushStatuses, InternalError> {
        Ok(DatasetPushStatuses {
            statuses: vec![
                PushStatus {
                    remote: odf::DatasetRefRemote::Url(Arc::new(
                        Url::parse("https://example.com/ahead").unwrap(),
                    )),
                    check_result: Ok(CompareChainsResult::LhsBehind {
                        rhs_ahead_blocks: vec![],
                    }),
                },
                PushStatus {
                    remote: odf::DatasetRefRemote::Url(Arc::new(
                        Url::parse("https://example.com/behind").unwrap(),
                    )),
                    check_result: Ok(CompareChainsResult::LhsAhead {
                        lhs_ahead_blocks: vec![],
                    }),
                },
                PushStatus {
                    remote: odf::DatasetRefRemote::Url(Arc::new(
                        Url::parse("https://example.com/diverged").unwrap(),
                    )),
                    check_result: Ok(CompareChainsResult::Divergence {
                        uncommon_blocks_in_lhs: 0,
                        uncommon_blocks_in_rhs: 0,
                    }),
                },
                PushStatus {
                    remote: odf::DatasetRefRemote::Url(Arc::new(
                        Url::parse("https://example.com/equal").unwrap(),
                    )),
                    check_result: Ok(CompareChainsResult::Equal),
                },
                PushStatus {
                    remote: odf::DatasetRefRemote::Url(Arc::new(
                        Url::parse("https://example.com/not-found").unwrap(),
                    )),
                    check_result: Err(StatusCheckError::RemoteDatasetNotFound),
                },
            ],
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
