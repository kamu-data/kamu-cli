// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use indoc::indoc;
use kamu::testing::BaseRepoHarness;
use kamu::{
    DatasetPullSourceResolverImpl,
    RemoteAliasesRegistryImpl,
    RemoteDatasetUrlResolverImpl,
    RemoteReposDir,
    RemoteRepositoryRegistryImpl,
    SetWatermarkExecutorImpl,
    SetWatermarkPlannerImpl,
    SetWatermarkUseCaseImpl,
};
use kamu_core::TenancyConfig;
use kamu_datasets::{
    CreateDatasetFromSnapshotUseCase,
    CreateDatasetResult,
    CreateDatasetUseCaseOptions,
    DatasetReferenceRepository,
};
use kamu_datasets_services::DatasetOperationGuardImpl;
use kamu_datasets_services::testing::DatasetReferenceRace;
use messaging_outbox::OutboxProvider;
use odf::metadata::testing::MetadataFactory;
use pretty_assertions::{assert_eq, assert_ne};

use crate::utils::{BaseGQLDatasetHarness, PredefinedAccountOpts, authentication_catalogs};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_set_watermark_updated() {
    let harness = SetWatermarkHarness::new().await;
    let foo = harness.create_root_dataset().await;
    let old_head = harness.stored_head(&foo).await;

    let res = harness.set_watermark(&foo).await;

    assert!(res.is_ok(), "{res:?}");
    assert_eq!(
        async_graphql::value!({
            "datasets": {
                "byId": {
                    "setWatermark": {
                        "__typename": "SetWatermarkUpdated",
                        "message": "Updated",
                    }
                }
            }
        }),
        res.data,
    );
    assert_ne!(harness.stored_head(&foo).await, old_head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_set_watermark_ref_moved_concurrently() {
    let harness = SetWatermarkHarness::new().await;
    let foo = harness.create_root_dataset().await;
    let planned_head = harness.stored_head(&foo).await;

    let concurrent_head = harness.append_unreferenced_block(&foo, &planned_head).await;
    harness.move_reference_before_next_update(concurrent_head.clone());

    let res = harness.set_watermark(&foo).await;

    assert!(res.is_ok(), "{res:?}");
    assert_eq!(
        async_graphql::value!({
            "datasets": {
                "byId": {
                    "setWatermark": {
                        "__typename": "SetWatermarkCasFailed",
                        "message": "Expected head didn't match, dataset was likely updated concurrently",
                        "expectedHead": planned_head.to_string(),
                        "actualHead": concurrent_head.to_string(),
                    }
                }
            }
        }),
        res.data,
    );
    assert_eq!(harness.stored_head(&foo).await, concurrent_head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_set_watermark_head_removed_concurrently() {
    let harness = SetWatermarkHarness::new().await;
    let foo = harness.create_root_dataset().await;
    let planned_head = harness.stored_head(&foo).await;

    harness.remove_reference_before_next_update();

    let res = harness.set_watermark(&foo).await;

    assert!(res.is_ok(), "{res:?}");
    assert_eq!(
        async_graphql::value!({
            "datasets": {
                "byId": {
                    "setWatermark": {
                        "__typename": "SetWatermarkCasFailed",
                        "message": "Expected head didn't match, dataset was likely updated concurrently",
                        "expectedHead": planned_head.to_string(),
                        "actualHead": null,
                    }
                }
            }
        }),
        res.data,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseGQLDatasetHarness, base_gql_harness)]
struct SetWatermarkHarness {
    base_gql_harness: BaseGQLDatasetHarness,
    catalog_authorized: dill::Catalog,
    reference_race: Arc<DatasetReferenceRace>,
    dataset_ref_repo: Arc<dyn DatasetReferenceRepository>,
}

impl SetWatermarkHarness {
    async fn new() -> Self {
        let base_gql_harness = BaseGQLDatasetHarness::builder()
            .tenancy_config(TenancyConfig::SingleTenant)
            .outbox_provider(OutboxProvider::Immediate {
                force_immediate: true,
            })
            .racing_references(true)
            .build();

        let base_catalog = dill::CatalogBuilder::new_chained(base_gql_harness.catalog())
            .add::<SetWatermarkUseCaseImpl>()
            .add::<DatasetOperationGuardImpl>()
            .add::<SetWatermarkPlannerImpl>()
            .add::<SetWatermarkExecutorImpl>()
            .add::<RemoteAliasesRegistryImpl>()
            .add::<DatasetPullSourceResolverImpl>()
            .add::<RemoteDatasetUrlResolverImpl>()
            .add::<RemoteRepositoryRegistryImpl>()
            .add_value(RemoteReposDir::new_with_creation(
                base_gql_harness.temp_dir().join("repos"),
            ))
            .add::<odf::dataset::DummyOdfServerAccessTokenResolver>()
            .build();

        let (_, catalog_authorized) =
            authentication_catalogs(&base_catalog, PredefinedAccountOpts::default()).await;

        Self {
            base_gql_harness,
            reference_race: catalog_authorized.get_one().unwrap(),
            dataset_ref_repo: catalog_authorized.get_one().unwrap(),
            catalog_authorized,
        }
    }

    async fn create_root_dataset(&self) -> CreateDatasetResult {
        self.catalog_authorized
            .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
            .unwrap()
            .execute(
                MetadataFactory::dataset_snapshot()
                    .name("foo")
                    .kind(odf::DatasetKind::Root)
                    .push_event(MetadataFactory::set_polling_source().build())
                    .build(),
                CreateDatasetUseCaseOptions::default(),
            )
            .await
            .unwrap()
    }

    /// Appends a `SetInfo` block after `head` without moving any reference
    async fn append_unreferenced_block(
        &self,
        created: &CreateDatasetResult,
        head: &odf::Multihash,
    ) -> odf::Multihash {
        BaseRepoHarness::append_unreferenced_set_info(
            created.dataset.as_metadata_chain(),
            head,
            "concurrent",
            None,
        )
        .await
    }

    fn move_reference_before_next_update(&self, concurrent_head: odf::Multihash) {
        self.reference_race
            .move_reference_before_next_update(&self.catalog_authorized, concurrent_head);
    }

    fn remove_reference_before_next_update(&self) {
        self.reference_race
            .remove_reference_before_next_update(&self.catalog_authorized);
    }

    async fn stored_head(&self, created: &CreateDatasetResult) -> odf::Multihash {
        self.dataset_ref_repo
            .get_dataset_reference(&created.dataset_handle.id, &odf::BlockRef::Head)
            .await
            .unwrap()
    }

    async fn set_watermark(&self, created: &CreateDatasetResult) -> async_graphql::Response {
        let request_code = indoc!(
            r#"
            mutation {
                datasets {
                    byId (datasetId: "<id>") {
                        setWatermark(watermark: "2050-01-01T12:00:00Z") {
                            __typename
                            message
                            ... on SetWatermarkCasFailed {
                                expectedHead
                                actualHead
                            }
                        }
                    }
                }
            }
            "#
        )
        .replace("<id>", &created.dataset_handle.id.to_string());

        self.execute_query(request_code, &self.catalog_authorized)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
