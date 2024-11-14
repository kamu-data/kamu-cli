// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::sync::Arc;

use chrono::{DateTime, TimeDelta, Utc};
use dill::{Catalog, Component};
use kamu::testing::{MetadataFactory, MockDatasetActionAuthorizer};
use kamu::*;
use kamu_accounts::CurrentAccountSubject;
use kamu_core::auth::DatasetActionAuthorizer;
use kamu_core::*;
use opendatafabric::*;
use time_source::SystemTimeSourceDefault;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_set_watermark_success() {
    let alias_foo = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));

    let harness = SetWatermarkUseCaseHarness::new(
        MockDatasetActionAuthorizer::new().expect_check_write_dataset(&alias_foo, 1, true),
    );

    let create_result_foo = harness.create_root_dataset(&alias_foo).await;
    harness.dependencies_eager_initialization().await;

    let watermark = Utc::now() - TimeDelta::minutes(5);
    let result = harness
        .use_case
        .execute(&create_result_foo.dataset_handle, watermark)
        .await
        .unwrap();

    assert_matches!(result, SetWatermarkResult::Updated { .. });
    assert_eq!(
        harness
            .current_watermark(&create_result_foo.dataset_handle)
            .await,
        Some(watermark)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_set_watermark_unauthorized() {
    let alias_foo = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));

    let harness = SetWatermarkUseCaseHarness::new(
        MockDatasetActionAuthorizer::new().expect_check_write_dataset(&alias_foo, 1, false),
    );

    let create_result_foo = harness.create_root_dataset(&alias_foo).await;
    harness.dependencies_eager_initialization().await;

    assert_matches!(
        harness
            .use_case
            .execute(&create_result_foo.dataset_handle, Utc::now())
            .await,
        Err(SetWatermarkError::Access(_))
    );
    assert_eq!(
        harness
            .current_watermark(&create_result_foo.dataset_handle)
            .await,
        None,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct SetWatermarkUseCaseHarness {
    _temp_dir: tempfile::TempDir,
    catalog: Catalog,
    use_case: Arc<dyn SetWatermarkUseCase>,
    watermark_svc: Arc<dyn WatermarkService>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_repo_writer: Arc<dyn DatasetRepositoryWriter>,
}

impl SetWatermarkUseCaseHarness {
    fn new(mock_dataset_action_authorizer: MockDatasetActionAuthorizer) -> Self {
        let tempdir = tempfile::tempdir().unwrap();

        let datasets_dir = tempdir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();

        let catalog = dill::CatalogBuilder::new()
            .add_value(TenancyConfig::SingleTenant)
            .add::<SetWatermarkUseCaseImpl>()
            .add::<WatermarkServiceImpl>()
            .add::<RemoteAliasesRegistryImpl>()
            .add::<SystemTimeSourceDefault>()
            .add_builder(DatasetRepositoryLocalFs::builder().with_root(datasets_dir))
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .bind::<dyn DatasetRepositoryWriter, DatasetRepositoryLocalFs>()
            .add::<DatasetRegistryRepoBridge>()
            .add::<DependencyGraphServiceInMemory>()
            .add_value(CurrentAccountSubject::new_test())
            .add_value(mock_dataset_action_authorizer)
            .bind::<dyn DatasetActionAuthorizer, MockDatasetActionAuthorizer>()
            .build();

        let use_case = catalog.get_one().unwrap();
        let watermark_svc = catalog.get_one().unwrap();
        let dataset_registry = catalog.get_one().unwrap();
        let dataset_repo_writer = catalog.get_one().unwrap();

        Self {
            _temp_dir: tempdir,
            catalog,
            use_case,
            watermark_svc,
            dataset_registry,
            dataset_repo_writer,
        }
    }

    async fn create_root_dataset(&self, alias: &DatasetAlias) -> CreateDatasetResult {
        let snapshot = MetadataFactory::dataset_snapshot()
            .name(alias.clone())
            .kind(DatasetKind::Root)
            .push_event(MetadataFactory::set_polling_source().build())
            .build();

        let result = self
            .dataset_repo_writer
            .create_dataset_from_snapshot(snapshot)
            .await
            .unwrap();

        result.create_dataset_result
    }

    async fn dependencies_eager_initialization(&self) {
        let dependency_graph_service = self
            .catalog
            .get_one::<dyn DependencyGraphService>()
            .unwrap();
        let dataset_repo = self.catalog.get_one::<dyn DatasetRepository>().unwrap();

        dependency_graph_service
            .eager_initialization(&DependencyGraphRepositoryInMemory::new(dataset_repo))
            .await
            .unwrap();
    }

    async fn current_watermark(&self, hdl: &DatasetHandle) -> Option<DateTime<Utc>> {
        let dataset = self.dataset_registry.get_dataset_by_handle(hdl);

        self.watermark_svc
            .try_get_current_watermark(dataset)
            .await
            .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
