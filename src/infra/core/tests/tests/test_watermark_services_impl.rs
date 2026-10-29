// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::sync::Arc;

use chrono::{DateTime, TimeZone, Utc};
use kamu::testing::BaseRepoHarness;
use kamu::{
    DatasetPullSourceResolverImpl,
    MetadataQueryServiceImpl,
    RemoteAliasesRegistryImpl,
    RemoteDatasetUrlResolverImpl,
    RemoteReposDir,
    RemoteRepositoryRegistryImpl,
    SetWatermarkExecutorImpl,
    SetWatermarkPlannerImpl,
};
use kamu_core::{
    MetadataQueryService,
    RemoteAliasKind,
    RemoteAliasesRegistry,
    SetWatermarkError,
    SetWatermarkExecutionError,
    SetWatermarkExecutor,
    SetWatermarkPlan,
    SetWatermarkPlanner,
    SetWatermarkPlanningError,
    SetWatermarkResult,
    TenancyConfig,
};
use kamu_datasets::{CreateDatasetResult, ResolvedDataset};
use kamu_datasets_services::DatasetHeadCommitterImpl;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_no_watermark_initially() {
    let harness = WatermarkTestHarness::new(TenancyConfig::SingleTenant);

    let foo = harness
        .create_root_dataset(&odf::DatasetAlias::new(
            None,
            odf::DatasetName::try_from("foo").unwrap(),
        ))
        .await;

    assert_eq!(
        harness
            .current_watermark(ResolvedDataset::from_created(&foo))
            .await,
        None,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_set_watermark() {
    let harness = WatermarkTestHarness::new(TenancyConfig::SingleTenant);

    let foo = harness
        .create_root_dataset(&odf::DatasetAlias::new(
            None,
            odf::DatasetName::try_from("foo").unwrap(),
        ))
        .await;

    assert_eq!(
        harness
            .num_blocks(ResolvedDataset::from_created(&foo))
            .await,
        2
    );

    let watermark_1 = Utc.with_ymd_and_hms(2000, 1, 2, 0, 0, 0).unwrap();
    assert_matches!(
        harness
            .set_watermark(ResolvedDataset::from_created(&foo), watermark_1)
            .await,
        Ok(SetWatermarkResult::Updated { .. })
    );
    assert_eq!(
        harness
            .num_blocks(ResolvedDataset::from_created(&foo))
            .await,
        3
    );
    assert_eq!(
        harness
            .current_watermark(ResolvedDataset::from_created(&foo))
            .await,
        Some(watermark_1),
    );

    let watermark_2 = Utc.with_ymd_and_hms(2000, 1, 3, 0, 0, 0).unwrap();
    assert_matches!(
        harness
            .set_watermark(ResolvedDataset::from_created(&foo), watermark_2)
            .await,
        Ok(SetWatermarkResult::Updated { .. })
    );
    assert_eq!(
        harness
            .num_blocks(ResolvedDataset::from_created(&foo))
            .await,
        4
    );
    assert_eq!(
        harness
            .current_watermark(ResolvedDataset::from_created(&foo))
            .await,
        Some(watermark_2),
    );

    assert_matches!(
        harness
            .set_watermark(ResolvedDataset::from_created(&foo), watermark_2)
            .await,
        Ok(SetWatermarkResult::UpToDate)
    );
    assert_eq!(
        harness
            .num_blocks(ResolvedDataset::from_created(&foo))
            .await,
        4
    );
    assert_eq!(
        harness
            .current_watermark(ResolvedDataset::from_created(&foo))
            .await,
        Some(watermark_2),
    );

    let watermark_3 = Utc.with_ymd_and_hms(2000, 1, 2, 0, 0, 0).unwrap();
    assert_matches!(
        harness
            .set_watermark(ResolvedDataset::from_created(&foo), watermark_3)
            .await,
        Ok(SetWatermarkResult::UpToDate)
    );
    assert_eq!(
        harness
            .num_blocks(ResolvedDataset::from_created(&foo))
            .await,
        4
    );
    assert_eq!(
        harness
            .current_watermark(ResolvedDataset::from_created(&foo))
            .await,
        Some(watermark_2),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_set_watermark_rejects_on_derivative() {
    let harness = WatermarkTestHarness::new(TenancyConfig::MultiTenant);

    let root = harness
        .create_root_dataset(&odf::DatasetAlias::new(
            None,
            odf::DatasetName::try_from("foo").unwrap(),
        ))
        .await;

    let derived = harness
        .create_derived_dataset(
            &odf::DatasetAlias::new(None, odf::DatasetName::try_from("bar").unwrap()),
            vec![root.dataset_handle.as_local_ref()],
        )
        .await;

    assert_matches!(
        harness
            .set_watermark(
                ResolvedDataset::from_created(&derived),
                Utc.with_ymd_and_hms(2000, 1, 2, 0, 0, 0).unwrap()
            )
            .await,
        Err(SetWatermarkError::Planning(
            SetWatermarkPlanningError::IsDerivative
        ))
    );

    assert_eq!(
        harness
            .num_blocks(ResolvedDataset::from_created(&derived))
            .await,
        2
    );
    assert_eq!(
        harness
            .current_watermark(ResolvedDataset::from_created(&derived))
            .await,
        None,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_set_watermark_rejects_on_dataset_pulled_from_remote() {
    let harness = WatermarkTestHarness::new(TenancyConfig::SingleTenant);

    let foo = harness
        .create_root_dataset(&odf::DatasetAlias::new(
            None,
            odf::DatasetName::try_from("foo").unwrap(),
        ))
        .await;
    harness
        .add_pull_alias(&foo, "odf+http://example.com/foo")
        .await;

    assert_matches!(
        harness
            .set_watermark(
                ResolvedDataset::from_created(&foo),
                Utc.with_ymd_and_hms(2000, 1, 2, 0, 0, 0).unwrap()
            )
            .await,
        Err(SetWatermarkError::Planning(
            SetWatermarkPlanningError::IsRemote
        ))
    );
    assert_eq!(
        harness
            .current_watermark(ResolvedDataset::from_created(&foo))
            .await,
        None,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_set_watermark_head_moved_after_planning() {
    let harness = WatermarkTestHarness::new(TenancyConfig::SingleTenant);

    let foo = harness
        .create_root_dataset(&odf::DatasetAlias::new(
            None,
            odf::DatasetName::try_from("foo").unwrap(),
        ))
        .await;
    let planned_head = harness.get_head(&foo).await;

    let plan = harness
        .plan_set_watermark(
            ResolvedDataset::from_created(&foo),
            Utc.with_ymd_and_hms(2000, 1, 2, 0, 0, 0).unwrap(),
        )
        .await;

    let concurrent_head = harness.append_unreferenced_block(&foo, "concurrent").await;
    harness
        .move_head(&foo, &planned_head, &concurrent_head)
        .await;

    assert_matches!(
        harness
            .set_watermark_executor
            .execute(ResolvedDataset::from_created(&foo), plan)
            .await,
        Err(SetWatermarkExecutionError::RefCASFailed(odf::dataset::RefCASError {
            expected,
            actual,
            ..
        })) if expected == Some(planned_head) && actual == Some(concurrent_head.clone())
    );
    assert_eq!(harness.get_head(&foo).await, concurrent_head);
    assert_eq!(
        harness
            .current_watermark(ResolvedDataset::from_created(&foo))
            .await,
        None,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseRepoHarness, base_repo_harness)]
struct WatermarkTestHarness {
    base_repo_harness: BaseRepoHarness,
    set_watermark_planner: Arc<dyn SetWatermarkPlanner>,
    set_watermark_executor: Arc<dyn SetWatermarkExecutor>,
    metadata_query_svc: Arc<dyn MetadataQueryService>,
    remote_aliases_registry: Arc<dyn RemoteAliasesRegistry>,
}

impl WatermarkTestHarness {
    fn new(tenancy_config: TenancyConfig) -> Self {
        let base_repo_harness = BaseRepoHarness::builder()
            .tenancy_config(tenancy_config)
            .build();

        let catalog = dill::CatalogBuilder::new_chained(base_repo_harness.catalog())
            .add::<RemoteAliasesRegistryImpl>()
            .add::<DatasetPullSourceResolverImpl>()
            .add::<RemoteDatasetUrlResolverImpl>()
            .add::<RemoteRepositoryRegistryImpl>()
            .add_value(RemoteReposDir::new_with_creation(
                base_repo_harness.temp_dir_path().join("repos"),
            ))
            .add::<odf::dataset::DummyOdfServerAccessTokenResolver>()
            .add::<SetWatermarkPlannerImpl>()
            .add::<SetWatermarkExecutorImpl>()
            .add::<DatasetHeadCommitterImpl>()
            .add::<MetadataQueryServiceImpl>()
            .build();

        Self {
            base_repo_harness,
            set_watermark_planner: catalog.get_one().unwrap(),
            set_watermark_executor: catalog.get_one().unwrap(),
            metadata_query_svc: catalog.get_one().unwrap(),
            remote_aliases_registry: catalog.get_one().unwrap(),
        }
    }

    async fn add_pull_alias(&self, created: &CreateDatasetResult, remote_ref: &str) {
        self.remote_aliases_registry
            .get_remote_aliases(&created.dataset_handle)
            .await
            .unwrap()
            .add(&remote_ref.parse().unwrap(), RemoteAliasKind::Pull)
            .await
            .unwrap();
    }

    async fn plan_set_watermark(
        &self,
        target: ResolvedDataset,
        new_watermark: DateTime<Utc>,
    ) -> SetWatermarkPlan {
        self.set_watermark_planner
            .plan_set_watermark(target, new_watermark)
            .await
            .unwrap()
    }

    async fn set_watermark(
        &self,
        target: ResolvedDataset,
        new_watermark: DateTime<Utc>,
    ) -> Result<SetWatermarkResult, SetWatermarkError> {
        let plan = self
            .set_watermark_planner
            .plan_set_watermark(target.clone(), new_watermark)
            .await?;

        let result = self.set_watermark_executor.execute(target, plan).await?;

        Ok(result)
    }

    async fn current_watermark(&self, target: ResolvedDataset) -> Option<DateTime<Utc>> {
        self.metadata_query_svc
            .try_get_current_watermark(&target)
            .await
            .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
