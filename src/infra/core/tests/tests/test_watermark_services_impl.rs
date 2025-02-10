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

use chrono::{DateTime, TimeZone, Utc};
use kamu::testing::BaseRepoHarness;
use kamu::{
    MetadataQueryServiceImpl,
    RemoteAliasesRegistryImpl,
    SetWatermarkExecutorImpl,
    SetWatermarkPlannerImpl,
};
use kamu_core::{
    MetadataQueryService,
    ResolvedDataset,
    SetWatermarkError,
    SetWatermarkExecutor,
    SetWatermarkPlanner,
    SetWatermarkPlanningError,
    SetWatermarkResult,
    TenancyConfig,
};

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

#[oop::extend(BaseRepoHarness, base_repo_harness)]
struct WatermarkTestHarness {
    base_repo_harness: BaseRepoHarness,
    set_watermark_planner: Arc<dyn SetWatermarkPlanner>,
    set_watermark_executor: Arc<dyn SetWatermarkExecutor>,
    metadata_query_svc: Arc<dyn MetadataQueryService>,
}

impl WatermarkTestHarness {
    fn new(tenancy_config: TenancyConfig) -> Self {
        let base_repo_harness = BaseRepoHarness::builder()
            .tenancy_config(tenancy_config)
            .build();

        let catalog = dill::CatalogBuilder::new_chained(base_repo_harness.catalog())
            .add::<RemoteAliasesRegistryImpl>()
            .add::<SetWatermarkPlannerImpl>()
            .add::<SetWatermarkExecutorImpl>()
            .add::<MetadataQueryServiceImpl>()
            .build();

        Self {
            base_repo_harness,
            set_watermark_planner: catalog.get_one().unwrap(),
            set_watermark_executor: catalog.get_one().unwrap(),
            metadata_query_svc: catalog.get_one().unwrap(),
        }
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
            .try_get_current_watermark(target)
            .await
            .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
