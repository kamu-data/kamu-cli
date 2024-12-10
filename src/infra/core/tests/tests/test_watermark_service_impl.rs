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
use kamu::{RemoteAliasesRegistryImpl, WatermarkServiceImpl};
use kamu_core::{
    ResolvedDataset,
    SetWatermarkError,
    SetWatermarkResult,
    TenancyConfig,
    WatermarkService,
};
use opendatafabric::{DatasetAlias, DatasetName};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_no_watermark_initially() {
    let harness = WatermarkTestHarness::new(TenancyConfig::SingleTenant);

    let foo = harness
        .create_root_dataset(&DatasetAlias::new(
            None,
            DatasetName::try_from("foo").unwrap(),
        ))
        .await;

    assert_eq!(
        harness.current_watermark(ResolvedDataset::from(&foo)).await,
        None,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_set_watermark() {
    let harness = WatermarkTestHarness::new(TenancyConfig::SingleTenant);

    let foo = harness
        .create_root_dataset(&DatasetAlias::new(
            None,
            DatasetName::try_from("foo").unwrap(),
        ))
        .await;

    assert_eq!(harness.num_blocks(ResolvedDataset::from(&foo)).await, 2);

    let watermark_1 = Utc.with_ymd_and_hms(2000, 1, 2, 0, 0, 0).unwrap();
    assert_matches!(
        harness
            .set_watermark(ResolvedDataset::from(&foo), watermark_1)
            .await,
        Ok(SetWatermarkResult::Updated { .. })
    );
    assert_eq!(harness.num_blocks(ResolvedDataset::from(&foo)).await, 3);
    assert_eq!(
        harness.current_watermark(ResolvedDataset::from(&foo)).await,
        Some(watermark_1),
    );

    let watermark_2 = Utc.with_ymd_and_hms(2000, 1, 3, 0, 0, 0).unwrap();
    assert_matches!(
        harness
            .set_watermark(ResolvedDataset::from(&foo), watermark_2)
            .await,
        Ok(SetWatermarkResult::Updated { .. })
    );
    assert_eq!(harness.num_blocks(ResolvedDataset::from(&foo)).await, 4);
    assert_eq!(
        harness.current_watermark(ResolvedDataset::from(&foo)).await,
        Some(watermark_2),
    );

    assert_matches!(
        harness
            .set_watermark(ResolvedDataset::from(&foo), watermark_2)
            .await,
        Ok(SetWatermarkResult::UpToDate)
    );
    assert_eq!(harness.num_blocks(ResolvedDataset::from(&foo)).await, 4);
    assert_eq!(
        harness.current_watermark(ResolvedDataset::from(&foo)).await,
        Some(watermark_2),
    );

    let watermark_3 = Utc.with_ymd_and_hms(2000, 1, 2, 0, 0, 0).unwrap();
    assert_matches!(
        harness
            .set_watermark(ResolvedDataset::from(&foo), watermark_3)
            .await,
        Ok(SetWatermarkResult::UpToDate)
    );
    assert_eq!(harness.num_blocks(ResolvedDataset::from(&foo)).await, 4);
    assert_eq!(
        harness.current_watermark(ResolvedDataset::from(&foo)).await,
        Some(watermark_2),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_set_watermark_rejects_on_derivative() {
    let harness = WatermarkTestHarness::new(TenancyConfig::MultiTenant);

    let root = harness
        .create_root_dataset(&DatasetAlias::new(
            None,
            DatasetName::try_from("foo").unwrap(),
        ))
        .await;

    let derived = harness
        .create_derived_dataset(
            &DatasetAlias::new(None, DatasetName::try_from("bar").unwrap()),
            vec![root.dataset_handle.as_local_ref()],
        )
        .await;

    assert_matches!(
        harness
            .set_watermark(
                ResolvedDataset::from(&derived),
                Utc.with_ymd_and_hms(2000, 1, 2, 0, 0, 0).unwrap()
            )
            .await,
        Err(SetWatermarkError::IsDerivative)
    );

    assert_eq!(harness.num_blocks(ResolvedDataset::from(&derived)).await, 2);
    assert_eq!(
        harness
            .current_watermark(ResolvedDataset::from(&derived))
            .await,
        None,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseRepoHarness, base_repo_harness)]
struct WatermarkTestHarness {
    base_repo_harness: BaseRepoHarness,
    watermark_svc: Arc<dyn WatermarkService>,
}

impl WatermarkTestHarness {
    fn new(tenancy_config: TenancyConfig) -> Self {
        let base_repo_harness = BaseRepoHarness::new(tenancy_config);

        let catalog = dill::CatalogBuilder::new_chained(base_repo_harness.catalog())
            .add::<RemoteAliasesRegistryImpl>()
            .add::<WatermarkServiceImpl>()
            .build();

        Self {
            base_repo_harness,
            watermark_svc: catalog.get_one().unwrap(),
        }
    }

    async fn set_watermark(
        &self,
        target: ResolvedDataset,
        new_watermark: DateTime<Utc>,
    ) -> Result<SetWatermarkResult, SetWatermarkError> {
        self.watermark_svc
            .set_watermark(target, new_watermark)
            .await
    }

    async fn current_watermark(&self, target: ResolvedDataset) -> Option<DateTime<Utc>> {
        self.watermark_svc
            .try_get_current_watermark(target)
            .await
            .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
