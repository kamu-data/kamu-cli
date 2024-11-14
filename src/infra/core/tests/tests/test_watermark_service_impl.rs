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
use kamu::{RemoteAliasesRegistryImpl, WatermarkServiceImpl};
use kamu_core::{
    CreateDatasetResult,
    SetWatermarkError,
    SetWatermarkResult,
    TenancyConfig,
    WatermarkService,
};
use opendatafabric::{DatasetAlias, DatasetName, DatasetRef};

use crate::BaseRepoHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_no_watermark_initially() {
    let harness = WatermarkTestHarness::new(TenancyConfig::SingleTenant);

    let foo_created = harness
        .create_root_dataset(&DatasetAlias::new(
            None,
            DatasetName::try_from("foo").unwrap(),
        ))
        .await;

    assert_eq!(harness.current_watermark(&foo_created).await, None,);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_set_watermark() {
    let harness = WatermarkTestHarness::new(TenancyConfig::SingleTenant);

    let foo_created = harness
        .create_root_dataset(&DatasetAlias::new(
            None,
            DatasetName::try_from("foo").unwrap(),
        ))
        .await;

    assert_eq!(harness.num_blocks(&foo_created).await, 2);

    let watermark_1 = Utc.with_ymd_and_hms(2000, 1, 2, 0, 0, 0).unwrap();
    assert_matches!(
        harness.set_watermark(&foo_created, watermark_1).await,
        Ok(SetWatermarkResult::Updated { .. })
    );
    assert_eq!(harness.num_blocks(&foo_created).await, 3);
    assert_eq!(
        harness.current_watermark(&foo_created).await,
        Some(watermark_1),
    );

    let watermark_2 = Utc.with_ymd_and_hms(2000, 1, 3, 0, 0, 0).unwrap();
    assert_matches!(
        harness.set_watermark(&foo_created, watermark_2).await,
        Ok(SetWatermarkResult::Updated { .. })
    );
    assert_eq!(harness.num_blocks(&foo_created).await, 4);
    assert_eq!(
        harness.current_watermark(&foo_created).await,
        Some(watermark_2),
    );

    assert_matches!(
        harness.set_watermark(&foo_created, watermark_2).await,
        Ok(SetWatermarkResult::UpToDate)
    );
    assert_eq!(harness.num_blocks(&foo_created).await, 4);
    assert_eq!(
        harness.current_watermark(&foo_created).await,
        Some(watermark_2),
    );

    let watermark_3 = Utc.with_ymd_and_hms(2000, 1, 2, 0, 0, 0).unwrap();
    assert_matches!(
        harness.set_watermark(&foo_created, watermark_3).await,
        Ok(SetWatermarkResult::UpToDate)
    );
    assert_eq!(harness.num_blocks(&foo_created).await, 4);
    assert_eq!(
        harness.current_watermark(&foo_created).await,
        Some(watermark_2),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_set_watermark_rejects_on_derivative() {
    let harness = WatermarkTestHarness::new(TenancyConfig::MultiTenant);

    let created_root = harness
        .create_root_dataset(&DatasetAlias::new(
            None,
            DatasetName::try_from("foo").unwrap(),
        ))
        .await;

    let created_derived = harness
        .create_derived_dataset(
            &DatasetAlias::new(None, DatasetName::try_from("bar").unwrap()),
            vec![created_root.dataset_handle.as_local_ref()],
        )
        .await;

    assert_matches!(
        harness
            .set_watermark(
                &created_derived,
                Utc.with_ymd_and_hms(2000, 1, 2, 0, 0, 0).unwrap()
            )
            .await,
        Err(SetWatermarkError::IsDerivative)
    );

    assert_eq!(harness.num_blocks(&created_derived).await, 2);
    assert_eq!(harness.current_watermark(&created_derived).await, None,);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

    #[inline]
    async fn create_root_dataset(&self, alias: &DatasetAlias) -> CreateDatasetResult {
        self.base_repo_harness.create_root_dataset(alias).await
    }

    #[inline]
    async fn create_derived_dataset(
        &self,
        alias: &DatasetAlias,
        input_dataset_refs: Vec<DatasetRef>,
    ) -> CreateDatasetResult {
        self.base_repo_harness
            .create_derived_dataset(alias, input_dataset_refs)
            .await
    }

    #[inline]
    async fn num_blocks(&self, create_result: &CreateDatasetResult) -> usize {
        self.base_repo_harness.num_blocks(create_result).await
    }

    async fn set_watermark(
        &self,
        create_result: &CreateDatasetResult,
        new_watermark: DateTime<Utc>,
    ) -> Result<SetWatermarkResult, SetWatermarkError> {
        self.watermark_svc
            .set_watermark(create_result.dataset.clone(), new_watermark)
            .await
    }

    async fn current_watermark(
        &self,
        create_result: &CreateDatasetResult,
    ) -> Option<DateTime<Utc>> {
        self.watermark_svc
            .try_get_current_watermark(create_result.dataset.clone())
            .await
            .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
