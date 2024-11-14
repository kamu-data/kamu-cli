// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::path::Path;
use std::sync::Arc;

use chrono::{DateTime, TimeZone, Utc};
use dill::Component;
use kamu::testing::MetadataFactory;
use kamu::{
    DatasetRegistryRepoBridge,
    DatasetRepositoryLocalFs,
    DatasetRepositoryWriter,
    RemoteAliasesRegistryImpl,
    WatermarkServiceImpl,
};
use kamu_accounts::CurrentAccountSubject;
use kamu_core::{
    CreateDatasetResult,
    DatasetRegistry,
    DatasetRegistryExt,
    DatasetRepository,
    MetadataChainExt,
    SetWatermarkError,
    SetWatermarkResult,
    TenancyConfig,
    WatermarkService,
};
use opendatafabric::{DatasetAlias, DatasetHandle, DatasetKind, DatasetName};
use time_source::SystemTimeSourceDefault;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_no_watermark_initially() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let harness = WatermarkTestHarness::new(tmp_dir.path(), TenancyConfig::SingleTenant);

    let dataset_alias = DatasetAlias::new(None, DatasetName::try_from("foo").unwrap());
    let create_result = harness.create_dataset(&dataset_alias).await;

    assert_eq!(
        harness
            .current_watermark(&create_result.dataset_handle)
            .await,
        None,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_set_watermark() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let harness = WatermarkTestHarness::new(tmp_dir.path(), TenancyConfig::SingleTenant);

    let dataset_alias = DatasetAlias::new(None, DatasetName::try_from("foo").unwrap());
    let create_result = harness.create_dataset(&dataset_alias).await;

    assert_eq!(harness.num_blocks(&dataset_alias).await, 1);

    let watermark_1 = Utc.with_ymd_and_hms(2000, 1, 2, 0, 0, 0).unwrap();
    assert_matches!(
        harness
            .watermark_svc
            .set_watermark(create_result.dataset.clone(), watermark_1)
            .await,
        Ok(SetWatermarkResult::Updated { .. })
    );
    assert_eq!(harness.num_blocks(&dataset_alias).await, 2);
    assert_eq!(
        harness
            .current_watermark(&create_result.dataset_handle)
            .await,
        Some(watermark_1),
    );

    let watermark_2 = Utc.with_ymd_and_hms(2000, 1, 3, 0, 0, 0).unwrap();
    assert_matches!(
        harness
            .watermark_svc
            .set_watermark(create_result.dataset.clone(), watermark_2)
            .await,
        Ok(SetWatermarkResult::Updated { .. })
    );
    assert_eq!(harness.num_blocks(&dataset_alias).await, 3);
    assert_eq!(
        harness
            .current_watermark(&create_result.dataset_handle)
            .await,
        Some(watermark_2),
    );

    assert_matches!(
        harness
            .watermark_svc
            .set_watermark(create_result.dataset.clone(), watermark_2)
            .await,
        Ok(SetWatermarkResult::UpToDate)
    );
    assert_eq!(harness.num_blocks(&dataset_alias).await, 3);
    assert_eq!(
        harness
            .current_watermark(&create_result.dataset_handle)
            .await,
        Some(watermark_2),
    );

    let watermark_3 = Utc.with_ymd_and_hms(2000, 1, 2, 0, 0, 0).unwrap();
    assert_matches!(
        harness
            .watermark_svc
            .set_watermark(create_result.dataset, watermark_3)
            .await,
        Ok(SetWatermarkResult::UpToDate)
    );
    assert_eq!(harness.num_blocks(&dataset_alias).await, 3);
    assert_eq!(
        harness
            .current_watermark(&create_result.dataset_handle)
            .await,
        Some(watermark_2),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_set_watermark_rejects_on_derivative() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let harness = WatermarkTestHarness::new(tmp_dir.path(), TenancyConfig::MultiTenant);

    let dataset_alias = DatasetAlias::new(None, DatasetName::try_from("foo").unwrap());

    let create_result = harness
        .dataset_repo_writer
        .create_dataset(
            &dataset_alias,
            MetadataFactory::metadata_block(MetadataFactory::seed(DatasetKind::Derivative).build())
                .build_typed(),
        )
        .await
        .unwrap();

    assert_matches!(
        harness
            .watermark_svc
            .set_watermark(
                create_result.dataset,
                Utc.with_ymd_and_hms(2000, 1, 2, 0, 0, 0).unwrap()
            )
            .await,
        Err(SetWatermarkError::IsDerivative)
    );

    assert_eq!(harness.num_blocks(&dataset_alias).await, 1);
    assert_eq!(
        harness
            .current_watermark(&create_result.dataset_handle)
            .await,
        None,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct WatermarkTestHarness {
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_repo_writer: Arc<dyn DatasetRepositoryWriter>,
    watermark_svc: Arc<dyn WatermarkService>,
}

impl WatermarkTestHarness {
    fn new(tmp_path: &Path, tenancy_config: TenancyConfig) -> Self {
        let datasets_dir_path = tmp_path.join("datasets");
        std::fs::create_dir(&datasets_dir_path).unwrap();

        let catalog = dill::CatalogBuilder::new()
            .add::<SystemTimeSourceDefault>()
            .add_value(CurrentAccountSubject::new_test())
            .add_value(tenancy_config)
            .add_builder(DatasetRepositoryLocalFs::builder().with_root(datasets_dir_path))
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .bind::<dyn DatasetRepositoryWriter, DatasetRepositoryLocalFs>()
            .add::<DatasetRegistryRepoBridge>()
            .add::<RemoteAliasesRegistryImpl>()
            .add::<WatermarkServiceImpl>()
            .build();

        Self {
            dataset_registry: catalog.get_one().unwrap(),
            dataset_repo_writer: catalog.get_one().unwrap(),
            watermark_svc: catalog.get_one().unwrap(),
        }
    }

    async fn create_dataset(&self, dataset_alias: &DatasetAlias) -> CreateDatasetResult {
        self.dataset_repo_writer
            .create_dataset_from_snapshot(
                MetadataFactory::dataset_snapshot()
                    .name(DatasetAlias::new(None, dataset_alias.dataset_name.clone()))
                    .build(),
            )
            .await
            .unwrap()
            .create_dataset_result
    }

    async fn num_blocks(&self, dataset_alias: &DatasetAlias) -> usize {
        let ds = self
            .dataset_registry
            .get_dataset_by_ref(&dataset_alias.as_local_ref())
            .await
            .unwrap();

        use futures::StreamExt;
        ds.as_metadata_chain().iter_blocks().count().await
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
