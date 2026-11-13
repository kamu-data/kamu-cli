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
    SetWatermarkError,
    SetWatermarkExecutionError,
    SetWatermarkExecutor,
    SetWatermarkPlan,
    SetWatermarkPlanner,
    SetWatermarkPlanningError,
    SetWatermarkResult,
    TenancyConfig,
};
use kamu_datasets::ResolvedDataset;
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
async fn test_set_watermark_keeps_source_state() {
    let harness = WatermarkTestHarness::new(TenancyConfig::SingleTenant);

    let source_state = odf::metadata::SourceState {
        source_name: odf::metadata::SourceState::DEFAULT_SOURCE_NAME.to_string(),
        kind: odf::metadata::SourceState::KIND_ETAG.to_string(),
        value: "e1".to_string(),
    };
    let foo = harness
        .create_root_dataset(&odf::DatasetAlias::new(
            None,
            odf::DatasetName::try_from("foo").unwrap(),
        ))
        .await;
    let foo = ResolvedDataset::from_created(&foo);
    harness
        .commit_source_state(&foo, source_state.clone())
        .await;

    let watermark = Utc.with_ymd_and_hms(2000, 1, 2, 0, 0, 0).unwrap();
    assert_matches!(
        harness.set_watermark(foo.clone(), watermark).await,
        Ok(SetWatermarkResult::Updated { .. })
    );
    assert_eq!(harness.head_source_state(&foo).await, Some(source_state));

    // Setting the same watermark again changes nothing
    assert_matches!(
        harness.set_watermark(foo.clone(), watermark).await,
        Ok(SetWatermarkResult::UpToDate)
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
        .add_remote_alias(
            &foo.dataset_handle,
            "odf+http://example.com/foo",
            RemoteAliasKind::Pull,
        )
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

    let concurrent_head = harness.move_head_concurrently(&foo).await;

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
        }
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

    /// Commits an `AddData` block that carries only `source_state`
    async fn commit_source_state(
        &self,
        target: &ResolvedDataset,
        source_state: odf::metadata::SourceState,
    ) {
        target
            .commit_add_data(
                odf::dataset::AddDataParams {
                    prev_checkpoint: None,
                    prev_offset: None,
                    new_offset_interval: None,
                    new_linked_objects: None,
                    new_watermark: None,
                    new_source_state: Some(source_state),
                },
                None,
                None,
                odf::dataset::CommitOpts::default(),
            )
            .await
            .unwrap();
    }

    /// Source state of the `AddData` block at `HEAD`
    async fn head_source_state(
        &self,
        target: &ResolvedDataset,
    ) -> Option<odf::metadata::SourceState> {
        let head = target
            .as_metadata_chain()
            .resolve_ref(&odf::BlockRef::Head)
            .await
            .unwrap();
        let block = target.as_metadata_chain().get_block(&head).await.unwrap();
        let odf::MetadataEvent::AddData(add_data) = block.event else {
            panic!("HEAD is not an AddData block: {block:?}");
        };
        add_data.new_source_state
    }

    async fn current_watermark(&self, target: ResolvedDataset) -> Option<DateTime<Utc>> {
        self.metadata_query_svc
            .try_get_current_watermark(&target)
            .await
            .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
