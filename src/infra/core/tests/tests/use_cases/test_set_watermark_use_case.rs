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
use kamu::testing::{BaseUseCaseHarness, BaseUseCaseHarnessOptions};
use kamu::*;
use kamu_core::*;
use kamu_datasets::{CreateDatasetResult, ResolvedDataset};
use kamu_datasets_services::testing::MockDatasetActionAuthorizer;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_set_watermark_success() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

    let harness = SetWatermarkUseCaseHarness::new(
        MockDatasetActionAuthorizer::new().expect_check_write_dataset(&dataset_id_foo, 1, true),
        MockDidGenerator::predefined_dataset_ids(vec![dataset_id_foo]),
    );

    let foo = harness.create_root_dataset(&alias_foo).await;

    let watermark = Utc::now() - TimeDelta::minutes(5);
    let result = harness
        .use_case
        .execute(&foo.dataset_handle, watermark)
        .await
        .unwrap();

    assert_matches!(result, SetWatermarkResult::Updated { .. });
    assert_eq!(harness.current_watermark(&foo).await, Some(watermark));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_set_watermark_unauthorized() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

    let harness = SetWatermarkUseCaseHarness::new(
        MockDatasetActionAuthorizer::new().expect_check_write_dataset(&dataset_id_foo, 1, false),
        MockDidGenerator::predefined_dataset_ids(vec![dataset_id_foo]),
    );

    let foo = harness.create_root_dataset(&alias_foo).await;

    assert_matches!(
        harness
            .use_case
            .execute(&foo.dataset_handle, Utc::now())
            .await,
        Err(SetWatermarkError::Access(_))
    );
    assert_eq!(harness.current_watermark(&foo).await, None,);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseUseCaseHarness, base_use_case_harness)]
struct SetWatermarkUseCaseHarness {
    base_use_case_harness: BaseUseCaseHarness,
    use_case: Arc<dyn SetWatermarkUseCase>,
    metadata_query_svc: Arc<dyn MetadataQueryService>,
}

impl SetWatermarkUseCaseHarness {
    fn new(
        mock_dataset_action_authorizer: MockDatasetActionAuthorizer,
        mock_did_generator: MockDidGenerator,
    ) -> Self {
        let base_use_case_harness = BaseUseCaseHarness::new(
            BaseUseCaseHarnessOptions::new()
                .with_maybe_authorizer(Some(mock_dataset_action_authorizer))
                .with_maybe_mock_did_generator(Some(mock_did_generator)),
        );

        let catalog = dill::CatalogBuilder::new_chained(base_use_case_harness.catalog())
            .add::<SetWatermarkUseCaseImpl>()
            .add::<SetWatermarkPlannerImpl>()
            .add::<SetWatermarkExecutorImpl>()
            .add::<RemoteAliasesRegistryImpl>()
            .add::<MetadataQueryServiceImpl>()
            .build();

        Self {
            base_use_case_harness,
            use_case: catalog.get_one().unwrap(),
            metadata_query_svc: catalog.get_one().unwrap(),
        }
    }

    async fn current_watermark(
        &self,
        created_result: &CreateDatasetResult,
    ) -> Option<DateTime<Utc>> {
        self.metadata_query_svc
            .try_get_current_watermark(&ResolvedDataset::from_created(created_result))
            .await
            .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
