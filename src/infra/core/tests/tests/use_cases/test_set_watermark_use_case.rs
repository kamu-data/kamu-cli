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
use kamu::testing::MockDatasetActionAuthorizer;
use kamu::*;
use kamu_core::*;
use opendatafabric::*;

use super::{BaseUseCaseHarness, BaseUseCaseHarnessOptions};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_set_watermark_success() {
    let alias_foo = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));

    let harness = SetWatermarkUseCaseHarness::new(
        MockDatasetActionAuthorizer::new().expect_check_write_dataset(&alias_foo, 1, true),
    );

    let create_result_foo = harness.create_root_dataset(&alias_foo).await;

    let watermark = Utc::now() - TimeDelta::minutes(5);
    let result = harness
        .use_case
        .execute(&create_result_foo.dataset_handle, watermark)
        .await
        .unwrap();

    assert_matches!(result, SetWatermarkResult::Updated { .. });
    assert_eq!(
        harness.current_watermark(&create_result_foo).await,
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

    assert_matches!(
        harness
            .use_case
            .execute(&create_result_foo.dataset_handle, Utc::now())
            .await,
        Err(SetWatermarkError::Access(_))
    );
    assert_eq!(harness.current_watermark(&create_result_foo).await, None,);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseUseCaseHarness, base_harness)]
struct SetWatermarkUseCaseHarness {
    base_harness: BaseUseCaseHarness,
    use_case: Arc<dyn SetWatermarkUseCase>,
    watermark_svc: Arc<dyn WatermarkService>,
}

impl SetWatermarkUseCaseHarness {
    fn new(mock_dataset_action_authorizer: MockDatasetActionAuthorizer) -> Self {
        let base_harness = BaseUseCaseHarness::new(
            BaseUseCaseHarnessOptions::new().with_authorizer(mock_dataset_action_authorizer),
        );

        let catalog = dill::CatalogBuilder::new_chained(base_harness.catalog())
            .add::<SetWatermarkUseCaseImpl>()
            .add::<WatermarkServiceImpl>()
            .add::<RemoteAliasesRegistryImpl>()
            .build();

        let use_case = catalog.get_one().unwrap();
        let watermark_svc = catalog.get_one().unwrap();

        Self {
            base_harness,
            use_case,
            watermark_svc,
        }
    }

    async fn current_watermark(
        &self,
        created_result: &CreateDatasetResult,
    ) -> Option<DateTime<Utc>> {
        self.watermark_svc
            .try_get_current_watermark(created_result.dataset.clone())
            .await
            .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
