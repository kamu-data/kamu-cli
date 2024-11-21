// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use dill::{component, interface};
use kamu_core::auth::{DatasetAction, DatasetActionAuthorizer};
use kamu_core::{
    DatasetRegistry,
    SetWatermarkError,
    SetWatermarkResult,
    SetWatermarkUseCase,
    WatermarkService,
};
use opendatafabric::DatasetHandle;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn SetWatermarkUseCase)]
pub struct SetWatermarkUseCaseImpl {
    watermark_service: Arc<dyn WatermarkService>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
}

impl SetWatermarkUseCaseImpl {
    pub fn new(
        watermark_service: Arc<dyn WatermarkService>,
        dataset_registry: Arc<dyn DatasetRegistry>,
        dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
    ) -> Self {
        Self {
            watermark_service,
            dataset_registry,
            dataset_action_authorizer,
        }
    }
}

#[async_trait::async_trait]
impl SetWatermarkUseCase for SetWatermarkUseCaseImpl {
    #[tracing::instrument(
        level = "info",
        name = "SetWatermarkUseCase::execute",
        skip_all,
        fields(dataset_handle, new_watermark)
    )]
    async fn execute(
        &self,
        dataset_handle: &DatasetHandle,
        new_watermark: DateTime<Utc>,
    ) -> Result<SetWatermarkResult, SetWatermarkError> {
        // Permission check
        self.dataset_action_authorizer
            .check_action_allowed(dataset_handle, DatasetAction::Write)
            .await?;

        // Resolve dataset
        let resolved_dataset = self.dataset_registry.get_dataset_by_handle(dataset_handle);

        // Actual action
        self.watermark_service
            .set_watermark(resolved_dataset, new_watermark)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
