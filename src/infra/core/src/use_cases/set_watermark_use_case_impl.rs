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
use internal_error::ErrorIntoInternal;
use kamu_auth_rebac::RebacDatasetRegistryFacade;
use kamu_core::*;
use kamu_datasets::DatasetAction;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn SetWatermarkUseCase)]
pub struct SetWatermarkUseCaseImpl {
    set_watermark_planner: Arc<dyn SetWatermarkPlanner>,
    set_watermark_executor: Arc<dyn SetWatermarkExecutor>,
    rebac_dataset_registry_facade: Arc<dyn RebacDatasetRegistryFacade>,
}

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl SetWatermarkUseCase for SetWatermarkUseCaseImpl {
    #[tracing::instrument(
        level = "info",
        name = SetWatermarkUseCaseImpl_execute,
        skip_all,
        fields(dataset_handle, new_watermark)
    )]
    async fn execute(
        &self,
        dataset_handle: &odf::DatasetHandle,
        new_watermark: DateTime<Utc>,
    ) -> Result<SetWatermarkResult, SetWatermarkError> {
        // Resolve dataset
        let target = self
            .rebac_dataset_registry_facade
            .resolve_dataset_by_handle(dataset_handle, DatasetAction::Write)
            .await
            .map_err(|e| {
                use kamu_auth_rebac::RebacDatasetIdUnresolvedError as E;
                match e {
                    E::Access(e) => SetWatermarkError::Access(e),
                    e @ E::Internal(_) => SetWatermarkError::Internal(e.int_err()),
                }
            })?;

        // Make a plan
        let plan = self
            .set_watermark_planner
            .plan_set_watermark(target.clone(), new_watermark)
            .await?;

        // Execute the plan
        let result = self.set_watermark_executor.execute(target, plan).await?;

        Ok(result)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
