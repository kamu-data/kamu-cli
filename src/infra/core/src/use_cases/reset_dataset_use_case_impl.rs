// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::{component, interface};
use internal_error::ErrorIntoInternal;
use kamu_auth_rebac::{RebacDatasetIdUnresolvedError, RebacDatasetRegistryFacade};
use kamu_core::{auth, ResetDatasetUseCase, ResetError, ResetExecutor, ResetPlanner, ResetResult};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn ResetDatasetUseCase)]
pub struct ResetDatasetUseCaseImpl {
    reset_planner: Arc<dyn ResetPlanner>,
    reset_executor: Arc<dyn ResetExecutor>,
    rebac_dataset_registry_facade: Arc<dyn RebacDatasetRegistryFacade>,
}

impl ResetDatasetUseCaseImpl {
    pub fn new(
        reset_planner: Arc<dyn ResetPlanner>,
        reset_executor: Arc<dyn ResetExecutor>,
        rebac_dataset_registry_facade: Arc<dyn RebacDatasetRegistryFacade>,
    ) -> Self {
        Self {
            reset_planner,
            reset_executor,
            rebac_dataset_registry_facade,
        }
    }
}

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl ResetDatasetUseCase for ResetDatasetUseCaseImpl {
    #[tracing::instrument(
        level = "info",
        name = ResetDatasetUseCaseImpl_execute,
        skip_all,
        fields(dataset_handle, ?maybe_new_head, ?maybe_old_head)
    )]
    async fn execute(
        &self,
        dataset_handle: &odf::DatasetHandle,
        maybe_new_head: Option<&odf::Multihash>,
        maybe_old_head: Option<&odf::Multihash>,
    ) -> Result<ResetResult, ResetError> {
        // Resolve dataset
        let target = self
            .rebac_dataset_registry_facade
            .resolve_dataset_for_action_by_handle(dataset_handle, auth::DatasetAction::Maintain)
            .await
            .map_err(|e| {
                use RebacDatasetIdUnresolvedError as E;
                match e {
                    E::Access(e) => ResetError::Access(e),
                    e @ E::Internal(_) => ResetError::Internal(e.int_err()),
                }
            })?;

        // Make a plan
        let reset_plan = self
            .reset_planner
            .plan_reset(target.clone(), maybe_new_head, maybe_old_head)
            .await?;

        // Execute the plan
        let reset_result = self.reset_executor.execute(target, reset_plan).await?;

        Ok(reset_result)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
