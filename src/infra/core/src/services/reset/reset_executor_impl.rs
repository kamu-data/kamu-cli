// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::*;
use kamu_core::*;
use kamu_datasets::{DatasetReferenceUpdater, ResolvedDataset};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn ResetExecutor)]
pub struct ResetExecutorImpl {
    dataset_reference_updater: Arc<dyn DatasetReferenceUpdater>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl ResetExecutor for ResetExecutorImpl {
    #[tracing::instrument(name = ResetExecutorImpl_execute, level = "info", skip_all, fields(target=%target.get_handle(), new_head=%plan.new_head))]
    async fn execute(
        &self,
        target: ResolvedDataset,
        plan: ResetPlan,
    ) -> Result<ResetResult, ResetExecutionError> {
        self.dataset_reference_updater
            .update_reference(
                &target,
                &odf::BlockRef::Head,
                plan.old_head.as_ref(),
                &plan.new_head,
            )
            .await?;

        Ok(ResetResult {
            old_head: plan.old_head,
            new_head: plan.new_head,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
