// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::*;
use kamu_core::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn ResetExecutor)]
pub struct ResetExecutorImpl {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ResetExecutor for ResetExecutorImpl {
    #[tracing::instrument(level = "debug", skip_all, fields(target=%target.get_handle(), new_head=%plan.new_head))]
    async fn execute(
        &self,
        target: ResolvedDataset,
        plan: ResetPlan,
    ) -> Result<ResetResult, ResetExecutionError> {
        target
            .as_metadata_chain()
            .set_ref(
                &odf::BlockRef::Head,
                &plan.new_head,
                odf::dataset::SetRefOpts {
                    validate_block_present: true,
                    check_ref_is: None,
                },
            )
            .await?;

        Ok(ResetResult {
            old_head: plan.old_head,
            new_head: plan.new_head,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
