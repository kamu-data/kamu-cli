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

pub struct ResetExecutionServiceImpl {}

#[component(pub)]
#[interface(dyn ResetExecutionService)]
impl ResetExecutionServiceImpl {
    pub fn new() -> Self {
        Self {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ResetExecutionService for ResetExecutionServiceImpl {
    async fn execute_reset(
        &self,
        target: ResolvedDataset,
        plan: ResetPlan,
    ) -> Result<ResetResult, ResetExecutionError> {
        target
            .as_metadata_chain()
            .set_ref(
                &BlockRef::Head,
                &plan.new_head,
                SetRefOpts {
                    validate_block_present: true,
                    check_ref_is: None,
                },
            )
            .await?;

        Ok(ResetResult {
            new_head: plan.new_head,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
