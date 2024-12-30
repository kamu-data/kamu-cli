// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::*;
use internal_error::ResultIntoInternal;
use kamu_core::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn ResetPlanner)]
pub struct ResetPlannerImpl {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ResetPlanner for ResetPlannerImpl {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(target=%target.get_handle(), ?maybe_old_head, ?maybe_new_head)
    )]
    async fn plan_reset(
        &self,
        target: ResolvedDataset,
        maybe_new_head: Option<&odf::Multihash>,
        maybe_old_head: Option<&odf::Multihash>,
    ) -> Result<ResetPlan, ResetPlanningError> {
        use odf::dataset::MetadataChainExt;
        let new_head = if let Some(new_head) = maybe_new_head {
            new_head
        } else {
            &target
                .as_metadata_chain()
                .accept_one(odf::dataset::SearchSeedVisitor::new())
                .await
                .int_err()?
                .into_hashed_block()
                .unwrap()
                .0
        };

        if let Some(old_head) = maybe_old_head
            && let Some(current_head) = target
                .as_metadata_chain()
                .try_get_ref(&odf::BlockRef::Head)
                .await?
            && old_head != &current_head
        {
            return Err(ResetPlanningError::OldHeadMismatch(
                ResetOldHeadMismatchError {
                    current_head,
                    old_head: old_head.clone(),
                },
            ));
        }

        Ok(ResetPlan {
            new_head: new_head.clone(),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
