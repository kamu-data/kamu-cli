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
use internal_error::ResultIntoInternal;
use kamu_core::*;
use opendatafabric::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn ResetService)]
pub struct ResetServiceImpl {}

#[async_trait::async_trait]
impl ResetService for ResetServiceImpl {
    #[tracing::instrument(level = "info", skip_all, fields(new_head = ?new_head_maybe, old_head = ?old_head_maybe))]
    async fn reset_dataset(
        &self,
        dataset: Arc<dyn Dataset>,
        new_head_maybe: Option<&Multihash>,
        old_head_maybe: Option<&Multihash>,
    ) -> Result<Multihash, ResetError> {
        let new_head = if let Some(new_head) = new_head_maybe {
            new_head
        } else {
            &dataset
                .as_metadata_chain()
                .accept_one(SearchSeedVisitor::new())
                .await
                .int_err()?
                .into_hashed_block()
                .unwrap()
                .0
        };
        if let Some(old_head) = old_head_maybe
            && let Some(current_head) = dataset
                .as_metadata_chain()
                .try_get_ref(&BlockRef::Head)
                .await?
            && old_head != &current_head
        {
            return Err(ResetError::OldHeadMismatch(OldHeadMismatchError {
                current_head,
                old_head: old_head.clone(),
            }));
        }

        dataset
            .as_metadata_chain()
            .set_ref(
                &BlockRef::Head,
                new_head,
                SetRefOpts {
                    validate_block_present: true,
                    check_ref_is: None,
                },
            )
            .await?;

        Ok(new_head.clone())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
