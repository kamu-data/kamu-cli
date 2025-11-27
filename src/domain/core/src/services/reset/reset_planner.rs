// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use kamu_datasets::ResolvedDataset;
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ResetPlanner: Send + Sync {
    async fn plan_reset(
        &self,
        target: ResolvedDataset,
        maybe_new_head: Option<&odf::Multihash>,
        maybe_old_head: Option<&odf::Multihash>,
    ) -> Result<ResetPlan, ResetPlanningError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct ResetPlan {
    pub old_head: Option<odf::Multihash>,
    pub new_head: odf::Multihash,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ResetPlanningError {
    #[error(transparent)]
    OldHeadMismatch(
        #[from]
        #[backtrace]
        ResetOldHeadMismatchError,
    ),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("Current head is {current_head} but expected head is {old_head}")]
pub struct ResetOldHeadMismatchError {
    pub current_head: odf::Multihash,
    pub old_head: odf::Multihash,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
