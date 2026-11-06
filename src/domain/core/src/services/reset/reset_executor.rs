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
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::ResetPlan;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ResetExecutor: Send + Sync {
    /// Moves `HEAD` from `plan.old_head` to `plan.new_head` in the current
    /// transaction. Fails with `SetReferenceFailed(CASFailed)` when `HEAD`
    /// moved after planning.
    async fn execute(
        &self,
        target: ResolvedDataset,
        plan: ResetPlan,
    ) -> Result<ResetResult, ResetExecutionError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResetResult {
    pub old_head: Option<odf::Multihash>,
    pub new_head: odf::Multihash,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ResetExecutionError {
    #[error(transparent)]
    SetReferenceFailed(#[from] odf::dataset::SetChainRefError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
