// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use opendatafabric as odf;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{ResetPlan, ResolvedDataset, SetRefError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ResetExecutionService: Send + Sync {
    async fn execute_reset(
        &self,
        target: ResolvedDataset,
        plan: ResetPlan,
    ) -> Result<ResetResult, ResetExecutionError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResetResult {
    pub new_head: odf::Multihash,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ResetExecutionError {
    #[error(transparent)]
    SetReferenceFailed(#[from] SetRefError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
