// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::{ErrorIntoInternal, InternalError};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{CompactionListener, CompactionPlan, ResolvedDataset};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait CompactionExecutor: Send + Sync {
    async fn execute(
        &self,
        target: ResolvedDataset,
        plan: CompactionPlan,
        maybe_listener: Option<Arc<dyn CompactionListener>>,
    ) -> Result<CompactionResult, CompactionExecutionError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum CompactionResult {
    NothingToDo,
    Success {
        old_head: odf::Multihash,
        new_head: odf::Multihash,
        old_num_blocks: usize,
        new_num_blocks: usize,
    },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum CompactionExecutionError {
    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        odf::AccessError,
    ),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<odf::dataset::SetChainRefError> for CompactionExecutionError {
    fn from(v: odf::dataset::SetChainRefError) -> Self {
        match v {
            odf::dataset::SetChainRefError::Access(e) => Self::Access(e),
            odf::dataset::SetChainRefError::Internal(e) => Self::Internal(e),
            _ => Self::Internal(v.int_err()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
