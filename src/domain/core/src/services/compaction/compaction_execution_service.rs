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
use opendatafabric as odf;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{AccessError, CompactionListener, CompactionPlan, ResolvedDataset, SetRefError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait CompactionExecutionService: Send + Sync {
    async fn execute_compaction(
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
        AccessError,
    ),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<SetRefError> for CompactionExecutionError {
    fn from(v: SetRefError) -> Self {
        match v {
            SetRefError::Access(e) => Self::Access(e),
            SetRefError::Internal(e) => Self::Internal(e),
            _ => Self::Internal(v.int_err()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
