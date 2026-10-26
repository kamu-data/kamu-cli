// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use kamu_datasets::{ResolvedDataset, ResolvedDatasetsMap};
use thiserror::Error;

use super::TransformPreliminaryPlan;
use crate::engine::TransformRequestExt;
use crate::{InputNotPulledError, InvalidInputIntervalError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg_attr(feature = "testing", mockall::automock)]
#[async_trait::async_trait]
pub trait TransformElaborationService: Send + Sync {
    /// Reads the inputs and builds the transform request. Writes nothing.
    async fn elaborate_transform(
        &self,
        target: ResolvedDataset,
        plan: TransformPreliminaryPlan,
    ) -> Result<TransformElaboration, TransformElaborateError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum TransformElaboration {
    Elaborated(TransformPlan),
    UpToDate,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct TransformPlan {
    pub request: TransformRequestExt,
    pub datasets_map: ResolvedDatasetsMap,
}

impl std::fmt::Debug for TransformPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.request.fmt(f)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum TransformElaborateError {
    #[error(transparent)]
    InputNotPulled(
        #[from]
        #[backtrace]
        InputNotPulledError,
    ),
    #[error(transparent)]
    InvalidInputInterval(
        #[from]
        #[backtrace]
        InvalidInputIntervalError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
