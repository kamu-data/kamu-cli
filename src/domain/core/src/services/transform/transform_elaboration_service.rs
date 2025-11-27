// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::InternalError;
use kamu_datasets::{ResolvedDataset, ResolvedDatasetsMap};
use thiserror::Error;

use super::TransformPreliminaryPlan;
use crate::engine::TransformRequestExt;
use crate::{
    InputSchemaNotDefinedError,
    InvalidInputIntervalError,
    TransformListener,
    TransformOptions,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait TransformElaborationService: Send + Sync {
    async fn elaborate_transform(
        &self,
        target: ResolvedDataset,
        plan: TransformPreliminaryPlan,
        transform_options: TransformOptions,
        maybe_listener: Option<Arc<dyn TransformListener>>,
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
    InputSchemaNotDefined(
        #[from]
        #[backtrace]
        InputSchemaNotDefinedError,
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
