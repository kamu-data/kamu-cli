// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use thiserror::Error;

use super::{
    InputSchemaNotDefinedError,
    InvalidInputIntervalError,
    TransformNotDefinedError,
    TransformOptions,
};
use crate::engine::TransformRequestExt;
use crate::ResolvedDataset;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait TransformRequestPlanner: Send + Sync {
    async fn collect_transform_plan(
        &self,
        targets: Vec<ResolvedDataset>,
        options: TransformOptions,
    ) -> (Vec<TransformPlanItem>, Vec<TransformPlanError>);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub enum TransformPlanItem {
    ReadyToLaunch(TransformRequestExt),
    UpToDate,
    RetryAfterCompacting(InvalidInputIntervalError),
}

#[derive(Debug, Error)]
pub enum TransformPlanError {
    #[error(transparent)]
    TransformNotDefined(
        #[from]
        #[backtrace]
        TransformNotDefinedError,
    ),
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
