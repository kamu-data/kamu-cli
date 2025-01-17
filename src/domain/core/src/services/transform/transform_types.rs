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
    TransformElaborateError,
    TransformExecuteError,
    TransformPlanError,
    VerifyTransformExecuteError,
    VerifyTransformPlanError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DTOs
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum TransformResult {
    UpToDate,
    Updated {
        old_head: odf::Multihash,
        new_head: odf::Multihash,
    },
}

#[derive(Clone, Copy, Debug, Default)]
pub struct TransformOptions {
    /// Run compaction of derivative datasets without saving data
    /// if transformation fails due to root dataset compaction
    pub reset_derivatives_on_diverged_input: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum TransformError {
    #[error(transparent)]
    Plan(
        #[from]
        #[backtrace]
        TransformPlanError,
    ),
    #[error(transparent)]
    Elaborate(
        #[from]
        #[backtrace]
        TransformElaborateError,
    ),
    #[error(transparent)]
    Execute(
        #[from]
        #[backtrace]
        TransformExecuteError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum VerifyTransformError {
    #[error(transparent)]
    Plan(
        #[from]
        #[backtrace]
        VerifyTransformPlanError,
    ),
    #[error(transparent)]
    Execute(
        #[from]
        #[backtrace]
        VerifyTransformExecuteError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
#[error("Dataset {dataset_handle} has not defined a schema yet")]
pub struct InputSchemaNotDefinedError {
    pub dataset_handle: odf::DatasetHandle,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("Invalid block interval [{head}, {tail}) in input dataset '{input_dataset_id}'")]
pub struct InvalidInputIntervalError {
    pub input_dataset_id: odf::DatasetID,
    pub head: odf::Multihash,
    pub tail: odf::Multihash,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
