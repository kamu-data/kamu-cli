// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use opendatafabric::{MetadataBlock, Multihash};
use thiserror::Error;

use crate::engine::TransformRequestExt;
use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait TransformRequestPlanner: Send + Sync {
    async fn collect_transform_plan(
        &self,
        targets: Vec<ResolvedDataset>,
        options: TransformOptions,
    ) -> (Vec<TransformPlanItem>, Vec<TransformPlanError>);

    async fn collect_verification_plan(
        &self,
        target: ResolvedDataset,
        block_range: (Option<Multihash>, Option<Multihash>),
    ) -> Result<Vec<VerificationStep>, VerifyTransformPlanError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub enum TransformPlanItem {
    ReadyToLaunch(TransformRequestExt),
    UpToDate,
    RetryAfterCompacting(InvalidInputIntervalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct VerificationStep {
    pub request: TransformRequestExt,
    pub expected_block: MetadataBlock,
    pub expected_hash: Multihash,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

#[derive(Debug, Error)]
pub enum VerifyTransformPlanError {
    #[error(transparent)]
    DatasetNotFound(
        #[from]
        #[backtrace]
        DatasetNotFoundError,
    ),
    #[error(transparent)]
    RefNotFound(
        #[from]
        #[backtrace]
        RefNotFoundError,
    ),
    #[error(transparent)]
    BlockNotFound(
        #[from]
        #[backtrace]
        BlockNotFoundError,
    ),
    #[error(transparent)]
    BlockVersion(
        #[from]
        #[backtrace]
        BlockVersionError,
    ),
    #[error(transparent)]
    BlockMalformed(
        #[from]
        #[backtrace]
        BlockMalformedError,
    ),
    #[error(transparent)]
    InvalidInterval(
        #[from]
        #[backtrace]
        InvalidIntervalError,
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
    Access(
        #[from]
        #[backtrace]
        AccessError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<GetDatasetError> for VerifyTransformPlanError {
    fn from(v: GetDatasetError) -> Self {
        match v {
            GetDatasetError::NotFound(e) => Self::DatasetNotFound(e),
            GetDatasetError::Internal(e) => Self::Internal(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<GetRefError> for VerifyTransformPlanError {
    fn from(v: GetRefError) -> Self {
        match v {
            GetRefError::NotFound(e) => Self::RefNotFound(e),
            GetRefError::Access(e) => Self::Access(e),
            GetRefError::Internal(e) => Self::Internal(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<GetBlockError> for VerifyTransformPlanError {
    fn from(v: GetBlockError) -> Self {
        match v {
            GetBlockError::NotFound(e) => Self::BlockNotFound(e),
            GetBlockError::BlockVersion(e) => Self::BlockVersion(e),
            GetBlockError::BlockMalformed(e) => Self::BlockMalformed(e),
            GetBlockError::Access(e) => Self::Access(e),
            GetBlockError::Internal(e) => Self::Internal(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
