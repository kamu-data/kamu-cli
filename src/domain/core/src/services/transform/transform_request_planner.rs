// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use datafusion::arrow::datatypes::SchemaRef;
use internal_error::InternalError;
use thiserror::Error;

use crate::engine::TransformRequestExt;
use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait TransformRequestPlanner: Send + Sync {
    async fn build_transform_preliminary_plan(
        &self,
        target: ResolvedDataset,
    ) -> Result<TransformPreliminaryPlan, TransformPlanError>;

    async fn build_transform_verification_plan(
        &self,
        target: ResolvedDataset,
        block_range: (Option<odf::Multihash>, Option<odf::Multihash>),
    ) -> Result<VerifyTransformOperation, VerifyTransformPlanError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct TransformPreliminaryPlan {
    pub preliminary_request: TransformPreliminaryRequestExt,
    pub datasets_map: ResolvedDatasetsMap,
}

impl std::fmt::Debug for TransformPreliminaryPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.preliminary_request.fmt(f)
    }
}

#[derive(Debug, Clone)]
pub struct TransformPreliminaryRequestExt {
    /// Randomly assigned value that identifies this specific engine operation
    pub operation_id: String,
    /// Identifies the output dataset
    pub dataset_handle: odf::DatasetHandle,
    /// Block reference to advance upon commit
    pub block_ref: odf::BlockRef,
    /// Current head (for concurrency control)
    pub head: odf::Multihash,
    /// Transformation that will be applied to produce new data
    pub transform: odf::metadata::Transform,
    /// System time to use for new records
    pub system_time: DateTime<Utc>,
    /// Expected data schema (if already defined)
    pub schema: Option<SchemaRef>,
    /// Preceding record offset, if any
    pub prev_offset: Option<u64>,
    /// State of inputs
    pub input_states: Vec<(
        odf::metadata::TransformInput,
        Option<odf::metadata::ExecuteTransformInput>,
    )>,
    /// Output dataset's vocabulary
    pub vocab: odf::metadata::DatasetVocabulary,
    /// Previous checkpoint, if any
    pub prev_checkpoint: Option<odf::Multihash>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct VerifyTransformStep {
    pub request: TransformRequestExt,
    pub expected_block: odf::MetadataBlock,
    pub expected_hash: odf::Multihash,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct VerifyTransformOperation {
    pub steps: Vec<VerifyTransformStep>,
    pub datasets_map: ResolvedDatasetsMap,
}

impl std::fmt::Debug for VerifyTransformOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.steps.iter()).finish()
    }
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
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
#[error("Dataset does not define a transform")]
pub struct TransformNotDefinedError {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum VerifyTransformPlanError {
    #[error(transparent)]
    DatasetNotFound(
        #[from]
        #[backtrace]
        odf::DatasetNotFoundError,
    ),

    #[error(transparent)]
    RefNotFound(
        #[from]
        #[backtrace]
        odf::storage::RefNotFoundError,
    ),

    #[error(transparent)]
    BlockNotFound(
        #[from]
        #[backtrace]
        odf::storage::BlockNotFoundError,
    ),

    #[error(transparent)]
    BlockVersion(
        #[from]
        #[backtrace]
        odf::storage::BlockVersionError,
    ),

    #[error(transparent)]
    BlockMalformed(
        #[from]
        #[backtrace]
        odf::storage::BlockMalformedError,
    ),

    #[error(transparent)]
    InvalidInterval(
        #[from]
        #[backtrace]
        odf::dataset::InvalidIntervalError,
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
        odf::AccessError,
    ),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<odf::GetRefError> for VerifyTransformPlanError {
    fn from(v: odf::GetRefError) -> Self {
        match v {
            odf::GetRefError::NotFound(e) => Self::RefNotFound(e),
            odf::GetRefError::Access(e) => Self::Access(e),
            odf::GetRefError::Internal(e) => Self::Internal(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<odf::GetBlockError> for VerifyTransformPlanError {
    fn from(v: odf::GetBlockError) -> Self {
        match v {
            odf::GetBlockError::NotFound(e) => Self::BlockNotFound(e),
            odf::GetBlockError::BlockVersion(e) => Self::BlockVersion(e),
            odf::GetBlockError::BlockMalformed(e) => Self::BlockMalformed(e),
            odf::GetBlockError::Access(e) => Self::Access(e),
            odf::GetBlockError::Internal(e) => Self::Internal(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
