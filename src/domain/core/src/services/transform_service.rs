// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use opendatafabric::*;
use thiserror::Error;

use crate::engine::EngineError;
use crate::*;

///////////////////////////////////////////////////////////////////////////////
// Service
///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait TransformService: Send + Sync {
    /// Returns an active transform, if any
    async fn get_active_transform(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Result<Option<(Multihash, MetadataBlockTyped<SetTransform>)>, GetDatasetError>;

    async fn transform(
        &self,
        dataset_ref: &DatasetRef,
        transfrom_options: TransformOptions,
        listener: Option<Arc<dyn TransformListener>>,
    ) -> Result<TransformResult, TransformError>;

    async fn transform_multi(
        &self,
        dataset_refs: Vec<DatasetRef>,
        transfrom_options: TransformOptions,
        listener: Option<Arc<dyn TransformMultiListener>>,
    ) -> Vec<(DatasetRef, Result<TransformResult, TransformError>)>;

    async fn verify_transform(
        &self,
        dataset_ref: &DatasetRef,
        block_range: (Option<Multihash>, Option<Multihash>),
        listener: Option<Arc<dyn VerificationListener>>,
    ) -> Result<(), VerificationError>;
}

///////////////////////////////////////////////////////////////////////////////
// DTOs
///////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum TransformResult {
    UpToDate,
    Updated {
        old_head: Multihash,
        new_head: Multihash,
    },
}

#[derive(Clone, Copy, Debug, Default)]
pub struct TransformOptions {
    /// Run compaction of derivative datasets without saving data
    /// if transformation fails due to root dataset compaction
    pub reset_derivatives_on_diverged_input: bool,
}

///////////////////////////////////////////////////////////////////////////////
// Listeners
///////////////////////////////////////////////////////////////////////////////

pub trait TransformListener: Send + Sync {
    fn begin(&self) {}
    fn success(&self, _result: &TransformResult) {}
    fn error(&self, _error: &TransformError) {}

    fn get_engine_provisioning_listener(
        self: Arc<Self>,
    ) -> Option<Arc<dyn EngineProvisioningListener>> {
        None
    }
}

pub struct NullTransformListener;
impl TransformListener for NullTransformListener {}

///////////////////////////////////////////////////////////////////////////////

pub trait TransformMultiListener: Send + Sync {
    fn begin_transform(&self, _dataset: &DatasetHandle) -> Option<Arc<dyn TransformListener>> {
        None
    }
}

pub struct NullTransformMultiListener;
impl TransformMultiListener for NullTransformMultiListener {}

///////////////////////////////////////////////////////////////////////////////
// Errors
///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum TransformError {
    #[error(transparent)]
    DatasetNotFound(
        #[from]
        #[backtrace]
        DatasetNotFoundError,
    ),
    #[error(transparent)]
    TransformNotDefined(
        #[from]
        #[backtrace]
        TransformNotDefinedError,
    ),
    #[error(transparent)]
    EngineProvisioningError(
        #[from]
        #[backtrace]
        EngineProvisioningError,
    ),
    #[error(transparent)]
    EngineError(
        #[from]
        #[backtrace]
        EngineError,
    ),
    #[error(transparent)]
    CommitError(
        #[from]
        #[backtrace]
        CommitError,
    ),
    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        AccessError,
    ),
    #[error(transparent)]
    InvalidInterval(
        #[from]
        #[backtrace]
        InvalidIntervalError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

#[derive(Debug, thiserror::Error)]
#[error("Dataset does not define a transform")]
pub struct TransformNotDefinedError {}

impl From<GetDatasetError> for TransformError {
    fn from(v: GetDatasetError) -> Self {
        match v {
            GetDatasetError::NotFound(e) => Self::DatasetNotFound(e),
            GetDatasetError::Internal(e) => Self::Internal(e),
        }
    }
}

impl From<auth::DatasetActionUnauthorizedError> for TransformError {
    fn from(v: auth::DatasetActionUnauthorizedError) -> Self {
        match v {
            auth::DatasetActionUnauthorizedError::Access(e) => Self::Access(e),
            auth::DatasetActionUnauthorizedError::Internal(e) => Self::Internal(e),
        }
    }
}
