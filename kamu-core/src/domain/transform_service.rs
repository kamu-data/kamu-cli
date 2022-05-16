// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::*;
use opendatafabric::*;

use std::sync::Arc;
use std::usize;
use thiserror::Error;

///////////////////////////////////////////////////////////////////////////////
// Service
///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait(?Send)]
pub trait TransformService: Send + Sync {
    async fn transform(
        &self,
        dataset_ref: &DatasetRefLocal,
        listener: Option<Arc<dyn TransformListener>>,
    ) -> Result<TransformResult, TransformError>;

    async fn transform_multi(
        &self,
        dataset_refs: &mut dyn Iterator<Item = DatasetRefLocal>,
        listener: Option<Arc<dyn TransformMultiListener>>,
    ) -> Vec<(DatasetRefLocal, Result<TransformResult, TransformError>)>;

    async fn verify_transform(
        &self,
        dataset_ref: &DatasetRefLocal,
        block_range: (Option<Multihash>, Option<Multihash>),
        options: VerificationOptions,
        listener: Option<Arc<dyn VerificationListener>>,
    ) -> Result<VerificationResult, VerificationError>;

    async fn verify_transform_multi(
        &self,
        datasets: &mut dyn Iterator<Item = VerificationRequest>,
        options: VerificationOptions,
        listener: Option<Arc<dyn VerificationMultiListener>>,
    ) -> Result<VerificationResult, VerificationError>;
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
        num_blocks: usize,
    },
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

pub trait TransformMultiListener {
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
    #[error("Engine provisioning error")]
    EngineProvisioningError(
        #[from]
        #[backtrace]
        EngineProvisioningError,
    ),
    #[error("Engine error")]
    EngineError(
        #[from]
        #[backtrace]
        EngineError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

impl From<GetDatasetError> for TransformError {
    fn from(v: GetDatasetError) -> Self {
        match v {
            GetDatasetError::NotFound(e) => Self::DatasetNotFound(e),
            GetDatasetError::Internal(e) => Self::Internal(e),
        }
    }
}
