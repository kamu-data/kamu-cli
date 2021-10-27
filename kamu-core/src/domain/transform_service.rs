// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{
    EngineError, EngineProvisioningListener, VerificationError, VerificationListener,
    VerificationMultiListener, VerificationOptions, VerificationRequest, VerificationResult,
};
use opendatafabric::{DatasetID, DatasetIDBuf, Sha3_256};

use std::backtrace::Backtrace;
use std::sync::Arc;
use std::usize;
use thiserror::Error;

///////////////////////////////////////////////////////////////////////////////
// Service
///////////////////////////////////////////////////////////////////////////////

pub trait TransformService: Send + Sync {
    fn transform(
        &self,
        dataset_id: &DatasetID,
        listener: Option<Arc<dyn TransformListener>>,
    ) -> Result<TransformResult, TransformError>;

    fn transform_multi(
        &self,
        dataset_ids: &mut dyn Iterator<Item = &DatasetID>,
        listener: Option<Arc<dyn TransformMultiListener>>,
    ) -> Vec<(DatasetIDBuf, Result<TransformResult, TransformError>)>;

    fn verify_transform(
        &self,
        dataset_id: &DatasetID,
        block_range: (Option<Sha3_256>, Option<Sha3_256>),
        options: VerificationOptions,
        listener: Option<Arc<dyn VerificationListener>>,
    ) -> Result<VerificationResult, VerificationError>;

    fn verify_transform_multi(
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
        old_head: Sha3_256,
        new_head: Sha3_256,
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
    fn begin_transform(&self, _dataset_id: &DatasetID) -> Option<Arc<dyn TransformListener>> {
        None
    }
}

pub struct NullTransformMultiListener;
impl TransformMultiListener for NullTransformMultiListener {}

///////////////////////////////////////////////////////////////////////////////
// Errors
///////////////////////////////////////////////////////////////////////////////

type BoxedError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, Error)]
pub enum TransformError {
    #[error("Engine error: {0}")]
    EngineError(#[from] EngineError),
    #[error("Internal error: {source}")]
    InternalError {
        #[from]
        source: BoxedError,
        backtrace: Backtrace,
    },
}

impl TransformError {
    pub fn internal(e: impl std::error::Error + Send + Sync + 'static) -> Self {
        TransformError::InternalError {
            source: e.into(),
            backtrace: Backtrace::capture(),
        }
    }
}
