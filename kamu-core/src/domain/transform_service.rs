// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{DomainError, EngineError, PullImageListener};
use opendatafabric::{DatasetID, DatasetIDBuf, MetadataBlock, Sha3_256};

use std::backtrace::Backtrace;
use std::fmt::Display;
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

    fn verify(
        &self,
        dataset_id: &DatasetID,
        block_range: (Option<Sha3_256>, Option<Sha3_256>),
        options: VerificationOptions,
        listener: Option<Arc<dyn VerificationListener>>,
    ) -> Result<VerificationResult, VerificationError>;

    fn verify_multi(
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

#[derive(Debug)]
pub struct VerificationRequest<'a> {
    pub dataset_id: &'a DatasetID,
    pub block_range: (Option<Sha3_256>, Option<Sha3_256>),
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum VerificationResult {
    Valid { blocks_verified: usize },
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct VerificationOptions;

impl Default for VerificationOptions {
    fn default() -> Self {
        Self
    }
}

///////////////////////////////////////////////////////////////////////////////
// Listeners
///////////////////////////////////////////////////////////////////////////////

pub trait TransformListener: Send + Sync {
    fn begin(&self) {}
    fn success(&self, _result: &TransformResult) {}
    fn error(&self, _error: &TransformError) {}

    fn get_pull_image_listener(self: Arc<Self>) -> Option<Arc<dyn PullImageListener>> {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerificationPhase {
    HashData,
    ReplayTransform,
    BlockValid,
}

pub trait VerificationListener {
    fn begin(&self, _num_blocks: usize) {}
    fn on_phase(
        &self,
        _block: &Sha3_256,
        _block_index: usize,
        _num_blocks: usize,
        _phase: VerificationPhase,
    ) {
    }
    fn success(&self, _result: &VerificationResult) {}
    fn error(&self, _error: &VerificationError) {}

    fn get_transform_listener(self: Arc<Self>) -> Option<Arc<dyn TransformListener>> {
        None
    }
}

pub struct NullVerificationListener;
impl VerificationListener for NullVerificationListener {}

///////////////////////////////////////////////////////////////////////////////

pub trait VerificationMultiListener {
    fn begin_verify(&self, _dataset_id: &DatasetID) -> Option<Arc<dyn VerificationListener>> {
        None
    }
}

pub struct NullVerificationMultiListener;
impl VerificationMultiListener for NullVerificationMultiListener {}

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

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum VerificationError {
    #[error("Dataset is not derivative")]
    NotDerivative,
    #[error("Block {0} not found")]
    NoSuchBlock(Sha3_256),
    #[error("Data doesn't match metadata: {0}")]
    DataDoesNotMatchMetadata(DataDoesNotMatchMetadata),
    #[error("Data is not reproducible: {0}")]
    DataNotReproducible(DataNotReproducible),
    #[error("Tranform error: {0}")]
    TransformError(#[from] TransformError),
    #[error("Domain error: {0}")]
    DomainError(#[from] DomainError),
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct DataDoesNotMatchMetadata {
    pub block_hash: Sha3_256,
    pub expected_hash: String,
    pub actual_hash: String,
}

impl Display for DataDoesNotMatchMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Data hash for block {} is expected to be {} but actual {}",
            self.block_hash, self.expected_hash, self.actual_hash
        )
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct DataNotReproducible {
    pub expected_block: MetadataBlock,
    pub actual_block: MetadataBlock,
}

impl Display for DataNotReproducible {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Expected block {:?} but got non-equivalent block {:?}",
            self.expected_block, self.actual_block
        )
    }
}
