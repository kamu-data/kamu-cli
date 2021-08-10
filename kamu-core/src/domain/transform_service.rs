use super::{DomainError, EngineError, PullImageListener};
use opendatafabric::{DatasetID, DatasetIDBuf, MetadataBlock, Sha3_256};

use std::backtrace::Backtrace;
use std::fmt::Display;
use std::sync::{Arc, Mutex};
use std::usize;
use thiserror::Error;

///////////////////////////////////////////////////////////////////////////////
// Service
///////////////////////////////////////////////////////////////////////////////

pub trait TransformService: Send + Sync {
    fn transform(
        &self,
        dataset_id: &DatasetID,
        listener: Option<Arc<Mutex<dyn TransformListener>>>,
    ) -> Result<TransformResult, TransformError>;

    fn transform_multi(
        &self,
        dataset_ids: &mut dyn Iterator<Item = &DatasetID>,
        listener: Option<Arc<Mutex<dyn TransformMultiListener>>>,
    ) -> Vec<(DatasetIDBuf, Result<TransformResult, TransformError>)>;

    fn verify(
        &self,
        dataset_id: &DatasetID,
        block_range: (Option<Sha3_256>, Option<Sha3_256>),
        options: VerificationOptions,
        listener: Option<Arc<Mutex<dyn TransformListener>>>,
    ) -> Result<VerificationResult, VerificationError>;
}

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
pub enum VerificationResult {
    Valid,
}

#[derive(Debug, Clone)]
pub struct VerificationOptions;

impl Default for VerificationOptions {
    fn default() -> Self {
        Self
    }
}

///////////////////////////////////////////////////////////////////////////////
// Listener
///////////////////////////////////////////////////////////////////////////////

pub trait TransformListener: Send {
    fn begin(&mut self) {}
    fn success(&mut self, _result: &TransformResult) {}
    fn error(&mut self, _error: &TransformError) {}

    fn get_pull_image_listener(&mut self) -> Option<&mut dyn PullImageListener> {
        None
    }
}

pub struct NullTransformListener;
impl TransformListener for NullTransformListener {}

pub trait TransformMultiListener {
    fn begin_transform(
        &mut self,
        _dataset_id: &DatasetID,
    ) -> Option<Arc<Mutex<dyn TransformListener>>> {
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

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum VerificationError {
    #[error("Dataset is not derivative")]
    NotDerivative,
    #[error("Block {0} not found")]
    NoSuchBlock(Sha3_256),
    #[error("Invalid data: {0}")]
    Invalid(VerificationErrorInvalid),
    #[error("Tranform error: {0}")]
    TransformError(#[from] TransformError),
    #[error("Domain error: {0}")]
    DomainError(#[from] DomainError),
}

#[derive(Debug)]
pub struct VerificationErrorInvalid {
    pub expected_block: MetadataBlock,
    pub actual_block: MetadataBlock,
}

impl Display for VerificationErrorInvalid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Expected block {:?} but got non-equivalent block {:?}",
            self.expected_block, self.actual_block
        )
    }
}
