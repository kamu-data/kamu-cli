use super::{EngineError, PullImageListener};
use opendatafabric::{DatasetID, DatasetIDBuf, Sha3_256};

use std::backtrace::Backtrace;
use std::sync::{Arc, Mutex};
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
}

#[derive(Debug)]
pub enum TransformResult {
    UpToDate,
    Updated { block_hash: Sha3_256 },
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
