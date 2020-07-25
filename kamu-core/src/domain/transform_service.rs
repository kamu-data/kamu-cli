use crate::domain::{DatasetID, DatasetIDBuf};

use thiserror::Error;

///////////////////////////////////////////////////////////////////////////////
// Service
///////////////////////////////////////////////////////////////////////////////

pub trait TransformService {
    fn transform(
        &mut self,
        dataset_id: &DatasetID,
        listener: Option<Box<dyn TransformListener>>,
    ) -> Result<TransformResult, TransformError>;

    fn transform_multi(
        &mut self,
        dataset_ids: &mut dyn Iterator<Item = &DatasetID>,
        listener: Option<Box<dyn TransformMultiListener>>,
    ) -> Vec<(DatasetIDBuf, Result<TransformResult, TransformError>)>;
}

#[derive(Debug)]
pub enum TransformResult {
    UpToDate,
    Updated { block_hash: String },
}

///////////////////////////////////////////////////////////////////////////////
// Listener
///////////////////////////////////////////////////////////////////////////////

pub trait TransformListener: Send {
    fn begin(&mut self) {}
    fn success(&mut self, _result: &TransformResult) {}
    fn error(&mut self, _error: &TransformError) {}
}

pub struct NullTransformListener;
impl TransformListener for NullTransformListener {}

pub trait TransformMultiListener {
    fn begin_transform(&mut self, _dataset_id: &DatasetID) -> Option<Box<dyn TransformListener>> {
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
    #[error("Engine error")]
    EngineError,
}
