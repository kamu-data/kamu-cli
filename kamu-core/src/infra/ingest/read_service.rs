use super::*;
use crate::domain::*;
use crate::infra::serde::yaml::formats::datetime_rfc3339;
use crate::infra::serde::yaml::*;
use crate::infra::*;

use ::serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use serde_with::skip_serializing_none;
use std::path::Path;
use std::sync::{Arc, Mutex};

pub struct ReadService {
    engine_factory: Arc<Mutex<EngineFactory>>,
}

impl ReadService {
    pub fn new(engine_factory: Arc<Mutex<EngineFactory>>) -> Self {
        Self {
            engine_factory: engine_factory,
        }
    }

    // TODO: Don't use engine for anything but preprocessing
    pub fn read(
        &self,
        dataset_id: &DatasetID,
        dataset_layout: &DatasetLayout,
        source: &DatasetSourceRoot,
        source_event_time: Option<DateTime<Utc>>,
        vocab: &DatasetVocabulary,
        for_prepared_at: DateTime<Utc>,
        _old_checkpoint: Option<ReadCheckpoint>,
        src_path: &Path,
    ) -> Result<ExecutionResult<ReadCheckpoint>, IngestError> {
        let engine = self.engine_factory.lock().unwrap().get_engine("sparkSQL")?;

        let request = IngestRequest {
            dataset_id: dataset_id.to_owned(),
            ingest_path: src_path.to_owned(),
            event_time: source_event_time,
            source: source.clone(),
            dataset_vocab: vocab.clone(),
            checkpoints_dir: dataset_layout.checkpoints_dir.clone(),
            data_dir: dataset_layout.data_dir.clone(),
        };

        let response = engine.lock().unwrap().ingest(request)?;

        Ok(ExecutionResult {
            was_up_to_date: false,
            checkpoint: ReadCheckpoint {
                last_read: Utc::now(),
                for_prepared_at: for_prepared_at,
                last_block: response.block,
            },
        })
    }
}

#[skip_serializing_none]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadCheckpoint {
    #[serde(with = "datetime_rfc3339")]
    pub last_read: DateTime<Utc>,
    #[serde(with = "datetime_rfc3339")]
    pub for_prepared_at: DateTime<Utc>,
    pub last_block: MetadataBlock,
}
