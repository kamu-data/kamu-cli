// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;
use crate::domain::*;
use crate::infra::*;
use opendatafabric::serde::yaml::formats::datetime_rfc3339;
use opendatafabric::serde::yaml::generated::MetadataBlockDef;
use opendatafabric::*;

use ::serde::{Deserialize, Serialize};
use ::serde_with::skip_serializing_none;
use chrono::{DateTime, Utc};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub struct ReadService {
    engine_provisioner: Arc<dyn EngineProvisioner>,
}

impl ReadService {
    pub fn new(engine_provisioner: Arc<dyn EngineProvisioner>) -> Self {
        Self { engine_provisioner }
    }

    // TODO: Don't use engine for anything but preprocessing
    pub fn read(
        &self,
        dataset_id: &DatasetID,
        dataset_layout: &DatasetLayout,
        source: &DatasetSourceRoot,
        prev_checkpoint: Option<Sha3_256>,
        vocab: &DatasetVocabulary,
        source_event_time: Option<DateTime<Utc>>,
        for_prepared_at: DateTime<Utc>,
        _old_checkpoint: Option<ReadCheckpoint>,
        src_path: &Path,
        listener: Arc<dyn IngestListener>,
    ) -> Result<ExecutionResult<ReadCheckpoint>, IngestError> {
        let engine = self
            .engine_provisioner
            .provision_ingest_engine(listener.get_engine_provisioning_listener())?;

        let out_data_path = dataset_layout.data_dir.join(".pending");
        let new_checkpoint_dir = dataset_layout.checkpoints_dir.join(".pending");

        // Clean up previous state leftovers
        if out_data_path.exists() {
            std::fs::remove_file(&out_data_path).map_err(|e| IngestError::internal(e))?;
        }
        if new_checkpoint_dir.exists() {
            std::fs::remove_dir_all(&new_checkpoint_dir).map_err(|e| IngestError::internal(e))?;
        }
        std::fs::create_dir_all(&new_checkpoint_dir).map_err(|e| IngestError::internal(e))?;

        let request = IngestRequest {
            dataset_id: dataset_id.to_owned(),
            ingest_path: src_path.to_owned(),
            event_time: source_event_time,
            source: source.clone(),
            dataset_vocab: vocab.clone(),
            prev_checkpoint_dir: prev_checkpoint
                .map(|hash| dataset_layout.checkpoints_dir.join(hash.to_string())),
            new_checkpoint_dir: new_checkpoint_dir.clone(),
            data_dir: dataset_layout.data_dir.clone(),
            out_data_path: out_data_path.clone(),
        };

        let response = engine.ingest(request)?;

        if let Some(ref slice) = response.metadata_block.output_slice {
            if slice.num_records == 0 {
                return Err(EngineError::contract_error(
                    "Engine returned an output slice with zero records",
                    Vec::new(),
                )
                .into());
            }
            if !out_data_path.exists() {
                return Err(EngineError::contract_error(
                    "Engine did not write a response data file",
                    Vec::new(),
                )
                .into());
            }
        }

        Ok(ExecutionResult {
            was_up_to_date: false,
            checkpoint: ReadCheckpoint {
                last_read: Utc::now(),
                for_prepared_at: for_prepared_at,
                last_block: response.metadata_block,
                new_checkpoint_dir: new_checkpoint_dir,
                out_data_path: out_data_path,
            },
        })
    }
}

#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReadCheckpoint {
    #[serde(with = "datetime_rfc3339")]
    pub last_read: DateTime<Utc>,
    #[serde(with = "datetime_rfc3339")]
    pub for_prepared_at: DateTime<Utc>,
    #[serde(with = "MetadataBlockDef")]
    pub last_block: MetadataBlock,
    pub new_checkpoint_dir: PathBuf,
    pub out_data_path: PathBuf,
}
