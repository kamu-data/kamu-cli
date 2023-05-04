// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::engine::IngestRequest;
use crate::domain::*;
use crate::infra::*;
use opendatafabric::*;

use chrono::{DateTime, Utc};
use std::path::Path;
use std::sync::Arc;

pub struct ReadService {
    engine_provisioner: Arc<dyn EngineProvisioner>,
}

impl ReadService {
    pub fn new(engine_provisioner: Arc<dyn EngineProvisioner>) -> Self {
        Self { engine_provisioner }
    }

    // TODO: Don't use engine for anything but preprocessing
    pub async fn read<'a, 'b>(
        &'a self,
        dataset_handle: &'b DatasetHandle,
        dataset_layout: &'b DatasetLayout,
        source: &'b SetPollingSource,
        src_data_path: &'b Path,
        prev_watermark: Option<DateTime<Utc>>,
        prev_checkpoint: Option<Multihash>,
        vocab: &'b DatasetVocabulary,
        system_time: DateTime<Utc>,
        source_event_time: Option<DateTime<Utc>>,
        offset: i64,
        out_data_path: &'b Path,
        out_checkpoint_path: &'b Path,
        listener: Arc<dyn IngestListener>,
    ) -> Result<ExecuteQueryResponseSuccess, IngestError>
    where
        'a: 'b,
    {
        // Terminate early for zero-sized files
        // TODO: Should we still call an engine if only to propagate source_event_time to it?
        if src_data_path.metadata().int_err()?.len() == 0 {
            return Ok(ExecuteQueryResponseSuccess {
                data_interval: None,
                output_watermark: None,
            });
        }

        let engine = self
            .engine_provisioner
            .provision_ingest_engine(listener.get_engine_provisioning_listener())
            .await?;

        let request = IngestRequest {
            dataset_id: dataset_handle.id.clone(),
            dataset_name: dataset_handle.alias.dataset_name.clone(),
            ingest_path: src_data_path.to_owned(),
            system_time,
            event_time: source_event_time,
            offset,
            source: source.clone(),
            dataset_vocab: vocab.clone(),
            prev_watermark,
            prev_checkpoint_path: prev_checkpoint.map(|cp| dataset_layout.checkpoint_path(&cp)),
            data_dir: dataset_layout.data_dir.clone(),
            out_data_path: out_data_path.to_owned(),
            new_checkpoint_path: out_checkpoint_path.to_owned(),
        };

        let mut response = engine.ingest(request).await?;

        if let Some(data_interval) = &mut response.data_interval {
            if data_interval.end < data_interval.start || data_interval.start != offset {
                return Err(EngineError::contract_error(
                    "Engine returned an output slice with invalid data inverval",
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

        Ok(response)
    }
}
