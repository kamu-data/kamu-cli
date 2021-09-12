// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use super::{CLIError, Command};
use crate::output::*;
use kamu::domain::QueryService;

use opendatafabric::DatasetIDBuf;

pub struct TailCommand {
    query_svc: Arc<dyn QueryService>,
    dataset_id: DatasetIDBuf,
    num_records: u64,
    output_cfg: Arc<OutputConfig>,
}

impl TailCommand {
    pub fn new(
        query_svc: Arc<dyn QueryService>,
        dataset_id: DatasetIDBuf,
        num_records: u64,
        output_cfg: Arc<OutputConfig>,
    ) -> Self {
        Self {
            query_svc,
            dataset_id,
            num_records,
            output_cfg,
        }
    }
}

impl Command for TailCommand {
    fn run(&mut self) -> Result<(), CLIError> {
        let df = self
            .query_svc
            .tail(&self.dataset_id, self.num_records)
            .map_err(|e| CLIError::failure(e))?;

        let runtime = tokio::runtime::Runtime::new().unwrap();
        let record_batches = runtime
            .block_on(df.collect())
            .map_err(|e| CLIError::failure(e))?;

        let mut writer = self.output_cfg.get_records_writer();
        writer.write_batches(&record_batches)?;
        writer.finish()?;
        Ok(())
    }
}
