// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu::domain::QueryService;
use opendatafabric::*;

use super::{CLIError, Command};
use crate::output::*;

pub struct TailCommand {
    query_svc: Arc<dyn QueryService>,
    dataset_ref: DatasetRef,
    skip: u64,
    limit: u64,
    output_cfg: Arc<OutputConfig>,
}

impl TailCommand {
    pub fn new(
        query_svc: Arc<dyn QueryService>,
        dataset_ref: DatasetRef,
        skip: u64,
        limit: u64,
        output_cfg: Arc<OutputConfig>,
    ) -> Self {
        Self {
            query_svc,
            dataset_ref,
            skip,
            limit,
            output_cfg,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for TailCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        let df = self
            .query_svc
            .tail(&self.dataset_ref, self.skip, self.limit)
            .await
            .map_err(|e| CLIError::failure(e))?;

        let record_batches = df.collect().await.map_err(|e| CLIError::failure(e))?;

        let mut writer = self.output_cfg.get_records_writer(RecordsFormat::default());
        writer.write_batches(&record_batches)?;
        writer.finish()?;
        Ok(())
    }
}
