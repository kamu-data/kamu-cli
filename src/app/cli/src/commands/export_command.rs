// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu::domain::{ExportFormat, ExportService};

use crate::{CLIError, Command};

pub struct ExportCommand {
    export_service: Arc<dyn ExportService>,
    dataset: String,
    output_path: String,
    output_format: String,
    partition_size: Option<usize>,
}

impl ExportCommand {
    pub fn new(
        export_service: Arc<dyn ExportService>,
        dataset: String,
        output_path: String,
        output_format: String,
        partition_size: Option<usize>,
    ) -> Self {
        Self {
            export_service,
            dataset,
            output_path,
            output_format,
            partition_size,
        }
    }

    fn parse_format(&self) -> Result<ExportFormat, CLIError> {
        match self.output_format.as_str() {
            "parquet" => Ok(ExportFormat::Parquet),
            "json" => Ok(ExportFormat::NdJson),
            "csv" => Ok(ExportFormat::Csv),
            _ => Err(CLIError::usage_error(format!(
                "Invalid output format '{}'. Supported formats: 'parquet', 'json', and 'csv'",
                &self.output_format
            ))),
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for ExportCommand {
    // async fn validate_args(&self) -> Result<(), CLIError> {
    //     self.parse_format().map(|_| ())
    // }

    async fn run(&mut self) -> Result<(), CLIError> {
        let format = self.parse_format()?;
        let query = format!("select * from '{}'", self.dataset);
        let rows_exported = self
            .export_service
            .export_to_fs(&query, &self.output_path, format, self.partition_size)
            .await?;
        eprintln!("Exported {} rows", rows_exported);
        Ok(())
    }
}
