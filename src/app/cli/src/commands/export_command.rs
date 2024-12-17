// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu::domain::{ExportFormat, ExportService, QueryOptions, QueryResponse, QueryService};

use crate::{CLIError, Command};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ExportCommand {
    export_service: Arc<dyn ExportService>,
    query_service: Arc<dyn QueryService>,
    dataset: String,
    output_path: String,
    output_format: String,
    partition_size: Option<usize>,
    quiet: bool,
}

impl ExportCommand {
    pub fn new(
        export_service: Arc<dyn ExportService>,
        query_service: Arc<dyn QueryService>,
        dataset: String,
        output_path: String,
        output_format: String,
        partition_size: Option<usize>,
        quiet: bool,
    ) -> Self {
        Self {
            export_service,
            query_service,
            dataset,
            output_path,
            output_format,
            partition_size,
            quiet,
        }
    }

    fn parse_format(&self) -> Result<ExportFormat, CLIError> {
        match self.output_format.as_str() {
            "parquet" => Ok(ExportFormat::Parquet),
            "ndjson" => Ok(ExportFormat::NdJson),
            "csv" => Ok(ExportFormat::Csv),
            _ => Err(CLIError::usage_error(format!(
                "Invalid output format '{}'. Supported formats: 'parquet', 'ndjson', and 'csv'",
                &self.output_format
            ))),
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for ExportCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        let format = self.parse_format()?;
        let query = format!("select * from '{}'", self.dataset);
        let result: QueryResponse = self
            .query_service
            .sql_statement(&query, QueryOptions::default())
            .await
            .map_err(CLIError::failure)?; // todo: handle error
        let rows_exported = self
            .export_service
            .export_to_fs(result.df, &self.output_path, format, self.partition_size)
            .await?;

        if !self.quiet {
            eprintln!(
                "{} {} {}",
                console::style("Exported").green(),
                console::style(format!("{rows_exported}")).green().bold(),
                console::style("rows").green()
            );
        }
        Ok(())
    }
}
