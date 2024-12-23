// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;
use std::sync::Arc;

use kamu::domain::{ExportFormat, ExportOptions, ExportService, QueryService};

use crate::{CLIError, Command};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ExportCommand {
    export_service: Arc<dyn ExportService>,
    query_service: Arc<dyn QueryService>,
    dataset_ref: odf::DatasetRef,
    output_path: Option<PathBuf>,
    output_format: ExportFormat,
    records_per_file: Option<usize>,
    quiet: bool,
}

impl ExportCommand {
    pub fn new(
        export_service: Arc<dyn ExportService>,
        query_service: Arc<dyn QueryService>,
        dataset_ref: odf::DatasetRef,
        output_path: Option<PathBuf>,
        output_format: ExportFormat,
        records_per_file: Option<usize>,
        quiet: bool,
    ) -> Self {
        Self {
            export_service,
            query_service,
            dataset_ref,
            output_path,
            output_format,
            records_per_file,
            quiet,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for ExportCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        let df = self
            .query_service
            .get_data(&self.dataset_ref)
            .await
            .map_err(CLIError::failure)?;

        let mut default_path: PathBuf = PathBuf::new();
        default_path.push(self.dataset_ref.to_string());
        default_path.push(""); // ensure trailing slash to have it as a dir

        let output_path = match self.output_path {
            Some(ref path) => path,
            None => &default_path,
        };

        let options = ExportOptions {
            format: self.output_format.clone(),
            records_per_file: self.records_per_file,
        };
        let rows_exported = self
            .export_service
            .export_to_fs(df, output_path, options)
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
