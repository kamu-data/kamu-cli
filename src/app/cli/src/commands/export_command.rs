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

use kamu::domain::{ExportFormat, ExportOptions, ExportService, GetDataOptions, QueryService};

use crate::{CLIError, Command};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Command)]
pub struct ExportCommand {
    export_service: Arc<dyn ExportService>,
    query_service: Arc<dyn QueryService>,

    #[dill::component(explicit)]
    dataset_ref: odf::DatasetRef,

    #[dill::component(explicit)]
    output_path: Option<PathBuf>,

    #[dill::component(explicit)]
    output_format: ExportFormat,

    #[dill::component(explicit)]
    records_per_file: Option<usize>,

    #[dill::component(explicit)]
    quiet: bool,
}

#[async_trait::async_trait(?Send)]
impl Command for ExportCommand {
    async fn run(&self) -> Result<(), CLIError> {
        let res = self
            .query_service
            .get_data(&self.dataset_ref, GetDataOptions::default())
            .await
            .map_err(CLIError::failure)?;

        let Some(df) = res.df else {
            if !self.quiet {
                eprintln!(
                    "{}",
                    console::style("Dataset is empty and has no schema").yellow(),
                );
            }
            return Ok(());
        };

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
