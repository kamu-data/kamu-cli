// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use container_runtime::ContainerRuntime;
use domain::ExportOptions;
use internal_error::*;
use itertools::Itertools;
use kamu::domain::{ExportFormat, ExportService, QueryOptions, QueryService};
use kamu::*;
use kamu_datafusion_cli::exec;
use kamu_datafusion_cli::print_format::PrintFormat;
use kamu_datafusion_cli::print_options::{MaxRows, PrintOptions};

use super::common::PullImageProgress;
use super::{CLIError, Command};
use crate::WorkspaceLayout;
use crate::explore::SqlShellImpl;
use crate::output::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const DEFAULT_MAX_ROWS_FOR_OUTPUT: usize = 40;

static SUPPORTED_EXPORT_FORMATS: LazyLock<Vec<OutputFormat>> = LazyLock::new(|| {
    vec![
        OutputFormat::Csv,
        OutputFormat::NdJson,
        OutputFormat::Parquet,
    ]
});

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum SqlShellEngine {
    Datafusion,
    Spark,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Command)]
pub struct SqlShellCommand {
    query_svc: Arc<dyn QueryService>,
    workspace_layout: Arc<WorkspaceLayout>,
    engine_prov_config: Arc<EngineProvisionerLocalConfig>,
    output_config: Arc<OutputConfig>,
    container_runtime: Arc<ContainerRuntime>,
    export_service: Arc<dyn ExportService>,

    #[dill::component(explicit)]
    command: Option<String>,

    #[dill::component(explicit)]
    url: Option<String>,

    #[dill::component(explicit)]
    engine: Option<SqlShellEngine>,

    #[dill::component(explicit)]
    output_path: Option<PathBuf>,

    #[dill::component(explicit)]
    records_per_file: Option<usize>,
}

impl SqlShellCommand {
    async fn run_spark_shell(&self) -> Result<(), CLIError> {
        let sql_shell = SqlShellImpl::new(
            self.container_runtime.clone(),
            self.engine_prov_config.spark_image.clone(),
        );

        let mut pull_progress = PullImageProgress::new(self.output_config.clone(), "container");
        sql_shell
            .ensure_images(&mut pull_progress)
            .await
            .int_err()?;

        let spinner = if self.output_config.verbosity_level == 0 && !self.output_config.quiet {
            let s = indicatif::ProgressBar::new_spinner();
            let style = indicatif::ProgressStyle::default_spinner()
                .template("{spinner:.cyan} {msg}")
                .unwrap();
            s.set_style(style);
            s.set_message("Starting Spark SQL shell");
            s.enable_steady_tick(Duration::from_millis(100));
            Some(s)
        } else {
            None
        };

        sql_shell
            .run(
                &self.workspace_layout,
                match self.output_config.format {
                    OutputFormat::Csv => Some("csv"),
                    OutputFormat::Json => Some("json"),
                    OutputFormat::Table => Some("table"),
                    OutputFormat::NdJson | OutputFormat::JsonSoA | OutputFormat::JsonAoA => {
                        unimplemented!(
                            "{:?} format is not yet supported by this command",
                            self.output_config.format
                        )
                    }
                    OutputFormat::Parquet => {
                        unimplemented!("Parquet format is applicable for data export only")
                    }
                },
                self.url.clone(),
                self.command.as_ref(),
                || {
                    if let Some(s) = spinner {
                        s.finish_and_clear();
                    }
                },
            )
            .await?;

        Ok(())
    }

    async fn run_datafusion_command(&self) -> Result<(), CLIError> {
        let res = self
            .query_svc
            .sql_statement(self.command.as_ref().unwrap(), QueryOptions::default())
            .await
            .map_err(CLIError::failure)?;

        let mut writer = self
            .output_config
            .get_records_writer(res.df.schema().as_arrow(), RecordsFormat::default());

        let records = res.df.collect().await.map_err(CLIError::failure)?;

        writer.write_batches(&records)?;
        writer.finish()?;

        Ok(())
    }

    async fn run_datafusion_cli_command(&self) -> Result<(), CLIError> {
        let mut print_options = PrintOptions {
            format: PrintFormat::Table,
            quiet: false,
            color: true,
            maxrows: MaxRows::Limited(DEFAULT_MAX_ROWS_FOR_OUTPUT),
            instrumented_registry: Arc::new(kamu_datafusion_cli::object_storage::instrumented::InstrumentedObjectStoreRegistry::new()),
        };

        let ctx = self.query_svc.create_session().await.unwrap();

        eprintln!(
            "{}",
            console::style("Kamu + DataFusion SQL shell. Type \\? for help.").dim()
        );

        Box::pin(exec::exec_from_repl(&ctx, &mut print_options))
            .await
            .map_err(CLIError::failure)?;

        Ok(())
    }

    async fn run_datafusion_export_command(&self) -> Result<(), CLIError> {
        match (&self.command, &self.output_path) {
            (Some(command), Some(output_path)) => {
                let format = match self.output_config.format {
                    OutputFormat::Csv => ExportFormat::Csv,
                    OutputFormat::NdJson => ExportFormat::NdJson,
                    OutputFormat::Parquet => ExportFormat::Parquet,
                    not_supported => {
                        // Normally should be unreachable, as the case should be caught by
                        // `validate_args` function.
                        unimplemented!("Format {:?} is not supported.", &not_supported)
                    }
                };

                let res = self
                    .query_svc
                    .sql_statement(command, QueryOptions::default())
                    .await
                    .map_err(CLIError::failure)?;

                let options = ExportOptions {
                    format,
                    records_per_file: self.records_per_file,
                };
                let rows_exported = self
                    .export_service
                    .export_to_fs(res.df, output_path, options)
                    .await?;
                eprintln!("Exported {rows_exported} rows");
            }
            _ => unreachable!(),
        }
        Ok(())
    }
}

#[async_trait::async_trait(?Send)]
impl Command for SqlShellCommand {
    async fn validate_args(&self) -> Result<(), CLIError> {
        // Bulk export related checks
        if self.output_path.is_some() {
            match self.engine {
                None | Some(SqlShellEngine::Datafusion) => (),
                _ => {
                    return Err(CLIError::usage_error(
                        "Data export to file(s) is available with DataFusion (default) engine only",
                    ));
                }
            }

            if !SUPPORTED_EXPORT_FORMATS.contains(&self.output_config.format) {
                let supported_str = SUPPORTED_EXPORT_FORMATS
                    .iter()
                    .map(|f| format!("'{f}'"))
                    .join(", ");
                return Err(CLIError::usage_error(format!(
                    "Invalid output format for export '{}'. Supported formats: {}",
                    &self.output_config.format, supported_str
                )));
            }
        } else if self.records_per_file.is_some() {
            return Err(CLIError::usage_error(
                "Partitioning is only supported for data export to file(s)",
            ));
        }

        Ok(())
    }

    async fn run(&self) -> Result<(), CLIError> {
        let engine = self.engine.unwrap_or(SqlShellEngine::Datafusion);

        match (engine, &self.command, &self.url, &self.output_path) {
            (SqlShellEngine::Datafusion, None, None, None) => {
                self.run_datafusion_cli_command().await
            }
            (SqlShellEngine::Datafusion, Some(_), None, Some(_)) => {
                self.run_datafusion_export_command().await
            }
            (SqlShellEngine::Datafusion, Some(_), None, _) => self.run_datafusion_command().await,
            (SqlShellEngine::Spark, _, _, _) => self.run_spark_shell().await,
            _ => unreachable!(),
        }
    }
}
