// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Duration;

use container_runtime::ContainerRuntime;
use internal_error::*;
use kamu::domain::{ExportFormat, ExportService, QueryOptions, QueryService};
use kamu::*;
use kamu_datafusion_cli::exec;
use kamu_datafusion_cli::print_format::PrintFormat;
use kamu_datafusion_cli::print_options::{MaxRows, PrintOptions};

use super::common::PullImageProgress;
use super::{CLIError, Command};
use crate::explore::SqlShellImpl;
use crate::output::*;
use crate::WorkspaceLayout;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const DEFAULT_MAX_ROWS_FOR_OUTPUT: usize = 40;

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum SqlShellEngine {
    Datafusion,
    Spark,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SqlShellCommand {
    query_svc: Arc<dyn QueryService>,
    workspace_layout: Arc<WorkspaceLayout>,
    engine_prov_config: Arc<EngineProvisionerLocalConfig>,
    output_config: Arc<OutputConfig>,
    container_runtime: Arc<ContainerRuntime>,
    export_service: Arc<dyn ExportService>,
    command: Option<String>,
    url: Option<String>,
    engine: Option<SqlShellEngine>,
    output_path: Option<String>,
    partition_size: Option<usize>,
}

impl SqlShellCommand {
    pub fn new(
        query_svc: Arc<dyn QueryService>,
        workspace_layout: Arc<WorkspaceLayout>,
        engine_prov_config: Arc<EngineProvisionerLocalConfig>,
        output_config: Arc<OutputConfig>,
        container_runtime: Arc<ContainerRuntime>,
        export_service: Arc<dyn ExportService>,
        command: Option<String>,
        url: Option<String>,
        engine: Option<SqlShellEngine>,
        output_path: Option<String>,
        partition_size: Option<usize>,
    ) -> Self {
        Self {
            query_svc,
            workspace_layout,
            engine_prov_config,
            output_config,
            container_runtime,
            export_service,
            command,
            url,
            engine,
            output_path,
            partition_size,
        }
    }

    async fn run_spark_shell(&self) -> Result<(), CLIError> {
        let sql_shell = SqlShellImpl::new(
            self.container_runtime.clone(),
            self.engine_prov_config.spark_image.clone(),
        );

        let spinner = if self.output_config.verbosity_level == 0 && !self.output_config.quiet {
            let mut pull_progress = PullImageProgress::new("container");
            sql_shell
                .ensure_images(&mut pull_progress)
                .await
                .int_err()?;

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
                    _ => unimplemented!("Not supported format"), // todo: improve message
                };

                let rows_exported = self
                    .export_service
                    .export_to_fs(&command, &output_path, format, self.partition_size)
                    .await?;
                eprintln!("Exported {} rows", rows_exported);
            }
            _ => unreachable!(),
        }
        Ok(())
    }
}

#[async_trait::async_trait(?Send)]
impl Command for SqlShellCommand {
    async fn validate_args(&self) -> Result<(), CLIError> {
        // todo:
        // - output_path and partition_size set only for datafusion engine
        // - partition_size is set only when output_path is not empty
        Ok(())
    }

    async fn run(&mut self) -> Result<(), CLIError> {
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
