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
use kamu::domain::{QueryOptions, QueryService};
use kamu::*;
use kamu_datafusion_cli::exec;
use kamu_datafusion_cli::print_format::PrintFormat;
use kamu_datafusion_cli::print_options::{MaxRows, PrintOptions};

use super::common::PullImageProgress;
use super::{CLIError, Command};
use crate::explore::SqlShellImpl;
use crate::output::*;
use crate::WorkspaceLayout;

pub const DEFAULT_MAX_ROWS_FOR_OUTPUT: usize = 40;

pub struct SqlShellCommand {
    query_svc: Arc<dyn QueryService>,
    workspace_layout: Arc<WorkspaceLayout>,
    engine_prov_config: Arc<EngineProvisionerLocalConfig>,
    output_config: Arc<OutputConfig>,
    container_runtime: Arc<ContainerRuntime>,
    command: Option<String>,
    url: Option<String>,
    engine: Option<String>,
}

impl SqlShellCommand {
    pub fn new(
        query_svc: Arc<dyn QueryService>,
        workspace_layout: Arc<WorkspaceLayout>,
        engine_prov_config: Arc<EngineProvisionerLocalConfig>,
        output_config: Arc<OutputConfig>,
        container_runtime: Arc<ContainerRuntime>,
        command: Option<&str>,
        url: Option<&str>,
        engine: Option<&str>,
    ) -> Self {
        Self {
            query_svc,
            workspace_layout,
            engine_prov_config,
            output_config,
            container_runtime,
            command: command.map(ToOwned::to_owned),
            url: url.map(ToOwned::to_owned),
            engine: engine.map(ToOwned::to_owned),
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
                    OutputFormat::Parquet => Some("parquet"),
                    OutputFormat::NdJson => {
                        unimplemented!("Line-delimited Json is not yet supported by this command")
                    }
                    OutputFormat::JsonSoA => {
                        unimplemented!("SoA Json is not yet supported by this command")
                    }
                    OutputFormat::Table => Some("table"),
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
        let df = self
            .query_svc
            .sql_statement(self.command.as_ref().unwrap(), QueryOptions::default())
            .await
            .map_err(CLIError::failure)?;

        let records = df.collect().await.map_err(CLIError::failure)?;

        let mut writer = self
            .output_config
            .get_records_writer(RecordsFormat::default());
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

        let mut ctx = self.query_svc.create_session().await.unwrap();

        eprintln!(
            "{}",
            console::style("Kamu + DataFusion SQL shell. Type \\? for help.").dim()
        );

        exec::exec_from_repl(&mut ctx, &mut print_options)
            .await
            .map_err(CLIError::failure)?;

        Ok(())
    }
}

#[async_trait::async_trait(?Send)]
impl Command for SqlShellCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        let engine = self.engine.as_deref().unwrap_or("datafusion");

        match (engine, &self.command, &self.url) {
            ("datafusion", None, None) => self.run_datafusion_cli_command().await,
            ("datafusion", Some(_), None) => self.run_datafusion_command().await,
            ("spark", _, _) => self.run_spark_shell().await,
            _ => unreachable!(),
        }
    }
}
