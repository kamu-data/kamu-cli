use super::common::PullImageProgress;
use super::{CLIError, Command};
use crate::output::*;
use kamu::domain::{QueryOptions, QueryService};
use kamu::infra::explore::*;
use kamu::infra::utils::docker_client::DockerClient;
use kamu::infra::*;

use slog::Logger;
use std::sync::Arc;

pub struct SqlShellCommand {
    query_svc: Arc<dyn QueryService>,
    workspace_layout: Arc<WorkspaceLayout>,
    volume_layout: Arc<VolumeLayout>,
    output_config: Arc<OutputConfig>,
    container_runtime: Arc<DockerClient>,
    command: Option<String>,
    url: Option<String>,
    engine: Option<String>,
    logger: Logger,
}

impl SqlShellCommand {
    pub fn new(
        query_svc: Arc<dyn QueryService>,
        workspace_layout: Arc<WorkspaceLayout>,
        volume_layout: Arc<VolumeLayout>,
        output_config: Arc<OutputConfig>,
        container_runtime: Arc<DockerClient>,
        command: Option<&str>,
        url: Option<&str>,
        engine: Option<&str>,
        logger: Logger,
    ) -> Self {
        Self {
            query_svc,
            workspace_layout,
            volume_layout,
            output_config,
            container_runtime: container_runtime,
            command: command.map(|v| v.to_owned()),
            url: url.map(|v| v.to_owned()),
            engine: engine.map(|v| v.to_owned()),
            logger: logger,
        }
    }

    fn run_spark_shell(&self) -> Result<(), CLIError> {
        let sql_shell = SqlShellImpl::new(self.container_runtime.clone());

        let spinner = if self.output_config.verbosity_level == 0 && !self.output_config.quiet {
            let mut pull_progress = PullImageProgress::new("container");
            sql_shell.ensure_images(&mut pull_progress);

            let s = indicatif::ProgressBar::new_spinner();
            s.set_style(
                indicatif::ProgressStyle::default_spinner().template("{spinner:.cyan} {msg}"),
            );
            s.set_message("Starting Spark SQL shell");
            s.enable_steady_tick(100);
            Some(s)
        } else {
            None
        };

        sql_shell.run(
            &self.workspace_layout,
            &self.volume_layout,
            match self.output_config.format {
                OutputFormat::Csv => Some("csv"),
                OutputFormat::Json => Some("json"),
                OutputFormat::JsonLD => {
                    unimplemented!("Line-delimited Json is not yet supported by this command")
                }
                OutputFormat::JsonSoA => {
                    unimplemented!("SoA Json is not yet supported by this command")
                }
                OutputFormat::Table => Some("table"),
            },
            self.url.clone(),
            self.command.as_ref(),
            self.logger.clone(),
            || {
                if let Some(s) = spinner {
                    s.finish_and_clear()
                }
            },
        )?;

        Ok(())
    }

    fn run_datafusion_command(&self) -> Result<(), CLIError> {
        let df = self
            .query_svc
            .sql_statement(self.command.as_ref().unwrap(), QueryOptions::default())
            .map_err(|e| CLIError::failure(e))?;

        let runtime = tokio::runtime::Runtime::new().unwrap();
        let records = runtime
            .block_on(df.collect())
            .map_err(|e| CLIError::failure(e))?;

        let mut writer = self.output_config.get_records_writer();
        writer.write_batches(&records)?;
        writer.finish()?;

        Ok(())
    }
}

impl Command for SqlShellCommand {
    fn run(&mut self) -> Result<(), CLIError> {
        match (
            self.engine.as_ref().map(|s| s.as_str()),
            &self.command,
            &self.url,
        ) {
            (Some("datafusion"), Some(_), None) => self.run_datafusion_command(),
            (Some("datafusion"), _, _) => Err(CLIError::usage_error("DataFusion engine currently doesn't have a shell and only supports single command execution")),
            (Some("spark") | None, _, _) => self.run_spark_shell(),
            _ => unreachable!(),
        }
    }
}
