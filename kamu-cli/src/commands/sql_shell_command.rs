use super::common::PullImageProgress;
use super::{CLIError, Command};
use crate::output::*;
use kamu::infra::explore::*;
use kamu::infra::utils::docker_client::DockerClient;
use kamu::infra::*;

use slog::Logger;
use std::sync::Arc;

pub struct SqlShellCommand {
    workspace_layout: Arc<WorkspaceLayout>,
    volume_layout: Arc<VolumeLayout>,
    output_config: Arc<OutputConfig>,
    container_runtime: Arc<DockerClient>,
    command: Option<String>,
    url: Option<String>,
    logger: Logger,
}

impl SqlShellCommand {
    pub fn new(
        workspace_layout: Arc<WorkspaceLayout>,
        volume_layout: Arc<VolumeLayout>,
        output_config: Arc<OutputConfig>,
        container_runtime: Arc<DockerClient>,
        command: Option<&str>,
        url: Option<&str>,
        logger: Logger,
    ) -> Self {
        Self {
            workspace_layout,
            volume_layout,
            output_config,
            container_runtime: container_runtime,
            command: command.map(|v| v.to_owned()),
            url: url.map(|v| v.to_owned()),
            logger: logger,
        }
    }
}

impl Command for SqlShellCommand {
    fn run(&mut self) -> Result<(), CLIError> {
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
}
