use super::{Command, Error};
use crate::output::*;
use kamu::infra::explore::*;
use kamu::infra::*;

use slog::Logger;

pub struct SqlShellCommand {
    workspace_layout: WorkspaceLayout,
    volume_layout: VolumeLayout,
    output_config: OutputConfig,
    command: Option<String>,
    logger: Logger,
}

impl SqlShellCommand {
    pub fn new(
        workspace_layout: &WorkspaceLayout,
        volume_layout: &VolumeLayout,
        output_config: &OutputConfig,
        command: Option<&str>,
        logger: Logger,
    ) -> Self {
        Self {
            workspace_layout: workspace_layout.clone(),
            volume_layout: volume_layout.clone(),
            output_config: output_config.clone(),
            command: command.map(|v| v.to_owned()),
            logger: logger,
        }
    }
}

impl Command for SqlShellCommand {
    fn run(&mut self) -> Result<(), Error> {
        let spinner = if self.output_config.verbosity_level == 0 {
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

        SqlShellImpl::run(
            &self.workspace_layout,
            &self.volume_layout,
            match self.output_config.format {
                OutputFormat::Csv => Some("csv"),
                OutputFormat::Json => Some("json"),
                OutputFormat::Table => Some("table"),
            },
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
