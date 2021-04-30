use super::{Command, Error};
use crate::output::*;
use kamu::domain::PullImageListener;
use kamu::infra::explore::*;
use kamu::infra::*;

use slog::Logger;

pub struct SqlShellCommand {
    workspace_layout: WorkspaceLayout,
    volume_layout: VolumeLayout,
    output_config: OutputConfig,
    command: Option<String>,
    url: Option<String>,
    logger: Logger,
}

impl SqlShellCommand {
    pub fn new(
        workspace_layout: &WorkspaceLayout,
        volume_layout: &VolumeLayout,
        output_config: &OutputConfig,
        command: Option<&str>,
        url: Option<&str>,
        logger: Logger,
    ) -> Self {
        Self {
            workspace_layout: workspace_layout.clone(),
            volume_layout: volume_layout.clone(),
            output_config: output_config.clone(),
            command: command.map(|v| v.to_owned()),
            url: url.map(|v| v.to_owned()),
            logger: logger,
        }
    }
}

impl Command for SqlShellCommand {
    fn run(&mut self) -> Result<(), Error> {
        let spinner = if self.output_config.verbosity_level == 0 {
            let mut pull_progress = PullImageProgress { progress_bar: None };
            SqlShellImpl::ensure_images(&mut pull_progress);

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

struct PullImageProgress {
    #[allow(dead_code)]
    progress_bar: Option<indicatif::ProgressBar>,
}

impl PullImageListener for PullImageProgress {
    fn begin(&mut self, image: &str) {
        let s = indicatif::ProgressBar::new_spinner();
        s.set_style(indicatif::ProgressStyle::default_spinner().template("{spinner:.cyan} {msg}"));
        s.set_message(&format!("Pulling docker image {}", image));
        s.enable_steady_tick(100);
        self.progress_bar = Some(s);
    }

    fn success(&mut self) {
        self.progress_bar = None;
    }
}
