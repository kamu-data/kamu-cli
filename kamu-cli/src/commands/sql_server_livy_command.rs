use super::{CLIError, Command};
use crate::output::*;
use kamu::domain::PullImageListener;
use kamu::infra::explore::*;
use kamu::infra::utils::docker_client::*;
use kamu::infra::*;

use console::style as s;
use slog::{o, Logger};
use std::sync::Arc;

pub struct SqlServerLivyCommand {
    workspace_layout: Arc<WorkspaceLayout>,
    volume_layout: Arc<VolumeLayout>,
    output_config: Arc<OutputConfig>,
    container_runtime: Arc<DockerClient>,
    logger: Logger,
    address: String,
    port: u16,
}

impl SqlServerLivyCommand {
    pub fn new(
        workspace_layout: Arc<WorkspaceLayout>,
        volume_layout: Arc<VolumeLayout>,
        output_config: Arc<OutputConfig>,
        container_runtime: Arc<DockerClient>,
        logger: Logger,
        address: &str,
        port: u16,
    ) -> Self {
        Self {
            workspace_layout,
            volume_layout,
            output_config,
            container_runtime: container_runtime,
            logger: logger,
            address: address.to_owned(),
            port: port,
        }
    }
}

impl Command for SqlServerLivyCommand {
    fn run(&mut self) -> Result<(), CLIError> {
        let livy_server = LivyServerImpl::new(self.container_runtime.clone());

        let spinner = if self.output_config.is_tty && self.output_config.verbosity_level == 0 {
            let mut pull_progress = PullImageProgress { progress_bar: None };
            livy_server.ensure_images(&mut pull_progress);

            let s = indicatif::ProgressBar::new_spinner();
            s.set_style(
                indicatif::ProgressStyle::default_spinner().template("{spinner:.cyan} {msg}"),
            );
            s.set_message("Starting Livy server");
            s.enable_steady_tick(100);
            Some(s)
        } else {
            None
        };

        let url = format!("{}:{}", self.address, self.port);

        livy_server.run(
            &self.address,
            self.port,
            &self.workspace_layout,
            &self.volume_layout,
            self.output_config.verbosity_level > 0,
            move || {
                if let Some(s) = spinner {
                    s.finish_and_clear()
                }
                eprintln!(
                    "{} {}",
                    s("Livy server is now running on:").green().bold(),
                    s(url).bold(),
                );
                eprintln!("{}", s("Use Ctrl+C to stop the server").yellow());
            },
            self.logger.new(o!()),
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
        s.set_message(format!("Pulling engine image {}", image));
        s.enable_steady_tick(100);
        self.progress_bar = Some(s);
    }

    fn success(&mut self) {
        self.progress_bar = None;
    }
}
