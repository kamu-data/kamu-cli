use super::{CLIError, Command};
use crate::output::*;
use kamu::domain::PullImageListener;
use kamu::infra::explore::*;
use kamu::infra::utils::docker_client::*;
use kamu::infra::*;

use console::style as s;
use slog::Logger;
use std::sync::Arc;

pub struct SqlServerCommand {
    workspace_layout: Arc<WorkspaceLayout>,
    volume_layout: Arc<VolumeLayout>,
    output_config: Arc<OutputConfig>,
    container_runtime: Arc<DockerClient>,
    logger: Logger,
    address: String,
    port: u16,
}

impl SqlServerCommand {
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

impl Command for SqlServerCommand {
    fn run(&mut self) -> Result<(), CLIError> {
        let sql_shell = SqlShellImpl::new(self.container_runtime.clone());

        let spinner = if self.output_config.verbosity_level == 0 {
            let mut pull_progress = PullImageProgress { progress_bar: None };
            sql_shell.ensure_images(&mut pull_progress);

            let s = indicatif::ProgressBar::new_spinner();
            s.set_style(
                indicatif::ProgressStyle::default_spinner().template("{spinner:.cyan} {msg}"),
            );
            s.set_message("Starting SQL server");
            s.enable_steady_tick(100);
            Some(s)
        } else {
            None
        };

        let mut spark = sql_shell.run_server(
            &self.workspace_layout,
            &self.volume_layout,
            Vec::new(),
            self.logger.clone(),
            Some(&self.address),
            Some(self.port),
        )?;

        // TODO: Move into a container whapper type
        let _drop_spark = DropContainer::new(self.container_runtime.clone(), "kamu-spark");

        if let Some(s) = spinner {
            s.finish_and_clear();
        }
        eprintln!(
            "{}\n  {}",
            s("SQL server is now running at:").green().bold(),
            s(format!("jdbc:hive2://{}:{}", self.address, self.port)).bold(),
        );
        eprintln!("{}", s("Use Ctrl+C to stop the server").yellow());

        spark.wait()?;

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
