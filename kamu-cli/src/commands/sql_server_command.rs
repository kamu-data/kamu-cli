use super::{Command, Error};
use crate::output::*;
use kamu::domain::PullImageListener;
use kamu::infra::explore::*;
use kamu::infra::utils::docker_client::*;
use kamu::infra::*;

use console::style as s;
use slog::Logger;

pub struct SqlServerCommand {
    workspace_layout: WorkspaceLayout,
    volume_layout: VolumeLayout,
    output_config: OutputConfig,
    logger: Logger,
    address: String,
    port: u16,
}

impl SqlServerCommand {
    pub fn new(
        workspace_layout: &WorkspaceLayout,
        volume_layout: &VolumeLayout,
        output_config: &OutputConfig,
        logger: Logger,
        address: &str,
        port: u16,
    ) -> Self {
        Self {
            workspace_layout: workspace_layout.clone(),
            volume_layout: volume_layout.clone(),
            output_config: output_config.clone(),
            logger: logger,
            address: address.to_owned(),
            port: port,
        }
    }
}

impl Command for SqlServerCommand {
    fn run(&mut self) -> Result<(), Error> {
        let spinner = if self.output_config.verbosity_level == 0 {
            let mut pull_progress = PullImageProgress { progress_bar: None };
            SqlShellImpl::ensure_images(&mut pull_progress);

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

        let mut spark = SqlShellImpl::run_server(
            &self.workspace_layout,
            &self.volume_layout,
            self.logger.clone(),
            Some(&self.address),
            Some(self.port),
        )?;

        // TODO: Move into a container whapper type
        let docker_client = DockerClient::new();
        let _drop_spark = DropContainer::new(docker_client.clone(), "kamu-spark");

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
        s.set_message(&format!("Pulling engine image {}", image));
        s.enable_steady_tick(100);
        self.progress_bar = Some(s);
    }

    fn success(&mut self) {
        self.progress_bar = None;
    }
}
