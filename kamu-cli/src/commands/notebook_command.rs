use super::{CLIError, Command};
use crate::output::OutputConfig;
use kamu::infra::explore::*;
use kamu::infra::*;
use kamu::{domain::PullImageListener, infra::utils::docker_client::DockerClient};

use console::style as s;
use slog::{o, Logger};
use std::sync::Arc;

pub struct NotebookCommand {
    workspace_layout: Arc<WorkspaceLayout>,
    volume_layout: Arc<VolumeLayout>,
    container_runtime: Arc<DockerClient>,
    output_config: Arc<OutputConfig>,
    env_vars: Vec<(String, Option<String>)>,
    logger: Logger,
}

impl NotebookCommand {
    pub fn new<Iter, Str>(
        workspace_layout: Arc<WorkspaceLayout>,
        volume_layout: Arc<VolumeLayout>,
        output_config: Arc<OutputConfig>,
        container_runtime: Arc<DockerClient>,
        env_vars: Iter,
        logger: Logger,
    ) -> Self
    where
        Iter: IntoIterator<Item = Str>,
        Str: AsRef<str>,
    {
        Self {
            workspace_layout,
            volume_layout,
            container_runtime: container_runtime,
            output_config,
            env_vars: env_vars
                .into_iter()
                .map(|elem| {
                    let s = elem.as_ref();
                    match s.find("=") {
                        None => (s.to_owned(), None),
                        Some(pos) => {
                            let (name, value) = s.split_at(pos);
                            (name.to_owned(), Some(value[1..].to_owned()))
                        }
                    }
                })
                .collect(),
            logger: logger,
        }
    }
}

impl Command for NotebookCommand {
    fn run(&mut self) -> Result<(), CLIError> {
        let notebook_server = NotebookServerImpl::new(self.container_runtime.clone());

        let environment_vars = self
            .env_vars
            .iter()
            .map(|(name, value)| {
                value
                    .clone()
                    .or_else(|| std::env::var(name).ok())
                    .ok_or_else(|| CLIError::UsageError {
                        msg: format!("Environment variable {} is not set", name),
                    })
                    .map(|v| (name.to_owned(), v))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let spinner = if self.output_config.verbosity_level == 0 {
            let mut pull_progress = PullImageProgress { progress_bar: None };
            notebook_server.ensure_images(&mut pull_progress);

            let s = indicatif::ProgressBar::new_spinner();
            s.set_style(
                indicatif::ProgressStyle::default_spinner().template("{spinner:.cyan} {msg}"),
            );
            s.set_message("Starting Jupyter server");
            s.enable_steady_tick(100);
            Some(s)
        } else {
            None
        };

        notebook_server.run(
            &self.workspace_layout,
            &self.volume_layout,
            environment_vars,
            self.output_config.verbosity_level > 0,
            move |url| {
                if let Some(s) = spinner {
                    s.finish_and_clear()
                }
                eprintln!(
                    "{}\n  {}",
                    s("Jupyter server is now running at:").green().bold(),
                    s(url).bold(),
                );
                eprintln!("{}", s("Use Ctrl+C to stop the server").yellow());
                let _ = webbrowser::open(url);
            },
            || eprintln!("{}", s("Shutting down").yellow()),
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
        s.set_message(format!("Pulling container image {}", image));
        s.enable_steady_tick(100);
        self.progress_bar = Some(s);
    }

    fn success(&mut self) {
        self.progress_bar = None;
    }
}
