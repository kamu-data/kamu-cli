// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use console::style as s;
use container_runtime::ContainerRuntime;
use internal_error::*;

use super::common::PullImageProgress;
use super::{CLIError, Command};
use crate::config::JupyterConfig;
use crate::explore::NotebookServerImpl;
use crate::output::OutputConfig;
use crate::WorkspaceLayout;

pub struct NotebookCommand {
    workspace_layout: Arc<WorkspaceLayout>,
    jupyter_config: Arc<JupyterConfig>,
    output_config: Arc<OutputConfig>,
    container_runtime: Arc<ContainerRuntime>,
    address: Option<IpAddr>,
    port: Option<u16>,
    env_vars: Vec<(String, Option<String>)>,
}

impl NotebookCommand {
    pub fn new<Iter, Str>(
        workspace_layout: Arc<WorkspaceLayout>,
        jupyter_config: Arc<JupyterConfig>,
        output_config: Arc<OutputConfig>,
        container_runtime: Arc<ContainerRuntime>,
        address: Option<IpAddr>,
        port: Option<u16>,
        env_vars: Iter,
    ) -> Self
    where
        Iter: IntoIterator<Item = Str>,
        Str: AsRef<str>,
    {
        Self {
            workspace_layout,
            jupyter_config,
            output_config,
            container_runtime,
            address,
            port,
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
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for NotebookCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        let notebook_server =
            NotebookServerImpl::new(self.container_runtime.clone(), self.jupyter_config.clone());

        let environment_vars = self
            .env_vars
            .iter()
            .map(|(name, value)| {
                value
                    .clone()
                    .or_else(|| std::env::var(name).ok())
                    .ok_or_else(|| {
                        CLIError::usage_error(format!("Environment variable {} is not set", name))
                    })
                    .map(|v| (name.to_owned(), v))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let spinner = if self.output_config.verbosity_level == 0 && !self.output_config.quiet {
            let pull_progress = PullImageProgress::new("container");
            notebook_server
                .ensure_images(&pull_progress)
                .await
                .int_err()?;

            let s = indicatif::ProgressBar::new_spinner();
            let style = indicatif::ProgressStyle::default_spinner()
                .template("{spinner:.cyan} {msg}")
                .unwrap();
            s.set_style(style);
            s.set_message("Starting Jupyter server");
            s.enable_steady_tick(Duration::from_millis(100));
            Some(s)
        } else {
            None
        };

        notebook_server
            .run(
                &self.workspace_layout.datasets_dir,
                &self.workspace_layout.run_info_dir,
                self.address.clone(),
                self.port,
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
            )
            .await?;
        Ok(())
    }
}
