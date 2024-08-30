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
use crate::explore::LivyServerImpl;
use crate::output::*;
use crate::WorkspaceLayout;

pub struct SqlServerLivyCommand {
    workspace_layout: Arc<WorkspaceLayout>,
    jupyter_config: Arc<JupyterConfig>,
    output_config: Arc<OutputConfig>,
    container_runtime: Arc<ContainerRuntime>,
    address: Option<IpAddr>,
    port: Option<u16>,
}

impl SqlServerLivyCommand {
    pub fn new(
        workspace_layout: Arc<WorkspaceLayout>,
        jupyter_config: Arc<JupyterConfig>,
        output_config: Arc<OutputConfig>,
        container_runtime: Arc<ContainerRuntime>,
        address: Option<IpAddr>,
        port: Option<u16>,
    ) -> Self {
        Self {
            workspace_layout,
            jupyter_config,
            output_config,
            container_runtime,
            address,
            port,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for SqlServerLivyCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        let livy_server = LivyServerImpl::new(
            self.container_runtime.clone(),
            self.jupyter_config.livy_image.clone().unwrap(),
        );

        let spinner = if self.output_config.is_tty
            && self.output_config.verbosity_level == 0
            && !self.output_config.quiet
        {
            let mut pull_progress = PullImageProgress::new("engine");
            livy_server
                .ensure_images(&mut pull_progress)
                .await
                .int_err()?;

            let s = indicatif::ProgressBar::new_spinner();
            let style = indicatif::ProgressStyle::default_spinner()
                .template("{spinner:.cyan} {msg}")
                .unwrap();
            s.set_style(style);
            s.set_message("Starting Livy server");
            s.enable_steady_tick(Duration::from_millis(100));
            Some(s)
        } else {
            None
        };

        let address = self.address.unwrap_or("127.0.0.1".parse().unwrap());
        let port = self.port.unwrap_or(10000);
        let url = format!("{address}:{port}");

        livy_server
            .run(
                &address.to_string(),
                port,
                &self.workspace_layout.datasets_dir,
                &self.workspace_layout.run_info_dir,
                self.output_config.verbosity_level > 0,
                move || {
                    if let Some(s) = spinner {
                        s.finish_and_clear();
                    }
                    eprintln!(
                        "{} {}",
                        s("Livy server is now running on:").green().bold(),
                        s(url).bold(),
                    );
                    eprintln!("{}", s("Use Ctrl+C to stop the server").yellow());
                },
            )
            .await
            .int_err()?;
        Ok(())
    }
}
