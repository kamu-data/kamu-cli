// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::common::PullImageProgress;
use super::{CLIError, Command};
use crate::explore::LivyServerImpl;
use crate::output::*;
use container_runtime::ContainerRuntime;
use kamu::infra::*;

use console::style as s;
use std::sync::Arc;
use std::time::Duration;

pub struct SqlServerLivyCommand {
    workspace_layout: Arc<WorkspaceLayout>,
    output_config: Arc<OutputConfig>,
    container_runtime: Arc<ContainerRuntime>,
    address: String,
    port: u16,
}

impl SqlServerLivyCommand {
    pub fn new(
        workspace_layout: Arc<WorkspaceLayout>,
        output_config: Arc<OutputConfig>,
        container_runtime: Arc<ContainerRuntime>,
        address: &str,
        port: u16,
    ) -> Self {
        Self {
            workspace_layout,
            output_config,
            container_runtime,
            address: address.to_owned(),
            port,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for SqlServerLivyCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        let livy_server = LivyServerImpl::new(self.container_runtime.clone());

        let spinner = if self.output_config.is_tty
            && self.output_config.verbosity_level == 0
            && !self.output_config.quiet
        {
            let mut pull_progress = PullImageProgress::new("engine");
            livy_server.ensure_images(&mut pull_progress);

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

        let url = format!("{}:{}", self.address, self.port);

        livy_server.run(
            &self.address,
            self.port,
            &self.workspace_layout,
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
        )?;
        Ok(())
    }
}
