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
use kamu::domain::error::*;
use kamu::infra::*;

use super::common::PullImageProgress;
use super::{CLIError, Command};
use crate::explore::SqlShellImpl;
use crate::output::*;

pub struct SqlServerCommand {
    workspace_layout: Arc<WorkspaceLayout>,
    engine_prov_config: Arc<EngineProvisionerLocalConfig>,
    output_config: Arc<OutputConfig>,
    container_runtime: Arc<ContainerRuntime>,
    address: IpAddr,
    port: u16,
}

impl SqlServerCommand {
    pub fn new(
        workspace_layout: Arc<WorkspaceLayout>,
        engine_prov_config: Arc<EngineProvisionerLocalConfig>,
        output_config: Arc<OutputConfig>,
        container_runtime: Arc<ContainerRuntime>,
        address: IpAddr,
        port: u16,
    ) -> Self {
        Self {
            workspace_layout,
            engine_prov_config,
            output_config,
            container_runtime,
            address,
            port,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for SqlServerCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        let sql_shell = SqlShellImpl::new(
            self.container_runtime.clone(),
            self.engine_prov_config.spark_image.clone(),
        );

        let spinner = if self.output_config.verbosity_level == 0 && !self.output_config.quiet {
            let mut pull_progress = PullImageProgress::new("engine");
            sql_shell
                .ensure_images(&mut pull_progress)
                .await
                .int_err()?;

            let s = indicatif::ProgressBar::new_spinner();
            let style = indicatif::ProgressStyle::default_spinner()
                .template("{spinner:.cyan} {msg}")
                .unwrap();
            s.set_style(style);
            s.set_message("Starting SQL server");
            s.enable_steady_tick(Duration::from_millis(100));
            Some(s)
        } else {
            None
        };

        let mut spark = sql_shell
            .run_server(
                &self.workspace_layout,
                Vec::new(),
                Some(&self.address),
                Some(self.port),
            )
            .await?;

        if let Some(s) = spinner {
            s.finish_and_clear();
        }
        eprintln!(
            "{}\n  {}",
            s("SQL server is now running at:").green().bold(),
            s(format!("jdbc:hive2://{}:{}", self.address, self.port)).bold(),
        );
        eprintln!("{}", s("Use Ctrl+C to stop the server").yellow());

        spark.wait().await?;

        Ok(())
    }
}
