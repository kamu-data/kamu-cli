// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use container_runtime::ContainerRuntime;
use kamu::infra::EngineProvisionerLocalConfig;

use super::{CLIError, Command};
use crate::JupyterConfig;

pub struct PullImagesCommand {
    container_runtime: Arc<ContainerRuntime>,
    engine_config: Arc<EngineProvisionerLocalConfig>,
    jupyter_config: Arc<JupyterConfig>,
    list_only: bool,
}

impl PullImagesCommand {
    pub fn new<'a>(
        container_runtime: Arc<ContainerRuntime>,
        engine_config: Arc<EngineProvisionerLocalConfig>,
        jupyter_config: Arc<JupyterConfig>,
        list_only: bool,
    ) -> Self {
        Self {
            container_runtime,
            engine_config,
            jupyter_config,
            list_only,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for PullImagesCommand {
    fn needs_workspace(&self) -> bool {
        false
    }

    async fn run(&mut self) -> Result<(), CLIError> {
        let mut images = vec![
            self.engine_config.spark_image.as_str(),
            self.engine_config.flink_image.as_str(),
            self.jupyter_config.image.as_ref().unwrap().as_str(),
            self.jupyter_config.livy_image.as_ref().unwrap().as_str(),
        ];

        images.sort();
        images.dedup();

        if self.list_only {
            for img in images {
                println!("{}", img);
            }
        } else {
            for img in images {
                eprintln!("{}: {}", console::style("Pulling image").bold(), img);
                self.container_runtime
                    .pull_cmd(&img)
                    .status()?
                    .exit_ok()
                    .map_err(|e| CLIError::failure(e))?;
            }
        }

        Ok(())
    }
}
