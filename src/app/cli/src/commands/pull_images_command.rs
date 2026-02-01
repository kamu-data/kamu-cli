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
use kamu::EngineProvisionerLocalConfig;

use super::{CLIError, Command};
use crate::config::JupyterConfig;

#[dill::component]
#[dill::interface(dyn Command)]
pub struct PullImagesCommand {
    container_runtime: Arc<ContainerRuntime>,
    engine_config: Arc<EngineProvisionerLocalConfig>,
    jupyter_config: Arc<JupyterConfig>,

    #[dill::component(explicit)]
    list_only: bool,
}

#[async_trait::async_trait(?Send)]
impl Command for PullImagesCommand {
    async fn run(&self) -> Result<(), CLIError> {
        let mut images = vec![
            self.engine_config.spark_image.as_str(),
            self.engine_config.flink_image.as_str(),
            self.engine_config.datafusion_image.as_str(),
            self.engine_config.risingwave_image.as_str(),
            self.jupyter_config.image.as_str(),
            self.jupyter_config.livy_image.as_str(),
        ];

        images.sort_unstable();
        images.dedup();

        if self.list_only {
            for img in images {
                println!("{img}");
            }
        } else {
            for img in images {
                eprintln!("{}: {}", console::style("Pulling image").bold(), img);
                self.container_runtime
                    .pull_image(img, None)
                    .await
                    .map_err(CLIError::failure)?;
            }
        }

        Ok(())
    }
}
