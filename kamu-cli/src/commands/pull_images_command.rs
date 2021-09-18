// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use super::{CLIError, Command};
use container_runtime::ContainerRuntime;
use kamu::infra::utils::docker_images;

pub struct PullImagesCommand {
    container_runtime: Arc<ContainerRuntime>,
    pull_test_deps: bool,
    list_only: bool,
}

impl PullImagesCommand {
    pub fn new<'a>(
        container_runtime: Arc<ContainerRuntime>,
        pull_test_deps: bool,
        list_only: bool,
    ) -> Self {
        Self {
            container_runtime,
            pull_test_deps,
            list_only,
        }
    }
}

impl Command for PullImagesCommand {
    fn needs_workspace(&self) -> bool {
        false
    }

    fn run(&mut self) -> Result<(), CLIError> {
        let mut images = vec![
            docker_images::SPARK,
            docker_images::FLINK,
            docker_images::JUPYTER,
        ];

        if self.pull_test_deps {
            images.extend(vec![
                docker_images::HTTPD,
                docker_images::FTP,
                docker_images::MINIO,
            ])
        }

        if self.list_only {
            for img in images {
                println!("{}", img);
            }
        } else {
            for img in images {
                eprintln!("{}: {}", console::style("Pulling image").bold(), img);
                self.container_runtime
                    .pull_cmd(img)
                    .status()?
                    .exit_ok()
                    .map_err(|e| CLIError::failure(e))?;
            }
        }

        Ok(())
    }
}
