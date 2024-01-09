// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fs::File;
use std::io::{Stdout, Write};
use std::path::Path;
use std::sync::Arc;

use console::style;
use container_runtime::ContainerRuntime;
use futures::TryStreamExt;
use kamu::domain::{DatasetRepository, VerificationOptions, VerificationService};

use super::{CLIError, Command};

pub const DUMMY_IMAGE: &str = "docker.io/busybox:latest";

pub struct SystemDiagnoseCommand {
    dataset_repo: Arc<dyn DatasetRepository>,
    verification_svc: Arc<dyn VerificationService>,
}

impl SystemDiagnoseCommand {
    pub fn new(
        dataset_repo: Arc<dyn DatasetRepository>,
        verification_svc: Arc<dyn VerificationService>,
    ) -> Self {
        Self {
            dataset_repo,
            verification_svc,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for SystemDiagnoseCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        SystemDiagnose::check(self.dataset_repo.clone(), self.verification_svc.clone()).await?;
        Ok(())
    }
}

///////////////////////////////////////////////////////////////////////////////

pub struct SystemDiagnose {}

impl SystemDiagnose {
    pub async fn check(
        dataset_repo: Arc<dyn DatasetRepository>,
        verification_svc: Arc<dyn VerificationService>,
    ) -> Result<(), CLIError> {
        let diagnose_check = RunCheck::new();
        diagnose_check.run(dataset_repo, verification_svc).await
    }
}

pub struct RunCheck {
    container_runtime: ContainerRuntime,
}

impl RunCheck {
    fn new() -> Self {
        Self {
            container_runtime: ContainerRuntime::default(),
        }
    }

    async fn run_container_check(&self, f: &mut Stdout) -> Result<(), CLIError> {
        write!(f, "container installed... ")?;

        let is_container_installed = match self
            .container_runtime
            .custom_cmd("--version".to_string())
            .output()
            .await
        {
            Ok(_) => style("ok").green(),
            Err(_) => style("failed").red(),
        };

        write!(f, "{is_container_installed}")?;
        Ok(())
    }

    async fn run_container_pull_check(&self, f: &mut Stdout) -> Result<(), CLIError> {
        write!(f, "\ncontainer can pull images... ")?;

        let is_container_image_pulled =
            match self.container_runtime.pull_image(DUMMY_IMAGE, None).await {
                Ok(_) => style("ok").green(),
                Err(_) => style("failed").red(),
            };

        write!(f, "{is_container_image_pulled}")?;
        Ok(())
    }

    async fn run_container_rootless_check(&self, f: &mut Stdout) -> Result<(), CLIError> {
        write!(f, "\ncontainer rootless check... ")?;

        let is_container_rootless = match self
            .container_runtime
            .run_attached(DUMMY_IMAGE)
            .init(true)
            .spawn()
            .unwrap()
            .wait()
            .await
        {
            Ok(_) => style("ok").green(),
            Err(_) => style("failed").red(),
        };

        write!(f, "{is_container_rootless}")?;
        Ok(())
    }

    async fn run_container_volume_mount_check(&self, f: &mut Stdout) -> Result<(), CLIError> {
        write!(f, "\ncontainer volume mounts work... ")?;
        let temp_dir = tempfile::tempdir()?;
        let cwd = Path::new(".").canonicalize()?;

        let file_path = temp_dir.path().join("tmp.txt");
        let _ = File::create(file_path)?;

        let is_container_rootless = match self
            .container_runtime
            .run_attached(DUMMY_IMAGE)
            .volume((cwd, temp_dir.into_path()))
            .init(true)
            .spawn()
            .unwrap()
            .wait()
            .await
        {
            Ok(_) => style("ok").green(),
            Err(_) => style("failed").red(),
        };

        write!(f, "{is_container_rootless}")?;
        Ok(())
    }

    async fn run_workspace_consistency_check(
        &self,
        dataset_repo: Arc<dyn DatasetRepository>,
        verification_svc: Arc<dyn VerificationService>,
        f: &mut Stdout,
    ) -> Result<(), CLIError> {
        write!(f, "\nworkspace consistent... ")?;
        let datasets: Vec<_> = dataset_repo.get_all_datasets().try_collect().await?;

        let verify_options = VerificationOptions {
            check_integrity: true,
            replay_transformations: false,
        };
        for dataset in datasets {
            if let Err(_) = verification_svc
                .verify(
                    &dataset.as_local_ref(),
                    (None, None),
                    verify_options.clone(),
                    None,
                )
                .await
            {
                write!(f, "{}", style("failed").red())?;
                return Ok(());
            };
        }
        write!(f, "{}", style("ok").green())?;

        Ok(())
    }

    pub async fn run(
        &self,
        dataset_repo: Arc<dyn DatasetRepository>,
        verification_svc: Arc<dyn VerificationService>,
    ) -> Result<(), CLIError> {
        let mut out = std::io::stdout();

        self.run_container_check(&mut out).await?;
        self.run_container_pull_check(&mut out).await?;
        self.run_container_rootless_check(&mut out).await?;
        self.run_container_volume_mount_check(&mut out).await?;
        self.run_workspace_consistency_check(dataset_repo, verification_svc, &mut out)
            .await?;
        Ok(())
    }
}
