// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use console::style;
use container_runtime::ContainerRuntime;
use futures::TryStreamExt;
use kamu::domain::{
    DatasetRepository,
    VerificationMultiListener,
    VerificationOptions,
    VerificationService,
};

use super::{CLIError, Command};
use crate::VerificationMultiProgress;

pub const DUMMY_IMAGE: &str = "docker.io/busybox:latest";

pub struct SystemDiagnoseCommand {
    dataset_repo: Arc<dyn DatasetRepository>,
    verification_svc: Arc<dyn VerificationService>,
    container_runtime: Arc<ContainerRuntime>,
}

impl SystemDiagnoseCommand {
    pub fn new(
        dataset_repo: Arc<dyn DatasetRepository>,
        verification_svc: Arc<dyn VerificationService>,
        container_runtime: Arc<ContainerRuntime>,
    ) -> Self {
        Self {
            dataset_repo,
            verification_svc,
            container_runtime,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for SystemDiagnoseCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        let diagnose_check = RunCheck::new(
            self.dataset_repo.clone(),
            self.verification_svc.clone(),
            self.container_runtime.clone(),
        );
        diagnose_check.run().await?;
        Ok(())
    }
}

///////////////////////////////////////////////////////////////////////////////

pub struct RunCheck {
    dataset_repo: Arc<dyn DatasetRepository>,
    verification_svc: Arc<dyn VerificationService>,
    container_runtime: Arc<ContainerRuntime>,
}

impl RunCheck {
    fn new(
        dataset_repo: Arc<dyn DatasetRepository>,
        verification_svc: Arc<dyn VerificationService>,
        container_runtime: Arc<ContainerRuntime>,
    ) -> Self {
        Self {
            dataset_repo,
            verification_svc,
            container_runtime,
        }
    }

    async fn container_check(&self, f: &mut dyn std::io::Write) -> Result<(), CLIError> {
        write!(f, "container installed... ")?;

        let container_installed_check_status = match self
            .container_runtime
            .custom_cmd("--version".to_string())
            .output()
            .await
        {
            Ok(_) => style("ok").green(),
            Err(_) => style("failed").red(),
        };

        write!(f, "{container_installed_check_status}")?;
        Ok(())
    }

    async fn container_pull_check(&self, f: &mut dyn std::io::Write) -> Result<(), CLIError> {
        write!(f, "container can pull images... ")?;

        let is_container_image_pulled =
            match self.container_runtime.pull_image(DUMMY_IMAGE, None).await {
                Ok(_) => style("ok").green(),
                Err(_) => style("failed").red(),
            };

        write!(f, "{is_container_image_pulled}")?;
        Ok(())
    }

    async fn container_rootless_run_check(
        &self,
        f: &mut dyn std::io::Write,
    ) -> Result<(), CLIError> {
        write!(f, "container rootless run check... ")?;

        let container_rootless_check_status = match self
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

        write!(f, "{container_rootless_check_status}")?;
        Ok(())
    }

    async fn container_volume_mount_check(
        &self,
        f: &mut dyn std::io::Write,
    ) -> Result<(), CLIError> {
        write!(f, "container volume mounts work... ")?;
        let temp_dir = tempfile::tempdir()?;
        let cwd = Path::new(".").canonicalize()?;

        let file_path = temp_dir.path().join("tmp.txt");
        let _ = File::create(file_path.clone())?;

        let container_volume_check_status = match self
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

        fs::remove_file(file_path)?;

        write!(f, "{container_volume_check_status}")?;
        Ok(())
    }

    async fn workspace_consistency_check(
        &self,
        listener: Option<Arc<VerificationMultiProgress>>,
        f: &mut dyn std::io::Write,
    ) -> Result<(), CLIError> {
        write!(f, "workspace consistent... ")?;
        let datasets: Vec<_> = self.dataset_repo.get_all_datasets().try_collect().await?;

        let verify_options = VerificationOptions {
            check_integrity: true,
            replay_transformations: false,
        };

        for dataset in datasets {
            let listener = listener.clone().and_then(|l| l.begin_verify(&dataset));
            if let Err(_) = self
                .verification_svc
                .verify(
                    &dataset.as_local_ref(),
                    (None, None),
                    verify_options.clone(),
                    listener,
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

    pub async fn run(&self) -> Result<(), CLIError> {
        let mut out = std::io::stdout();

        self.container_check(&mut out).await?;
        write!(out, "\n")?;
        self.container_pull_check(&mut out).await?;
        write!(out, "\n")?;
        self.container_rootless_run_check(&mut out).await?;
        write!(out, "\n")?;
        self.container_volume_mount_check(&mut out).await?;
        write!(out, "\n")?;
        let progress = VerificationMultiProgress::new();
        let listener = Arc::new(progress.clone());

        let draw_thread = std::thread::spawn(move || {
            progress.draw();
        });
        self.workspace_consistency_check(Some(listener.clone()), &mut out)
            .await?;
        write!(out, "\n")?;
        listener.finish();
        draw_thread.join().unwrap();

        Ok(())
    }
}
