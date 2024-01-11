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
use kamu::utils::docker_images::BUSYBOX;

use super::{CLIError, Command};
use crate::VerificationMultiProgress;

const TEST_CONTAINER_NAME: &str = "test-volume-mount";

pub struct SystemDiagnoseCommand {
    dataset_repo: Arc<dyn DatasetRepository>,
    verification_svc: Arc<dyn VerificationService>,
    container_runtime: Arc<ContainerRuntime>,
    is_in_workpace: bool,
}

impl SystemDiagnoseCommand {
    pub fn new(
        dataset_repo: Arc<dyn DatasetRepository>,
        verification_svc: Arc<dyn VerificationService>,
        container_runtime: Arc<ContainerRuntime>,
        is_in_workpace: bool,
    ) -> Self {
        Self {
            dataset_repo,
            verification_svc,
            container_runtime,
            is_in_workpace,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for SystemDiagnoseCommand {
    fn needs_workspace(&self) -> bool {
        false
    }

    async fn run(&mut self) -> Result<(), CLIError> {
        let diagnose_check = RunCheck::new(
            self.dataset_repo.clone(),
            self.verification_svc.clone(),
            self.container_runtime.clone(),
            self.is_in_workpace,
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
    is_in_workpace: bool,
}

impl RunCheck {
    fn new(
        dataset_repo: Arc<dyn DatasetRepository>,
        verification_svc: Arc<dyn VerificationService>,
        container_runtime: Arc<ContainerRuntime>,
        is_in_workpace: bool,
    ) -> Self {
        Self {
            dataset_repo,
            verification_svc,
            container_runtime,
            is_in_workpace,
        }
    }

    async fn container_check(&self, f: &mut dyn std::io::Write) -> Result<(), CLIError> {
        write!(f, "{} installed... ", self.container_runtime.config.runtime)?;

        match self.container_runtime.info().output().await {
            Ok(res) => {
                match res.status.success() {
                    true => write!(f, "{}", style("ok").green())?,
                    false => {
                        write!(f, "{}", style("failed").red())?;
                        write!(
                            f,
                            "\n{}",
                            style(String::from_utf8(res.stderr).unwrap()).red()
                        )?;
                    }
                };
            }
            Err(err) => {
                write!(f, "{}", style("failed").red())?;
                write!(f, "\n{}", style(err).red())?;
            }
        };

        Ok(())
    }

    async fn container_pull_check(&self, f: &mut dyn std::io::Write) -> Result<(), CLIError> {
        write!(
            f,
            "{} can pull images... ",
            self.container_runtime.config.runtime
        )?;

        let is_container_image_pulled = match self.container_runtime.pull_image(BUSYBOX, None).await
        {
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
        write!(
            f,
            "{} rootless run check... ",
            self.container_runtime.config.runtime
        )?;

        match self
            .container_runtime
            .run_attached(BUSYBOX)
            .init(true)
            .spawn()
            .unwrap()
            .wait()
            .await
        {
            Ok(status) => {
                match status.success() {
                    true => write!(f, "{}", style("ok").green())?,
                    false => write!(f, "{}", style("failed").red())?,
                };
            }
            Err(err) => {
                write!(f, "{}", style("failed").red())?;
                write!(f, "\n{}", style(err).red())?;
            }
        };

        Ok(())
    }

    async fn container_volume_mount_check(
        &self,
        f: &mut dyn std::io::Write,
    ) -> Result<(), CLIError> {
        write!(
            f,
            "{} volume mounts work... ",
            self.container_runtime.config.runtime
        )?;
        let dir_to_mount = std::env::current_dir()?.join("tmp");
        std::fs::create_dir(dir_to_mount.clone())?;

        let file_path = dir_to_mount.join("tmp.txt");
        let _ = File::create(file_path.clone())?;

        match self
            .container_runtime
            .run_attached(BUSYBOX)
            .volume((dir_to_mount.clone(), "/out"))
            .container_name(TEST_CONTAINER_NAME)
            .args(["cat", "/out/tmp.txt"])
            .init(true)
            .spawn()
            .unwrap()
            .wait()
            .await
        {
            Ok(status) => {
                match status.success() {
                    true => write!(f, "{}", style("ok").green())?,
                    false => write!(f, "{}", style("failed").red())?,
                };
            }
            Err(err) => {
                println!("{:?}", err);
                write!(f, "{}", style("failed").red())?;
                write!(f, "\n{}", style(err).red())?;
            }
        };

        fs::remove_dir_all(dir_to_mount)?;
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
            check_logical_hashes: false,
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
        if self.is_in_workpace {
            let progress = VerificationMultiProgress::new();
            let listener = Arc::new(progress.clone());

            let draw_thread = std::thread::spawn(move || {
                progress.draw();
            });
            self.workspace_consistency_check(Some(listener.clone()), &mut out)
                .await?;
            listener.finish();
            draw_thread.join().unwrap();
        } else {
            write!(
                out,
                "{}",
                style("directory is not a kamu workspace").yellow(),
            )?;
        }

        write!(out, "\n")?;
        Ok(())
    }
}
