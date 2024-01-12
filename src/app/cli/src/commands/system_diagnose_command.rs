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
use internal_error::{InternalError, ResultIntoInternal};
use kamu::domain::{
    DatasetRepository,
    VerificationMultiListener,
    VerificationOptions,
    VerificationService,
};
use kamu::utils::docker_images::BUSYBOX;
use thiserror::Error;

use super::{CLIError, Command};
use crate::VerificationMultiProgress;

const TEST_CONTAINER_NAME: &str = "test-volume-mount";
const SUCCESS_MESSAGE: &str = "ok";
const FAILED_MESSAGE: &str = "failed";

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

#[derive(thiserror::Error, Debug)]
pub enum DiagStepError {
    #[error(transparent)]
    InvalidOutputMessage(#[from] InvalidOutputMessageError),
    #[error(transparent)]
    CommandExecFailed(#[from] CommandExecError),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
    #[error(transparent)]
    IoError(
        #[from]
        #[backtrace]
        InputOutputError,
    ),
}

#[derive(Error, Debug)]
#[error("Cannot parse output message {message}")]
pub struct InvalidOutputMessageError {
    pub message: String,
}

#[derive(Error, Debug)]
#[error("Cannot execute command: {message}")]
pub struct CommandExecError {
    pub message: String,
}

#[derive(Error, Debug)]
#[error("Cannot perform io operation: {message}")]
pub struct InputOutputError {
    pub message: String,
}

impl From<std::io::Error> for DiagStepError {
    fn from(e: std::io::Error) -> Self {
        Self::IoError(InputOutputError {
            message: e.to_string(),
        })
    }
}

///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
trait DiagCheck {
    fn name(&self) -> String;
    async fn run(&self) -> Result<(), DiagStepError>;
}

///////////////////////////////////////////////////////////////////////////////

struct CheckContainerInstallation {
    container_runtime: Arc<ContainerRuntime>,
}

#[async_trait::async_trait]
impl DiagCheck for CheckContainerInstallation {
    fn name(&self) -> String {
        format!("{} installed", self.container_runtime.config.runtime)
    }
    async fn run(&self) -> Result<(), DiagStepError> {
        let command_res = self.container_runtime.info().output().await.int_err()?;
        match command_res.status.success() {
            true => Ok(()),
            false => {
                let err_msg = String::from_utf8(command_res.stderr).map_err(|e| {
                    DiagStepError::InvalidOutputMessage(InvalidOutputMessageError {
                        message: e.to_string(),
                    })
                })?;

                Err(DiagStepError::CommandExecFailed(CommandExecError {
                    message: err_msg,
                }))
            }
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

struct CheckContainerImagePull {
    container_runtime: Arc<ContainerRuntime>,
}

#[async_trait::async_trait]
impl DiagCheck for CheckContainerImagePull {
    fn name(&self) -> String {
        format!("{} can pull images", self.container_runtime.config.runtime)
    }
    async fn run(&self) -> Result<(), DiagStepError> {
        self.container_runtime
            .pull_image(BUSYBOX, None)
            .await
            .int_err()?;

        Ok(())
    }
}

///////////////////////////////////////////////////////////////////////////////

struct CheckContainerRootlessRun {
    container_runtime: Arc<ContainerRuntime>,
}

#[async_trait::async_trait]
impl DiagCheck for CheckContainerRootlessRun {
    fn name(&self) -> String {
        format!(
            "{} rootless run check",
            self.container_runtime.config.runtime
        )
    }
    async fn run(&self) -> Result<(), DiagStepError> {
        let command_status = self
            .container_runtime
            .run_attached(BUSYBOX)
            .init(true)
            .spawn()
            .unwrap()
            .wait()
            .await
            .int_err()?;

        if !command_status.success() {
            return Err(DiagStepError::CommandExecFailed(CommandExecError {
                message: "Failed to run docker container".to_string(),
            }));
        }

        Ok(())
    }
}

///////////////////////////////////////////////////////////////////////////////

struct CheckContainerVolumeMount {
    container_runtime: Arc<ContainerRuntime>,
}

#[async_trait::async_trait]
impl DiagCheck for CheckContainerVolumeMount {
    fn name(&self) -> String {
        format!(
            "{} volume mounts work",
            self.container_runtime.config.runtime
        )
    }
    async fn run(&self) -> Result<(), DiagStepError> {
        let dir_to_mount = std::env::current_dir()?.join("tmp");
        std::fs::create_dir(dir_to_mount.clone())?;
        let file_path = dir_to_mount.join("tmp.txt");
        let _ = File::create(file_path.clone())?;

        let command_status = self
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
            .int_err()?;
        fs::remove_dir_all(dir_to_mount)?;

        if !command_status.success() {
            return Err(DiagStepError::CommandExecFailed(CommandExecError {
                message: "Failed to mount volume in container".to_string(),
            }));
        }

        Ok(())
    }
}

///////////////////////////////////////////////////////////////////////////////

struct CheckWorkspaceConsistent {
    dataset_repo: Arc<dyn DatasetRepository>,
    verification_svc: Arc<dyn VerificationService>,
}

#[async_trait::async_trait]
impl DiagCheck for CheckWorkspaceConsistent {
    fn name(&self) -> String {
        "workspace consistent".to_string()
    }
    async fn run(&self) -> Result<(), DiagStepError> {
        let progress = VerificationMultiProgress::new();
        let listener_option = Arc::new(progress.clone());

        let draw_thread = std::thread::spawn(move || {
            progress.draw();
        });
        let datasets: Vec<_> = self.dataset_repo.get_all_datasets().try_collect().await?;

        let verify_options = VerificationOptions {
            check_integrity: true,
            check_logical_hashes: false,
            replay_transformations: false,
        };

        for dataset in datasets {
            let listener = Some(listener_option.clone()).and_then(|l| l.begin_verify(&dataset));
            self.verification_svc
                .verify(
                    &dataset.as_local_ref(),
                    (None, None),
                    verify_options.clone(),
                    listener,
                )
                .await
                .int_err()?;
        }

        listener_option.finish();
        draw_thread.join().unwrap();

        Ok(())
    }
}

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

    pub async fn run(&self) -> Result<(), CLIError> {
        let mut out = std::io::stdout();

        let mut diagnose_checks: Vec<Box<dyn DiagCheck>> = vec![
            Box::new(CheckContainerInstallation {
                container_runtime: self.container_runtime.clone(),
            }),
            Box::new(CheckContainerImagePull {
                container_runtime: self.container_runtime.clone(),
            }),
            Box::new(CheckContainerRootlessRun {
                container_runtime: self.container_runtime.clone(),
            }),
            Box::new(CheckContainerVolumeMount {
                container_runtime: self.container_runtime.clone(),
            }),
        ];
        // Add checks which required workspace initialization
        if self.is_in_workpace {
            diagnose_checks.push(Box::new(CheckWorkspaceConsistent {
                dataset_repo: self.dataset_repo.clone(),
                verification_svc: self.verification_svc.clone(),
            }));
        }

        for diag_check in diagnose_checks.iter() {
            write!(out, "{}... ", diag_check.name())?;
            match diag_check.run().await {
                Ok(_) => write!(out, "{}\n", style(SUCCESS_MESSAGE).green())?,
                Err(err) => {
                    write!(out, "{}\n", style(FAILED_MESSAGE).red())?;
                    write!(out, "{}\n", style(err.to_string()).red())?;
                }
            }
        }

        if !self.is_in_workpace {
            write!(
                out,
                "{}\n",
                style("Directory is not kamu workspace").yellow()
            )?;
        }
        Ok(())
    }
}
