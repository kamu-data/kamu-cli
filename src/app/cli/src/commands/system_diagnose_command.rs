// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fs::File;
use std::io::Write;
use std::process::Output;
use std::sync::Arc;

use console::style;
use container_runtime::{ContainerRuntime, RunArgs};
use futures::TryStreamExt;
use internal_error::{InternalError, ResultIntoInternal};
use kamu::domain::{
    DatasetRepository,
    OwnedFile,
    VerificationMultiListener,
    VerificationOptions,
    VerificationService,
};
use kamu::utils::docker_images::BUSYBOX;
use thiserror::Error;

use super::{CLIError, Command};
use crate::VerificationMultiProgress;

///////////////////////////////////////////////////////////////////////////////

const SUCCESS_MESSAGE: &str = "ok";
const FAILED_MESSAGE: &str = "failed";

///////////////////////////////////////////////////////////////////////////////

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
        let mut out = std::io::stdout();

        let mut diagnostic_checks: Vec<Box<dyn DiagnosticCheck>> = vec![
            Box::new(CheckContainerRuntimeIsInstalled {
                container_runtime: self.container_runtime.clone(),
            }),
            Box::new(CheckContainerRuntimeImagePull {
                container_runtime: self.container_runtime.clone(),
            }),
            Box::new(CheckContainerRuntimeRootlessRun {
                container_runtime: self.container_runtime.clone(),
            }),
            Box::new(CheckContainerRuntimeVolumeMount {
                container_runtime: self.container_runtime.clone(),
            }),
        ];
        // Add checks which required workspace initialization
        if self.is_in_workpace {
            diagnostic_checks.push(Box::new(CheckWorkspaceConsistent {
                dataset_repo: self.dataset_repo.clone(),
                verification_svc: self.verification_svc.clone(),
            }));
        }

        for diagnostic_check in &diagnostic_checks {
            write!(out, "{}... ", diagnostic_check.name())?;
            match diagnostic_check.run().await {
                Ok(_) => writeln!(out, "{}", style(SUCCESS_MESSAGE).green())?,
                Err(err) => {
                    writeln!(out, "{}", style(FAILED_MESSAGE).red())?;
                    writeln!(out, "{}", style(err.to_string()).red())?;
                }
            }
        }

        if !self.is_in_workpace {
            writeln!(out, "{}", style("Directory is not kamu workspace").yellow())?;
        }
        Ok(())
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
enum DiagnosticCheckError {
    #[error(transparent)]
    Failed(#[from] CommandExecError),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

#[derive(Error, Debug)]
#[error(
    "{message} \n {error_log}
    Make sure to follow kamu installation instructions to correctly configure \
     docker or podman:
        https://docs.kamu.dev/cli/get-started/installation/"
)]
pub struct CommandExecError {
    pub message: String,
    pub error_log: String,
}

impl From<std::io::Error> for DiagnosticCheckError {
    fn from(e: std::io::Error) -> Self {
        Self::Failed(CommandExecError {
            message: "Unable to perform io operation".to_string(),
            error_log: e.to_string(),
        })
    }
}

///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
trait DiagnosticCheck {
    fn name(&self) -> String;
    async fn run(&self) -> Result<(), DiagnosticCheckError>;
}

///////////////////////////////////////////////////////////////////////////////
struct CheckContainerRuntimeIsInstalled {
    container_runtime: Arc<ContainerRuntime>,
}

#[async_trait::async_trait]
impl DiagnosticCheck for CheckContainerRuntimeIsInstalled {
    fn name(&self) -> String {
        format!("{} installed", self.container_runtime.config.runtime)
    }
    async fn run(&self) -> Result<(), DiagnosticCheckError> {
        let command_res = self.container_runtime.info().output().await.int_err()?;
        handle_output_result(command_res)
    }
}

///////////////////////////////////////////////////////////////////////////////

struct CheckContainerRuntimeImagePull {
    container_runtime: Arc<ContainerRuntime>,
}

#[async_trait::async_trait]
impl DiagnosticCheck for CheckContainerRuntimeImagePull {
    fn name(&self) -> String {
        format!("{} can pull images", self.container_runtime.config.runtime)
    }
    async fn run(&self) -> Result<(), DiagnosticCheckError> {
        self.container_runtime
            .pull_image(BUSYBOX, None)
            .await
            .int_err()?;

        Ok(())
    }
}

///////////////////////////////////////////////////////////////////////////////

struct CheckContainerRuntimeRootlessRun {
    container_runtime: Arc<ContainerRuntime>,
}

#[async_trait::async_trait]
impl DiagnosticCheck for CheckContainerRuntimeRootlessRun {
    fn name(&self) -> String {
        format!(
            "{} rootless run check",
            self.container_runtime.config.runtime
        )
    }
    async fn run(&self) -> Result<(), DiagnosticCheckError> {
        let run_args = RunArgs {
            image: BUSYBOX.to_string(),
            container_name: Some(generate_container_name("kamu-check-rootless-run-")),
            ..RunArgs::default()
        };

        let command_res = self
            .container_runtime
            .run_cmd(run_args)
            .output()
            .await
            .int_err()?;

        handle_output_result(command_res)
    }
}

///////////////////////////////////////////////////////////////////////////////

struct CheckContainerRuntimeVolumeMount {
    container_runtime: Arc<ContainerRuntime>,
}

#[async_trait::async_trait]
impl DiagnosticCheck for CheckContainerRuntimeVolumeMount {
    fn name(&self) -> String {
        format!(
            "{} volume mounts work",
            self.container_runtime.config.runtime
        )
    }
    async fn run(&self) -> Result<(), DiagnosticCheckError> {
        let dir_to_mount = std::env::current_dir()?;
        let file_path = dir_to_mount.join("tmp.txt");
        let _ = File::create(file_path.clone())?;
        let _ = OwnedFile::new(file_path);
        let run_args = RunArgs {
            image: BUSYBOX.to_string(),
            container_name: Some(generate_container_name("kamu-check-volume-mount-")),
            ..RunArgs::default()
        };

        let command_res = self
            .container_runtime
            .run_cmd(run_args)
            .output()
            .await
            .int_err()?;

        handle_output_result(command_res)
    }
}

///////////////////////////////////////////////////////////////////////////////

struct CheckWorkspaceConsistent {
    dataset_repo: Arc<dyn DatasetRepository>,
    verification_svc: Arc<dyn VerificationService>,
}

#[async_trait::async_trait]
impl DiagnosticCheck for CheckWorkspaceConsistent {
    fn name(&self) -> String {
        "workspace consistent".to_string()
    }
    async fn run(&self) -> Result<(), DiagnosticCheckError> {
        let progress = VerificationMultiProgress::new();
        let listener_option = Arc::new(progress.clone());

        let draw_thread = std::thread::spawn(move || {
            progress.draw();
        });
        let datasets: Vec<_> = self
            .dataset_repo
            .get_all_datasets(None)
            .try_collect()
            .await?;

        let verify_options = Arc::new(VerificationOptions {
            check_integrity: true,
            check_logical_hashes: false,
            replay_transformations: false,
        });

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

///////////////////////////////////////////////////////////////////////////////

fn handle_output_result(result: Output) -> Result<(), DiagnosticCheckError> {
    match result.status.success() {
        true => Ok(()),
        false => {
            let err_msg = String::from_utf8(result.stderr).map_err(|e| {
                DiagnosticCheckError::Failed(CommandExecError {
                    message: "Cannot parse command execution error".to_string(),
                    error_log: e.to_string(),
                })
            })?;

            Err(DiagnosticCheckError::Failed(CommandExecError {
                message: "Container runtime command unavailable".to_string(),
                error_log: err_msg,
            }))
        }
    }
}

fn generate_container_name(container_prefix: &str) -> String {
    use rand::Rng;
    let mut res = container_prefix.to_owned();

    res.extend(
        rand::thread_rng()
            .sample_iter(&rand::distributions::Alphanumeric)
            .take(5)
            .map(char::from),
    );
    res
}

///////////////////////////////////////////////////////////////////////////////
