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
use std::path::{Path, PathBuf};
use std::process::Output;
use std::sync::Arc;

use console::style;
use container_runtime::{ContainerRuntime, RunArgs};
use file_utils::OwnedFile;
use futures::TryStreamExt;
use internal_error::{InternalError, ResultIntoInternal};
use kamu::domain::engine::normalize_logs;
use kamu::domain::{
    DatasetRegistry,
    VerificationOptions,
    VerificationRequest,
    VerifyDatasetUseCase,
};
use kamu::utils::docker_images::BUSYBOX;
use random_names::get_random_name;
use thiserror::Error;

use super::{CLIError, Command};
use crate::{VerificationMultiProgress, WorkspaceService};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const SUCCESS_MESSAGE: &str = "ok";
const FAILED_MESSAGE: &str = "failed";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SystemDiagnoseCommand {
    dataset_registry: Arc<dyn DatasetRegistry>,
    verify_dataset_use_case: Arc<dyn VerifyDatasetUseCase>,
    container_runtime: Arc<ContainerRuntime>,
    workspace_svc: Arc<WorkspaceService>,
}

impl SystemDiagnoseCommand {
    pub fn new(
        dataset_registry: Arc<dyn DatasetRegistry>,
        verify_dataset_use_case: Arc<dyn VerifyDatasetUseCase>,
        container_runtime: Arc<ContainerRuntime>,
        workspace_svc: Arc<WorkspaceService>,
    ) -> Self {
        Self {
            dataset_registry,
            verify_dataset_use_case,
            container_runtime,
            workspace_svc,
        }
    }

    fn run_info_dir(&self) -> Option<PathBuf> {
        if !self.workspace_svc.is_in_workspace() {
            return None;
        }
        Some(self.workspace_svc.layout().unwrap().run_info_dir.clone())
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
                run_info_dir: self.run_info_dir(),
            }),
            Box::new(CheckContainerRuntimeImagePull {
                container_runtime: self.container_runtime.clone(),
            }),
            Box::new(CheckContainerRuntimeRootlessRun {
                container_runtime: self.container_runtime.clone(),
                run_info_dir: self.run_info_dir(),
            }),
            Box::new(CheckContainerRuntimeVolumeMount {
                container_runtime: self.container_runtime.clone(),
                run_info_dir: self.run_info_dir(),
            }),
        ];
        // Add checks which required workspace initialization
        if self.workspace_svc.is_in_workspace() {
            diagnostic_checks.push(Box::new(CheckWorkspaceConsistent {
                dataset_registry: self.dataset_registry.clone(),
                verify_dataset_use_case: self.verify_dataset_use_case.clone(),
            }));
        }
        let mut is_err = false;

        for diagnostic_check in &diagnostic_checks {
            write!(out, "{}... ", diagnostic_check.name())?;
            match diagnostic_check.run().await {
                Ok(_) => writeln!(out, "{}", style(SUCCESS_MESSAGE).green())?,
                Err(err) => {
                    is_err = true;
                    writeln!(out, "{}", style(FAILED_MESSAGE).red())?;
                    writeln!(out, "{}", style(err.to_string()).red())?;
                }
            }
        }

        if !self.workspace_svc.is_in_workspace() {
            writeln!(out, "{}", style("Directory is not kamu workspace").yellow())?;
            writeln!(
                out,
                "{}",
                style("Run `kamu init` command to initialize a new workspace").yellow()
            )?;
        }
        if !is_err {
            writeln!(out, "{}", style("Kamu environment is healthy").green())?;
        }
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
struct CommandExecError {
    pub message: String,
    pub error_log: String,
    pub log_files: Vec<PathBuf>,
}

impl CommandExecError {
    pub fn new(log_files: Vec<PathBuf>, message: String, error_log: String) -> Self {
        Self {
            log_files: normalize_logs(log_files),
            message,
            error_log,
        }
    }
}

impl From<std::io::Error> for DiagnosticCheckError {
    fn from(e: std::io::Error) -> Self {
        Self::Failed(CommandExecError::new(
            vec![],
            "Unable to perform io operation".to_string(),
            e.to_string(),
        ))
    }
}

impl std::fmt::Display for CommandExecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} \n {}", self.message, self.error_log)?;

        if !self.log_files.is_empty() {
            writeln!(f, ", see log files for details:")?;
            for path in &self.log_files {
                writeln!(f, "- {}", path.display())?;
            }
        }
        writeln!(f, "Make sure to follow kamu installation instructions to correctly configure \
            docker or podman:
            https://docs.kamu.dev/cli/get-started/installation/")?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
trait DiagnosticCheck {
    fn name(&self) -> String;

    async fn run(&self) -> Result<(), DiagnosticCheckError>;

    fn stderr_file_path(&self, run_info_dir: &Path) -> PathBuf;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct CheckContainerRuntimeIsInstalled {
    container_runtime: Arc<ContainerRuntime>,
    run_info_dir: Option<PathBuf>,
}

#[async_trait::async_trait]
impl DiagnosticCheck for CheckContainerRuntimeIsInstalled {
    fn name(&self) -> String {
        format!("{} installed", self.container_runtime.config.runtime)
    }

    fn stderr_file_path(&self, run_info_dir: &Path) -> PathBuf {
        run_info_dir.join("kamu.diagnose-stderr-installed.log")
    }

    async fn run(&self) -> Result<(), DiagnosticCheckError> {
        let mut command = self.container_runtime.info();
        let mut err_output = vec![];
        if let Some(run_info_dir) = self.run_info_dir.as_ref() {
            command.stderr(File::create(self.stderr_file_path(run_info_dir))?);
            err_output.push(self.stderr_file_path(run_info_dir));
        }
        let command_res = command.output().await.int_err()?;
        handle_output_result(command_res, err_output)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct CheckContainerRuntimeImagePull {
    container_runtime: Arc<ContainerRuntime>,
}

#[async_trait::async_trait]
impl DiagnosticCheck for CheckContainerRuntimeImagePull {
    fn name(&self) -> String {
        format!("{} can pull images", self.container_runtime.config.runtime)
    }

    fn stderr_file_path(&self, _: &Path) -> PathBuf {
        unimplemented!()
    }

    async fn run(&self) -> Result<(), DiagnosticCheckError> {
        self.container_runtime
            .pull_image(BUSYBOX, None)
            .await
            .int_err()?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct CheckContainerRuntimeRootlessRun {
    container_runtime: Arc<ContainerRuntime>,
    run_info_dir: Option<PathBuf>,
}

#[async_trait::async_trait]
impl DiagnosticCheck for CheckContainerRuntimeRootlessRun {
    fn name(&self) -> String {
        format!(
            "{} rootless run check",
            self.container_runtime.config.runtime
        )
    }

    fn stderr_file_path(&self, run_info_dir: &Path) -> PathBuf {
        run_info_dir.join("kamu.diagnose-stderr-rootless.log")
    }

    async fn run(&self) -> Result<(), DiagnosticCheckError> {
        let run_args = RunArgs {
            image: BUSYBOX.to_string(),
            container_name: Some(get_random_name(Some("kamu-check-rootless-run-"), 10)),
            ..RunArgs::default()
        };

        let mut command = self.container_runtime.run_cmd(run_args);
        let mut err_output = vec![];
        if let Some(run_info_dir) = self.run_info_dir.as_ref() {
            command.stderr(File::create(self.stderr_file_path(run_info_dir))?);
            err_output.push(self.stderr_file_path(run_info_dir));
        }
        let command_res = command.output().await.int_err()?;
        handle_output_result(command_res, err_output)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct CheckContainerRuntimeVolumeMount {
    container_runtime: Arc<ContainerRuntime>,
    run_info_dir: Option<PathBuf>,
}

#[async_trait::async_trait]
impl DiagnosticCheck for CheckContainerRuntimeVolumeMount {
    fn name(&self) -> String {
        format!(
            "{} volume mounts work",
            self.container_runtime.config.runtime
        )
    }

    fn stderr_file_path(&self, run_info_dir: &Path) -> PathBuf {
        run_info_dir.join("kamu.diagnose-stderr-volume.log")
    }

    async fn run(&self) -> Result<(), DiagnosticCheckError> {
        let dir_to_mount = std::env::current_dir()?;
        let file_path = dir_to_mount.join("tmp.txt");
        let _ = File::create(file_path.clone())?;
        let _ = OwnedFile::new(file_path);
        let run_args = RunArgs {
            image: BUSYBOX.to_string(),
            container_name: Some(get_random_name(Some("kamu-check-volume-mount-"), 10)),
            ..RunArgs::default()
        };

        let mut command = self.container_runtime.run_cmd(run_args);
        let mut err_output = vec![];
        if let Some(run_info_dir) = self.run_info_dir.as_ref() {
            command.stderr(File::create(self.stderr_file_path(run_info_dir))?);
            err_output.push(self.stderr_file_path(run_info_dir));
        }
        let command_res = command.output().await.int_err()?;
        handle_output_result(command_res, err_output)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct CheckWorkspaceConsistent {
    dataset_registry: Arc<dyn DatasetRegistry>,
    verify_dataset_use_case: Arc<dyn VerifyDatasetUseCase>,
}

#[async_trait::async_trait]
impl DiagnosticCheck for CheckWorkspaceConsistent {
    fn name(&self) -> String {
        "workspace consistent".to_string()
    }

    fn stderr_file_path(&self, _: &Path) -> PathBuf {
        unimplemented!()
    }

    async fn run(&self) -> Result<(), DiagnosticCheckError> {
        let progress = Arc::new(VerificationMultiProgress::new());

        let progress_cloned = progress.clone();
        let draw_thread = std::thread::spawn(move || {
            progress_cloned.draw();
        });

        let dataset_handles: Vec<_> = self
            .dataset_registry
            .all_dataset_handles()
            .try_collect()
            .await?;

        let mut verification_tasks = Vec::new();
        for dataset_handle in dataset_handles {
            verification_tasks.push(VerificationRequest {
                target: dataset_handle,
                block_range: (None, None),
                options: VerificationOptions {
                    check_integrity: true,
                    check_logical_hashes: false,
                    replay_transformations: false,
                },
            });
        }

        let results = self
            .verify_dataset_use_case
            .execute_multi(verification_tasks, Some(progress.clone()))
            .await;

        for result in results {
            // TODO: This will currently not show which dataset validation failed for
            // We need to improve `verify_multi` signature.
            result.outcome.int_err()?;
        }

        progress.finish();
        draw_thread.join().unwrap();

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn handle_output_result(
    result: Output,
    stderr_file_path: Vec<PathBuf>,
) -> Result<(), DiagnosticCheckError> {
    if result.status.success() {
        Ok(())
    } else {
        let err_msg = String::from_utf8(result.stderr).map_err(|e| {
            DiagnosticCheckError::Failed(CommandExecError::new(
                stderr_file_path.clone(),
                "Cannot parse command execution error".to_string(),
                e.to_string(),
            ))
        })?;

        Err(DiagnosticCheckError::Failed(CommandExecError::new(
            stderr_file_path,
            "Container runtime command unavailable".to_string(),
            err_msg,
        )))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
