// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::backtrace::Backtrace;
use std::process::Output;
use std::time::Duration;

use thiserror::Error;
use tokio::process::Command;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum ContainerRuntimeError {
    #[error(transparent)]
    Timeout(#[from] TimeoutError),
    #[error(transparent)]
    Process(#[from] ProcessError),
    #[error(transparent)]
    IO(#[from] std::io::Error),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ImagePullError {
    // TODO: Differentiate "not found",
    #[error(transparent)]
    Runtime(#[from] ImagePullErrorRuntime),
}

impl ImagePullError {
    pub fn runtime(image_name: impl Into<String>, source: ContainerRuntimeError) -> Self {
        ImagePullErrorRuntime {
            image_name: image_name.into(),
            source,
            backtrace: Backtrace::capture(),
        }
        .into()
    }
}

#[derive(Debug, Error)]
#[error("Failed pulling image: {image_name}")]
pub struct ImagePullErrorRuntime {
    pub image_name: String,
    pub source: ContainerRuntimeError,
    pub backtrace: Backtrace,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub struct ProcessError {
    command: Command,
    code: i32,
    stdout: Option<String>,
    stderr: Option<String>,
}

impl std::fmt::Display for ProcessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Process exited with code {}", self.code)?;
        writeln!(f, "  Command: {:?}", self.command)?;
        if let Some(stdout) = &self.stdout {
            writeln!(f, "  StdOut: {}", stdout)?;
        }
        if let Some(stderr) = &self.stderr {
            writeln!(f, "  StdErr: {}", stderr)?;
        }
        Ok(())
    }
}

impl ProcessError {
    pub fn from_output(command: Command, output: Output) -> Self {
        Self {
            command,
            code: output.status.code().unwrap(),
            stdout: Some(String::from_utf8(output.stdout).expect("Non-unicode data")),
            stderr: Some(String::from_utf8(output.stderr).expect("Non-unicode data")),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
#[error("Timed out after {duration:?}")]
pub struct TimeoutError {
    duration: Duration,
    backtrace: Backtrace,
}

impl TimeoutError {
    pub fn new(d: Duration) -> Self {
        Self {
            duration: d,
            backtrace: Backtrace::capture(),
        }
    }
}
