// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::*;

use super::{CLIError, Command};
use crate::output::*;

///////////////////////////////////////////////////////////////////////////////

pub struct SystemInfoCommand {
    output_config: Arc<OutputConfig>,
    output_format: Option<String>,
}

impl SystemInfoCommand {
    pub fn new<S>(output_config: Arc<OutputConfig>, output_format: Option<S>) -> Self
    where
        S: Into<String>,
    {
        Self {
            output_config,
            output_format: output_format.map(|s| s.into()),
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for SystemInfoCommand {
    fn needs_workspace(&self) -> bool {
        false
    }

    async fn run(&mut self) -> Result<(), CLIError> {
        write_output(
            SystemInfo::collect(),
            &self.output_config,
            self.output_format.as_ref(),
        )?;
        Ok(())
    }
}

///////////////////////////////////////////////////////////////////////////////

pub struct VersionCommand {
    output_config: Arc<OutputConfig>,
    output_format: Option<String>,
}

impl VersionCommand {
    pub fn new<S>(output_config: Arc<OutputConfig>, output_format: Option<S>) -> Self
    where
        S: Into<String>,
    {
        Self {
            output_config,
            output_format: output_format.map(|s| s.into()),
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for VersionCommand {
    fn needs_workspace(&self) -> bool {
        false
    }

    async fn run(&mut self) -> Result<(), CLIError> {
        write_output(
            BuildInfo::collect(),
            &self.output_config,
            self.output_format.as_ref(),
        )?;
        Ok(())
    }
}

///////////////////////////////////////////////////////////////////////////////

fn write_output<T: serde::Serialize>(
    value: T,
    output_config: &OutputConfig,
    output_format: Option<impl AsRef<str>>,
) -> Result<(), InternalError> {
    let output_format = if let Some(fmt) = &output_format {
        fmt.as_ref()
    } else {
        if output_config.is_tty {
            "shell"
        } else {
            "json"
        }
    };

    // TODO: Generalize this code in output config, just like we do for tabular
    // output
    match output_format {
        "json" => serde_json::to_writer_pretty(std::io::stdout(), &value).int_err()?,
        "shell" | "yaml" | _ => serde_yaml::to_writer(std::io::stdout(), &value).int_err()?,
    }

    Ok(())
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SystemInfo {
    pub build: BuildInfo,
}

impl SystemInfo {
    pub fn collect() -> Self {
        Self {
            build: BuildInfo::collect(),
        }
    }
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BuildInfo {
    pub app_version: &'static str,
    pub build_timestamp: Option<&'static str>,
    pub git_describe: Option<&'static str>,
    pub git_sha: Option<&'static str>,
    pub git_commit_date: Option<&'static str>,
    pub git_branch: Option<&'static str>,
    pub rustc_semver: Option<&'static str>,
    pub rustc_channel: Option<&'static str>,
    pub rustc_host_triple: Option<&'static str>,
    pub rustc_commit_sha: Option<&'static str>,
    pub cargo_target_triple: Option<&'static str>,
    pub cargo_features: Option<&'static str>,
    pub cargo_opt_level: Option<&'static str>,
}

impl BuildInfo {
    pub fn collect() -> Self {
        Self {
            app_version: env!("CARGO_PKG_VERSION"),
            build_timestamp: option_env!("VERGEN_BUILD_TIMESTAMP"),
            git_describe: option_env!("VERGEN_GIT_DESCRIBE"),
            git_sha: option_env!("VERGEN_GIT_SHA"),
            git_commit_date: option_env!("VERGEN_GIT_COMMIT_DATE"),
            git_branch: option_env!("VERGEN_GIT_BRANCH"),
            rustc_semver: option_env!("VERGEN_RUSTC_SEMVER"),
            rustc_channel: option_env!("VERGEN_RUSTC_CHANNEL"),
            rustc_host_triple: option_env!("VERGEN_RUSTC_HOST_TRIPLE"),
            rustc_commit_sha: option_env!("VERGEN_RUSTC_COMMIT_HASH"),
            cargo_target_triple: option_env!("VERGEN_CARGO_TARGET_TRIPLE"),
            cargo_features: option_env!("VERGEN_CARGO_FEATURES"),
            cargo_opt_level: option_env!("VERGEN_CARGO_OPT_LEVEL"),
        }
    }
}
