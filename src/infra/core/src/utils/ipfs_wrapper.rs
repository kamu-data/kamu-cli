// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::{Path, PathBuf};

use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};

#[derive(Default, Clone)]
pub struct IpfsClient {
    ipfs_path: Option<PathBuf>,
    default_allow_offline: bool,
}

impl IpfsClient {
    pub fn new(ipfs_path: Option<impl Into<PathBuf>>, default_allow_offline: bool) -> Self {
        Self {
            ipfs_path: ipfs_path.map(Into::into),
            default_allow_offline,
        }
    }

    fn ipfs_cmd(&self) -> tokio::process::Command {
        let mut cmd = tokio::process::Command::new("ipfs");
        if let Some(ipfs_path) = &self.ipfs_path {
            cmd.env("IPFS_PATH", ipfs_path);
        }
        cmd.kill_on_drop(true);
        cmd
    }

    pub async fn key_list(&self) -> Result<Vec<IpfsKey>, InternalError> {
        let mut keys = Vec::new();

        let mut cmd = self.ipfs_cmd();
        cmd.args(["key", "list", "-l"]);

        tracing::info!(?cmd, "Running process");

        let output = cmd.output().await.int_err()?;

        let stdout = std::str::from_utf8(&output.stdout[..]).int_err()?;
        let stderr = std::str::from_utf8(&output.stderr[..]).int_err()?;

        if !output.status.success() {
            tracing::warn!(%stdout, %stderr, "Process exited with non-zero code");
            output.status.exit_ok().int_err()?;
        }

        for line in stdout.split('\n') {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            if let Some((id, name)) = line.split_once(' ') {
                keys.push(IpfsKey::new(id.trim(), name.trim()));
            } else {
                return Err(format!("Failed to parse IPFS output: {stdout}").int_err());
            }
        }

        Ok(keys)
    }

    pub async fn key_gen(&self, name: impl AsRef<str>) -> Result<String, InternalError> {
        let mut cmd = self.ipfs_cmd();
        cmd.args(["key", "gen"]);
        cmd.arg(name.as_ref());

        tracing::info!(?cmd, "Running process");
        let output = cmd.output().await.int_err()?;
        let stdout = std::str::from_utf8(&output.stdout[..]).int_err()?;
        let stderr = std::str::from_utf8(&output.stderr[..]).int_err()?;

        if !output.status.success() {
            tracing::warn!(%stdout, %stderr, "Process exited with non-zero code");
            output.status.exit_ok().int_err()?;
        }

        Ok(stdout.trim().to_owned())
    }

    // TODO: PERF: Use more efficient DAG format suitable for streaming datasets
    pub async fn add_path(
        &self,
        path: impl AsRef<Path>,
        opts: AddOptions<'_>,
    ) -> Result<String, InternalError> {
        let mut cmd = self.ipfs_cmd();
        cmd.args(["add", "-Q", "--cid-version", "1", "--raw-leaves", "-r"]);
        for ign in opts.ignore.unwrap_or(&[]) {
            cmd.arg("--ignore");
            cmd.arg(ign);
        }
        cmd.arg(path.as_ref());

        tracing::info!(?cmd, "Running process");
        let output = cmd.output().await.int_err()?;
        let stdout = std::str::from_utf8(&output.stdout[..]).int_err()?;
        let stderr = std::str::from_utf8(&output.stderr[..]).int_err()?;

        if !output.status.success() {
            tracing::warn!(%stdout, %stderr, "Process exited with non-zero code");
            output.status.exit_ok().int_err()?;
        }

        Ok(stdout.trim().to_owned())
    }

    pub async fn name_resolve_local(&self, key: &str) -> Result<Option<String>, InternalError> {
        let mut cmd = self.ipfs_cmd();
        cmd.args([
            "name",
            "resolve",
            "--dht-record-count",
            "1",
            "--dht-timeout",
            "1s",
        ]);

        cmd.arg(format!("/ipns/{key}"));

        tracing::info!(?cmd, "Running process");

        let output = cmd.output().await.int_err()?;
        let stdout = std::str::from_utf8(&output.stdout[..]).int_err()?;

        // TODO: Differentiate error codes
        if !output.status.success() {
            Ok(None)
        } else {
            let (scheme, cid) = stdout
                .trim()
                .trim_start_matches('/')
                .split_once('/')
                .unwrap();
            assert_eq!(scheme, "ipfs");
            Ok(Some(cid.to_owned()))
        }
    }

    pub async fn name_publish(
        &self,
        cid: &str,
        options: PublishOptions<'_>,
    ) -> Result<String, InternalError> {
        let mut cmd = self.ipfs_cmd();
        cmd.args(["name", "publish", "-Q"]);
        if let Some(key) = options.key {
            cmd.arg("--key");
            cmd.arg(key);
        }
        if options.allow_offline.unwrap_or(self.default_allow_offline) {
            cmd.arg("--allow-offline");
        }
        if let Some(resolve) = options.resolve {
            cmd.arg(if resolve {
                "--resolve=true"
            } else {
                "--resolve=false"
            });
        }
        cmd.arg(format!("/ipfs/{cid}"));

        tracing::info!(?cmd, "Running process");

        let output = cmd.output().await.int_err()?;
        let stdout = std::str::from_utf8(&output.stdout[..]).int_err()?;
        let stderr = std::str::from_utf8(&output.stderr[..]).int_err()?;

        if !output.status.success() {
            tracing::warn!(%stdout, %stderr, "Process exited with non-zero code");
            output.status.exit_ok().int_err()?;
        }

        Ok(stdout.trim().to_owned())
    }
}

pub struct IpfsKey {
    pub id: String,
    pub name: String,
}

impl IpfsKey {
    pub fn new(id: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
        }
    }
}

pub struct AddOptions<'a> {
    pub ignore: Option<&'a [&'a str]>,
}

#[derive(Default)]
pub struct PublishOptions<'a> {
    pub key: Option<&'a str>,
    pub allow_offline: Option<bool>,
    pub resolve: Option<bool>,
}
