// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{CLIError, Command};
use kamu::domain::*;
use opendatafabric::RepositoryName;

use std::sync::Arc;
use url::Url;

pub struct RepositoryAddCommand {
    remote_repo_reg: Arc<dyn RemoteRepositoryRegistry>,
    name: RepositoryName,
    url: String,
}

impl RepositoryAddCommand {
    pub fn new<S>(
        remote_repo_reg: Arc<dyn RemoteRepositoryRegistry>,
        name: RepositoryName,
        url: S,
    ) -> Self
    where
        S: Into<String>,
    {
        Self {
            remote_repo_reg,
            name,
            url: url.into(),
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for RepositoryAddCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        let url = Url::parse(&self.url).map_err(|e| {
            eprintln!("{}: {}", console::style("Invalid URL").red(), e);
            CLIError::Aborted
        })?;

        self.remote_repo_reg
            .add_repository(&self.name, url)
            .map_err(CLIError::failure)?;

        eprintln!("{}: {}", console::style("Added").green(), &self.name);
        Ok(())
    }
}
