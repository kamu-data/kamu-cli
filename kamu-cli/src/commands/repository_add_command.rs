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
    metadata_repo: Arc<dyn MetadataRepository>,
    name: RepositoryName,
    url: String,
}

impl RepositoryAddCommand {
    pub fn new<N, S>(metadata_repo: Arc<dyn MetadataRepository>, name: N, url: S) -> Self
    where
        N: TryInto<RepositoryName>,
        <N as TryInto<RepositoryName>>::Error: std::fmt::Debug,
        S: Into<String>,
    {
        Self {
            metadata_repo: metadata_repo,
            name: name.try_into().unwrap(),
            url: url.into(),
        }
    }
}

impl Command for RepositoryAddCommand {
    fn run(&mut self) -> Result<(), CLIError> {
        let url = Url::parse(&self.url).map_err(|e| {
            eprintln!("{}: {}", console::style("Invalid URL").red(), e);
            CLIError::Aborted
        })?;

        self.metadata_repo.add_repository(&self.name, url)?;

        eprintln!("{}: {}", console::style("Added").green(), &self.name);
        Ok(())
    }
}
