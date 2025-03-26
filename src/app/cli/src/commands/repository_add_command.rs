// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu::domain::*;
use url::Url;

use super::{CLIError, Command};

#[dill::component]
#[dill::interface(dyn Command)]
pub struct RepositoryAddCommand {
    remote_repo_reg: Arc<dyn RemoteRepositoryRegistry>,

    #[dill::component(explicit)]
    name: odf::RepoName,

    #[dill::component(explicit)]
    url: Url,
}

#[async_trait::async_trait(?Send)]
impl Command for RepositoryAddCommand {
    async fn run(&self) -> Result<(), CLIError> {
        self.remote_repo_reg
            .add_repository(&self.name, self.url.clone())
            .map_err(CLIError::failure)?;

        eprintln!("{}: {}", console::style("Added").green(), &self.name);
        Ok(())
    }
}
