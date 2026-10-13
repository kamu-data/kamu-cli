// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use url::Url;

use crate::{CLIError, Command, odf_server};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Non-interactive login: `kamu login oauth ...` / `kamu login password ...`.
///
/// The credential modes themselves live in [`odf_server::LoginMethod`], shared
/// with the interactive command and with `kamu context add`.
#[dill::component]
#[dill::interface(dyn Command)]
pub struct LoginSilentCommand {
    login_flow_service: Arc<odf_server::LoginFlowService>,

    #[dill::component(explicit)]
    scope: odf_server::AccessTokenStoreScope,

    #[dill::component(explicit)]
    server: Option<Url>,

    #[dill::component(explicit)]
    method: odf_server::LoginMethod,

    #[dill::component(explicit)]
    repo_name: Option<odf::RepoName>,

    #[dill::component(explicit)]
    skip_add_repo: bool,
}

impl LoginSilentCommand {
    fn get_server_url(&self) -> Url {
        self.server
            .clone()
            .unwrap_or_else(|| Url::parse(odf_server::DEFAULT_ODF_BACKEND_URL).unwrap())
    }
}

#[async_trait::async_trait(?Send)]
impl Command for LoginSilentCommand {
    async fn run(&self) -> Result<(), CLIError> {
        let odf_server_backend_url = self.get_server_url();

        self.login_flow_service
            .ensure_logged_in(odf_server::LoginFlowOptions {
                server_url: odf_server_backend_url,
                scope: self.scope,
                method: self.method.clone(),
                add_repo: !self.skip_add_repo,
                repo_name: self.repo_name.clone(),
                report_progress: true,
            })
            .await?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
