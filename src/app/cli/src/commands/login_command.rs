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

#[dill::component]
#[dill::interface(dyn Command)]
pub struct LoginCommand {
    login_flow_service: Arc<odf_server::LoginFlowService>,

    #[dill::component(explicit)]
    scope: odf_server::AccessTokenStoreScope,

    #[dill::component(explicit)]
    server: Option<Url>,

    #[dill::component(explicit)]
    access_token: Option<String>,

    #[dill::component(explicit)]
    check: bool,

    #[dill::component(explicit)]
    repo_name: Option<odf::RepoName>,

    #[dill::component(explicit)]
    skip_add_repo: bool,

    #[dill::component(explicit)]
    predefined_odf_backend_url: Option<Url>,
}

impl LoginCommand {
    fn get_server_url(&self) -> Url {
        self.server
            .clone()
            .unwrap_or_else(|| Url::parse(odf_server::DEFAULT_ODF_FRONTEND_URL).unwrap())
    }
}

#[async_trait::async_trait(?Send)]
impl Command for LoginCommand {
    async fn run(&self) -> Result<(), CLIError> {
        let odf_server_url = self.get_server_url();

        // Check token and exit
        if self.check {
            return self
                .login_flow_service
                .check_access_token(&odf_server_url)
                .await;
        }

        let method = match &self.access_token {
            Some(access_token) => odf_server::LoginMethod::ExistingToken {
                access_token: access_token.clone(),
            },
            None => odf_server::LoginMethod::Interactive {
                predefined_backend_url: self.predefined_odf_backend_url.clone(),
            },
        };

        self.login_flow_service
            .ensure_logged_in(odf_server::LoginFlowOptions {
                server_url: odf_server_url,
                scope: self.scope,
                method,
                add_repo: !self.skip_add_repo,
                repo_name: self.repo_name.clone(),
                report_progress: true,
            })
            .await?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
