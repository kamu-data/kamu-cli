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
use kamu::AuthenticationServiceImpl;

use crate::{CLIError, Command};

////////////////////////////////////////////////////////////////////////////////////////

pub struct GenerateTokenCommand {
    auth_service: Arc<AuthenticationServiceImpl>,
    gh_login: String,
    gh_access_token: String,
}

impl GenerateTokenCommand {
    pub fn new(
        auth_service: Arc<AuthenticationServiceImpl>,
        gh_login: String,
        gh_access_token: String,
    ) -> Self {
        Self {
            auth_service,
            gh_login,
            gh_access_token,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for GenerateTokenCommand {
    fn needs_workspace(&self) -> bool {
        false
    }

    async fn run(&mut self) -> Result<(), CLIError> {
        let provider_credentials_json =
            serde_json::to_string(&kamu_adapter_oauth::GithubProviderCredentials {
                access_token: self.gh_access_token.clone(),
            })
            .int_err()?;

        let token = self.auth_service.make_access_token(
            self.gh_login.clone(),
            kamu_adapter_oauth::LOGIN_METHOD_GITHUB,
            provider_credentials_json,
        )?;

        println!("{token}");
        Ok(())
    }
}
