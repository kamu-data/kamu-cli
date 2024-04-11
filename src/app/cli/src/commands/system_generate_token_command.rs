// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_accounts_services::AuthenticationServiceImpl;

use crate::{CLIError, Command};

////////////////////////////////////////////////////////////////////////////////////////

pub struct GenerateTokenCommand {
    auth_service: Arc<AuthenticationServiceImpl>,
    login: String,
    expiration_time_sec: usize,
}

impl GenerateTokenCommand {
    pub fn new(
        auth_service: Arc<AuthenticationServiceImpl>,
        login: String,
        expiration_time_sec: usize,
    ) -> Self {
        Self {
            auth_service,
            login,
            expiration_time_sec,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for GenerateTokenCommand {
    fn needs_workspace(&self) -> bool {
        false
    }

    async fn run(&mut self) -> Result<(), CLIError> {
        let token = self
            .auth_service
            .make_access_token(self.login.clone(), self.expiration_time_sec)?;

        println!("{token}");
        Ok(())
    }
}
