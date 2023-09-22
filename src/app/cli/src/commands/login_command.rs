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

use crate::services::{RemoteServerCredentialsService, RemoteServerLoginService};
use crate::{CLIError, Command, RemoteServerCredentialsScope, DEFAULT_LOGIN_URL};

////////////////////////////////////////////////////////////////////////////////////////

pub struct LoginCommand {
    remote_server_login_service: Arc<RemoteServerLoginService>,
    remote_server_credentials_service: Arc<RemoteServerCredentialsService>,
    scope: RemoteServerCredentialsScope,
    server: Option<Url>,
}

impl LoginCommand {
    pub fn new(
        remote_server_login_service: Arc<RemoteServerLoginService>,
        remote_server_credentials_service: Arc<RemoteServerCredentialsService>,
        scope: RemoteServerCredentialsScope,
        server: Option<Url>,
    ) -> Self {
        Self {
            remote_server_login_service,
            remote_server_credentials_service,
            scope,
            server,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for LoginCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        let server_url = self
            .server
            .as_ref()
            .map(|u| u.clone())
            .unwrap_or_else(|| Url::parse(DEFAULT_LOGIN_URL).unwrap());

        let credentials = self
            .remote_server_login_service
            .obtain_login_credentials(&server_url)
            .await?;

        self.remote_server_credentials_service
            .save_credentials(self.scope, credentials)?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////
