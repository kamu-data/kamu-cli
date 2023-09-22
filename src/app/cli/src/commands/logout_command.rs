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

use crate::services::RemoteServerCredentialsService;
use crate::{CLIError, Command, RemoteServerCredentialsScope};

////////////////////////////////////////////////////////////////////////////////////////

pub struct LogoutCommand {
    remote_server_credentials_service: Arc<RemoteServerCredentialsService>,
    scope: RemoteServerCredentialsScope,
    server: Option<Url>,
}

impl LogoutCommand {
    pub fn new(
        remote_server_credentials_service: Arc<RemoteServerCredentialsService>,
        scope: RemoteServerCredentialsScope,
        server: Option<Url>,
    ) -> Self {
        Self {
            remote_server_credentials_service,
            scope,
            server,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for LogoutCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        self.remote_server_credentials_service
            .drop_credentials(self.scope, self.server.clone())?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////
