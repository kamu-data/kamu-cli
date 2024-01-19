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

use crate::{odf_server, CLIError, Command};

////////////////////////////////////////////////////////////////////////////////////////

pub struct LogoutCommand {
    access_token_registry_service: Arc<odf_server::AccessTokenRegistryService>,
    scope: odf_server::AccessTokenStoreScope,
    server: Option<Url>,
}

impl LogoutCommand {
    pub fn new(
        access_token_registry_service: Arc<odf_server::AccessTokenRegistryService>,
        scope: odf_server::AccessTokenStoreScope,
        server: Option<Url>,
    ) -> Self {
        Self {
            access_token_registry_service,
            scope,
            server,
        }
    }

    fn get_server_url(&self) -> Url {
        self.server
            .as_ref()
            .cloned()
            .unwrap_or_else(|| Url::parse(odf_server::DEFAULT_ODF_FRONTEND_URL).unwrap())
    }
}

#[async_trait::async_trait(?Send)]
impl Command for LogoutCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        let odf_server_frontend_url = self.get_server_url();

        self.access_token_registry_service
            .drop_access_token(self.scope, &odf_server_frontend_url)?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////
