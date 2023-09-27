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

use crate::services::OdfServerTokenService;
use crate::{CLIError, Command, OdfServerAccessTokenStoreScope, DEFAULT_ODF_FRONTEND_URL};

////////////////////////////////////////////////////////////////////////////////////////

pub struct LogoutCommand {
    odf_server_token_service: Arc<OdfServerTokenService>,
    scope: OdfServerAccessTokenStoreScope,
    server: Option<Url>,
}

impl LogoutCommand {
    pub fn new(
        odf_server_token_service: Arc<OdfServerTokenService>,
        scope: OdfServerAccessTokenStoreScope,
        server: Option<Url>,
    ) -> Self {
        Self {
            odf_server_token_service,
            scope,
            server,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for LogoutCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        let odf_server_frontend_url = self
            .server
            .clone()
            .unwrap_or_else(|| Url::parse(DEFAULT_ODF_FRONTEND_URL).unwrap());

        self.odf_server_token_service
            .drop_access_token(self.scope, &odf_server_frontend_url)?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////
