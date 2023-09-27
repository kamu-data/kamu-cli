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

use crate::services::{OdfServerLoginService, OdfServerTokenService};
use crate::{
    CLIError,
    Command,
    OdfServerAccessToken,
    OdfServerAccessTokenStoreScope,
    OdfServerInvalidTokenError,
    OdfServerLoginError,
    OdfServerTokenFindReport,
    OdfServerValidateAccessTokenError,
    DEFAULT_ODF_FRONTEND_URL,
};

////////////////////////////////////////////////////////////////////////////////////////

pub struct LoginCommand {
    odf_server_login_service: Arc<OdfServerLoginService>,
    odf_server_token_service: Arc<OdfServerTokenService>,
    scope: OdfServerAccessTokenStoreScope,
    server: Option<Url>,
}

impl LoginCommand {
    pub fn new(
        odf_server_login_service: Arc<OdfServerLoginService>,
        odf_server_token_service: Arc<OdfServerTokenService>,
        scope: OdfServerAccessTokenStoreScope,
        server: Option<Url>,
    ) -> Self {
        Self {
            odf_server_login_service,
            odf_server_token_service,
            scope,
            server,
        }
    }

    async fn new_login(&self, odf_server_frontend_url: Url) -> Result<(), CLIError> {
        let login_callback_response = self
            .odf_server_login_service
            .login(&odf_server_frontend_url)
            .await
            .map_err(|e| match e {
                OdfServerLoginError::AccessFailed => CLIError::usage_error(e.to_string()),
                OdfServerLoginError::Internal(e) => CLIError::failure(e),
            })?;

        self.odf_server_token_service.save_access_token(
            self.scope,
            &odf_server_frontend_url,
            &login_callback_response.backend_url,
            OdfServerAccessToken::new(login_callback_response.access_token),
        )?;

        eprintln!(
            "{}: {}",
            console::style("Login successful").green().bold(),
            odf_server_frontend_url
        );

        Ok(())
    }

    async fn validate_login(
        &self,
        token_find_report: OdfServerTokenFindReport,
    ) -> Result<(), OdfServerValidateAccessTokenError> {
        self.odf_server_login_service
            .validate_access_token(
                &token_find_report.odf_server_backend_url,
                &token_find_report.access_token,
            )
            .await?;
        Ok(())
    }

    async fn handle_token_expired(&self, odf_server_frontend_url: &Url) -> Result<(), CLIError> {
        eprintln!(
            "{}: {}",
            console::style("Dropping expired access token")
                .yellow()
                .bold(),
            odf_server_frontend_url
        );

        self.odf_server_token_service
            .drop_access_token(self.scope, odf_server_frontend_url)
            .map_err(|e| CLIError::critical(e))?;

        Ok(())
    }

    fn handle_token_invalid(
        &self,
        e: OdfServerInvalidTokenError,
        odf_server_frontend_url: &Url,
    ) -> Result<(), CLIError> {
        self.odf_server_token_service
            .drop_access_token(self.scope, odf_server_frontend_url)
            .map_err(|e| CLIError::critical(e))?;

        Err(CLIError::failure(e))
    }
}

#[async_trait::async_trait(?Send)]
impl Command for LoginCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        let odf_server_frontend_url = self
            .server
            .as_ref()
            .map(|u| u.clone())
            .unwrap_or_else(|| Url::parse(DEFAULT_ODF_FRONTEND_URL).unwrap());

        if let Some(token_find_report) = self
            .odf_server_token_service
            .find_by_frontend_url(self.scope, &odf_server_frontend_url)
        {
            match self.validate_login(token_find_report).await {
                Ok(_) => {
                    eprintln!(
                        "{}: {}",
                        console::style("Access token valid").green().bold(),
                        odf_server_frontend_url
                    );
                    Ok(())
                }
                Err(OdfServerValidateAccessTokenError::ExpiredToken(_)) => {
                    self.handle_token_expired(&odf_server_frontend_url).await?;
                    self.new_login(odf_server_frontend_url).await
                }
                Err(OdfServerValidateAccessTokenError::InvalidToken(e)) => {
                    self.handle_token_invalid(e, &odf_server_frontend_url)
                }
                Err(OdfServerValidateAccessTokenError::Internal(e)) => Err(CLIError::critical(e)),
            }
        } else {
            self.new_login(odf_server_frontend_url).await
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////
