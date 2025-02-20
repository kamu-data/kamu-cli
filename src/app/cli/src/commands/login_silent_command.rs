// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_adapter_oauth::PROVIDER_GITHUB;
use url::Url;

use crate::{odf_server, CLIError, Command};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum LoginSilentMode {
    OAuth(LoginSilentModeOAuth),
    Password(LoginSilentModePassword),
}

#[derive(Debug)]
pub struct LoginSilentModeOAuth {
    pub provider: String,
    pub access_token: String,
}

#[derive(Debug)]
pub struct LoginSilentModePassword {
    pub login: String,
    pub password: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct LoginSilentCommand {
    login_service: Arc<odf_server::LoginService>,
    access_token_registry_service: Arc<odf_server::AccessTokenRegistryService>,
    scope: odf_server::AccessTokenStoreScope,
    server: Option<Url>,
    mode: LoginSilentMode,
}

impl LoginSilentCommand {
    pub fn new(
        login_service: Arc<odf_server::LoginService>,
        access_token_registry_service: Arc<odf_server::AccessTokenRegistryService>,
        scope: odf_server::AccessTokenStoreScope,
        server: Option<Url>,
        mode: LoginSilentMode,
    ) -> Self {
        Self {
            login_service,
            access_token_registry_service,
            scope,
            server,
            mode,
        }
    }

    fn get_server_url(&self) -> Url {
        self.server
            .clone()
            .unwrap_or_else(|| Url::parse(odf_server::DEFAULT_ODF_BACKEND_URL).unwrap())
    }

    async fn new_login(&self, odf_server_backend_url: Url) -> Result<(), CLIError> {
        // Execute login method depending on the input mode
        let login_response = match &self.mode {
            LoginSilentMode::OAuth(github_mode) => {
                let oauth_login_method = match github_mode.provider.to_ascii_lowercase().as_str() {
                    "github" => Ok(PROVIDER_GITHUB),
                    _ => Err(CLIError::usage_error(
                        "Only 'github' provider is supported at the moment",
                    )),
                }?;
                self.login_service
                    .login_oauth(
                        &odf_server_backend_url,
                        oauth_login_method,
                        &github_mode.access_token,
                    )
                    .await
                    .map_err(|e| match e {
                        odf_server::LoginError::AccessFailed(e) => {
                            CLIError::usage_error(e.to_string())
                        }
                        odf_server::LoginError::Internal(e) => CLIError::failure(e),
                    })?
            }

            LoginSilentMode::Password(password_mode) => self
                .login_service
                .login_password(
                    &odf_server_backend_url,
                    &password_mode.login,
                    &password_mode.password,
                )
                .await
                .map_err(|e| match e {
                    odf_server::LoginError::AccessFailed(e) => CLIError::usage_error(e.to_string()),
                    odf_server::LoginError::Internal(e) => CLIError::failure(e),
                })?,
        };

        // Save access token and associate it with backend URL only,
        // as we don't have frontend here
        self.access_token_registry_service.save_access_token(
            self.scope,
            None,
            &odf_server_backend_url,
            login_response.access_token,
        )?;

        eprintln!(
            "{}: {}",
            console::style("Login successful").green().bold(),
            odf_server_backend_url
        );

        Ok(())
    }

    async fn validate_login(
        &self,
        token_find_report: odf_server::AccessTokenFindReport,
    ) -> Result<(), odf_server::ValidateAccessTokenError> {
        self.login_service
            .validate_access_token(
                &token_find_report.backend_url,
                &token_find_report.access_token,
            )
            .await?;
        Ok(())
    }

    fn handle_token_expired(&self, odf_server_backend_url: &Url) -> Result<(), CLIError> {
        eprintln!(
            "{}: {}",
            console::style("Dropping expired access token")
                .yellow()
                .bold(),
            odf_server_backend_url,
        );

        self.access_token_registry_service
            .drop_access_token(self.scope, odf_server_backend_url)
            .map_err(CLIError::critical)?;

        Ok(())
    }

    fn handle_token_invalid(
        &self,
        e: odf_server::InvalidTokenError,
        odf_server_backend_url: &Url,
    ) -> Result<(), CLIError> {
        self.access_token_registry_service
            .drop_access_token(self.scope, odf_server_backend_url)
            .map_err(CLIError::critical)?;

        Err(CLIError::failure(e))
    }
}

#[async_trait::async_trait(?Send)]
impl Command for LoginSilentCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        let odf_server_backend_url = self.get_server_url();

        // Validate token and trigger browser login flow if needed
        if let Some(token_find_report) = self
            .access_token_registry_service
            .find_by_backend_url(&odf_server_backend_url)
        {
            match self.validate_login(token_find_report).await {
                Ok(_) => {
                    eprintln!(
                        "{}: {}",
                        console::style("Access token valid").green().bold(),
                        odf_server_backend_url
                    );
                    Ok(())
                }
                Err(odf_server::ValidateAccessTokenError::ExpiredToken(_)) => {
                    self.handle_token_expired(&odf_server_backend_url)?;
                    self.new_login(odf_server_backend_url).await
                }
                Err(odf_server::ValidateAccessTokenError::InvalidToken(e)) => {
                    self.handle_token_invalid(e, &odf_server_backend_url)
                }
                Err(odf_server::ValidateAccessTokenError::Internal(e)) => {
                    Err(CLIError::critical(e))
                }
            }
        } else {
            self.new_login(odf_server_backend_url).await
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
