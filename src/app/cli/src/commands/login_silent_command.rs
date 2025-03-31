// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu::domain::{AddRepoError, RemoteRepositoryRegistry};
use kamu_adapter_oauth::PROVIDER_GITHUB;
use url::Url;

use crate::{odf_server, CLIError, Command};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub enum LoginSilentMode {
    OAuth(LoginSilentModeOAuth),
    Password(LoginSilentModePassword),
}

#[derive(Debug, Clone)]
pub struct LoginSilentModeOAuth {
    pub provider: String,
    pub access_token: String,
}

#[derive(Debug, Clone)]
pub struct LoginSilentModePassword {
    pub login: String,
    pub password: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Command)]
pub struct LoginSilentCommand {
    login_service: Arc<odf_server::LoginService>,
    access_token_registry_service: Arc<odf_server::AccessTokenRegistryService>,
    remote_repo_reg: Arc<dyn RemoteRepositoryRegistry>,

    #[dill::component(explicit)]
    scope: odf_server::AccessTokenStoreScope,

    #[dill::component(explicit)]
    server: Option<Url>,

    #[dill::component(explicit)]
    mode: LoginSilentMode,

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

    async fn new_login(&self, odf_server_backend_url: Url) -> Result<(), CLIError> {
        let maybe_repo_name = if self.skip_add_repo {
            None
        } else {
            let repo_name = if let Some(repo_name) = self.repo_name.as_ref() {
                repo_name.clone()
            } else {
                let host = odf_server_backend_url.host_str().ok_or_else(|| {
                    CLIError::usage_error(format!(
                        "Server URL does not contain the host part: {}",
                        odf_server_backend_url.as_str()
                    ))
                })?;
                odf::RepoName::try_from(host).map_err(CLIError::failure)?
            };
            Some(repo_name)
        };

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

        if let Some(repo_name) = maybe_repo_name {
            self.add_repository(&repo_name, &odf_server_backend_url)?;
        }

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

    fn add_repository(
        &self,
        repo_name: &odf::RepoName,
        odf_server_backend_url: &Url,
    ) -> Result<(), CLIError> {
        use kamu::UrlExt;

        match self.remote_repo_reg.add_repository(
            repo_name,
            odf_server_backend_url
                .as_odf_protocol()
                .map_err(CLIError::failure)?,
        ) {
            Ok(_) | Err(AddRepoError::AlreadyExists(_)) => Ok(()),
            Err(e) => Err(CLIError::failure(e)),
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for LoginSilentCommand {
    async fn run(&self) -> Result<(), CLIError> {
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
