// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use console::style as s;
use kamu::UrlExt;
use kamu::domain::{AddRepoError, RemoteRepositoryRegistry};
use url::Url;

use crate::{CLIError, Command, OutputConfig, odf_server};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Command)]
pub struct LoginCommand {
    login_service: Arc<odf_server::LoginService>,
    access_token_registry_service: Arc<odf_server::AccessTokenRegistryService>,
    remote_repo_reg: Arc<dyn RemoteRepositoryRegistry>,
    output_config: Arc<OutputConfig>,

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

    fn new_login_with_token(
        &self,
        odf_server_backend_url: &Url,
        access_token: &str,
    ) -> Result<(), CLIError> {
        self.access_token_registry_service.save_access_token(
            self.scope,
            Some(odf_server_backend_url),
            odf_server_backend_url,
            access_token.to_string(),
        )?;
        if !self.skip_add_repo {
            self.add_repository(odf_server_backend_url, odf_server_backend_url)?;
        }
        eprintln!(
            "{}: {}",
            console::style("Login successful").green().bold(),
            odf_server_backend_url
        );

        Ok(())
    }

    fn add_repository(&self, frontend_url: &Url, backend_url: &Url) -> Result<(), CLIError> {
        let repo_name = self.repo_name.clone().unwrap_or(
            odf::RepoName::try_from(frontend_url.host_str().unwrap()).map_err(CLIError::failure)?,
        );
        match self.remote_repo_reg.add_repository(
            &repo_name,
            backend_url.as_odf_protocol().map_err(CLIError::failure)?,
        ) {
            Ok(_) | Err(AddRepoError::AlreadyExists(_)) => Ok(()),
            Err(e) => Err(CLIError::failure(e)),
        }
    }

    async fn new_login(&self, odf_server_frontend_url: Url) -> Result<(), CLIError> {
        let login_interactive_response = self
            .login_service
            .login_interactive(
                &odf_server_frontend_url,
                self.predefined_odf_backend_url.as_ref(),
                |u| {
                    self.report_device_flow_authorization_started(u);
                },
            )
            .await
            .map_err(|e| match e {
                odf_server::LoginError::AccessFailed(e) => CLIError::usage_error(e.to_string()),
                odf_server::LoginError::Internal(e) => CLIError::failure(e),
            })?;

        self.access_token_registry_service.save_access_token(
            self.scope,
            Some(&odf_server_frontend_url),
            &login_interactive_response.backend_url,
            login_interactive_response.access_token,
        )?;

        if !self.skip_add_repo {
            self.add_repository(
                &odf_server_frontend_url,
                &login_interactive_response.backend_url,
            )?;
        }

        eprintln!(
            "{}: {}",
            console::style("Login successful").green().bold(),
            odf_server_frontend_url
        );

        Ok(())
    }

    fn report_device_flow_authorization_started(&self, kamu_platform_login_url: &str) {
        if self.output_config.is_tty
            && self.output_config.verbosity_level == 0
            && !self.output_config.quiet
        {
            eprintln!(
                "{}\n{}\n",
                s("Please open this URL in the browser to login:")
                    .green()
                    .bold(),
                s(kamu_platform_login_url).bold(),
            );
        }
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

    fn handle_token_expired(&self, odf_server_frontend_url: &Url) -> Result<(), CLIError> {
        eprintln!(
            "{}: {}",
            console::style("Dropping expired access token")
                .yellow()
                .bold(),
            odf_server_frontend_url
        );

        self.access_token_registry_service
            .drop_access_token(self.scope, odf_server_frontend_url)
            .map_err(CLIError::critical)?;

        Ok(())
    }

    fn handle_token_invalid(
        &self,
        e: odf_server::InvalidTokenError,
        odf_server_frontend_url: &Url,
    ) -> Result<(), CLIError> {
        self.access_token_registry_service
            .drop_access_token(self.scope, odf_server_frontend_url)
            .map_err(CLIError::critical)?;

        Err(CLIError::failure(e))
    }
}

#[async_trait::async_trait(?Send)]
impl Command for LoginCommand {
    async fn run(&self) -> Result<(), CLIError> {
        let odf_server_url = self.get_server_url();

        // Check token and exit
        // Note: try the URL as both backend or frontend
        if self.check {
            if let Some(token_find_report) = self
                .access_token_registry_service
                .find_by_frontend_or_backend_url(&odf_server_url)
            {
                match self.validate_login(token_find_report).await {
                    Ok(_) => {
                        eprintln!(
                            "{}: {}",
                            console::style("Access token valid").green().bold(),
                            odf_server_url
                        );
                        Ok(())
                    }
                    Err(odf_server::ValidateAccessTokenError::ExpiredToken(_)) => {
                        Err(CLIError::usage_error("Access token expired"))
                    }
                    Err(odf_server::ValidateAccessTokenError::InvalidToken(_)) => {
                        Err(CLIError::usage_error("Access token invalid"))
                    }
                    Err(odf_server::ValidateAccessTokenError::Internal(e)) => {
                        Err(CLIError::critical(e))
                    }
                }
            } else {
                Err(CLIError::usage_error(format!(
                    "No access token found for: {odf_server_url}",
                )))
            }?;
            return Ok(());
        }

        // Login with existing token
        if let Some(access_token) = &self.access_token {
            return self.new_login_with_token(&odf_server_url, access_token);
        }

        // Validate token and trigger browser login flow if needed
        // Note: only check frontend URL, this flow is not supposed to cover
        // non-interactive login, only the interactive one with the browser
        if let Some(token_find_report) = self
            .access_token_registry_service
            .find_by_frontend_url(&odf_server_url)
        {
            match self.validate_login(token_find_report).await {
                Ok(_) => {
                    eprintln!(
                        "{}: {}",
                        console::style("Access token valid").green().bold(),
                        odf_server_url
                    );
                    Ok(())
                }
                Err(odf_server::ValidateAccessTokenError::ExpiredToken(_)) => {
                    self.handle_token_expired(&odf_server_url)?;
                    self.new_login(odf_server_url).await
                }
                Err(odf_server::ValidateAccessTokenError::InvalidToken(e)) => {
                    self.handle_token_invalid(e, &odf_server_url)
                }
                Err(odf_server::ValidateAccessTokenError::Internal(e)) => {
                    Err(CLIError::critical(e))
                }
            }
        } else {
            self.new_login(odf_server_url).await
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
