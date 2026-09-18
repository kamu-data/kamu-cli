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
use kamu_accounts::{AccountProvider, CurrentAccountSubject};
use url::Url;

use crate::{CLIError, OutputConfig, odf_server};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// How a fresh access token is obtained when no valid one is already stored.
#[derive(Debug, Clone)]
pub enum LoginMethod {
    /// Browser device flow against a *frontend* URL.
    Interactive {
        /// E2E-only backend override; skips runtime-config discovery.
        predefined_backend_url: Option<Url>,
    },
    /// The caller already holds a platform access token. Stored verbatim and
    /// deliberately *not* validated.
    ExistingToken { access_token: String },
    /// Non-interactive OAuth provider token exchange against a *backend* URL.
    OAuth {
        provider: String,
        access_token: String,
    },
    /// Non-interactive login/password exchange against a *backend* URL.
    Password { login: String, password: String },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct LoginFlowOptions {
    /// Server URL exactly as the user typed it.
    ///
    /// Interactive login treats it as a frontend URL and discovers the backend;
    /// the other methods treat it as a backend URL directly.
    pub server_url: Url,

    pub scope: odf_server::AccessTokenStoreScope,

    pub method: LoginMethod,

    /// Register a remote repository on success. Only honored in workspace
    /// scope.
    pub add_repo: bool,

    pub repo_name: Option<odf::RepoName>,

    /// Emit the `Login successful` / `Access token valid` status lines.
    pub report_progress: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct LoginFlowOutcome {
    /// The backend URL the token is keyed by.
    pub backend_url: Url,
    pub result: LoginFlowResult,
}

#[derive(Debug, PartialEq, Eq)]
pub enum LoginFlowResult {
    /// A stored token was found and the backend confirmed it.
    AlreadyValid,
    /// A new token was obtained and persisted.
    LoggedIn,
    /// An expired token was dropped and a new one obtained.
    Reauthenticated,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The single implementation of "make sure we hold a valid access token for
/// this server", shared by `kamu login`, `kamu login oauth|password`, and
/// `kamu context add`.
pub struct LoginFlowService {
    login_service: Arc<odf_server::LoginService>,
    access_token_registry_service: Arc<odf_server::AccessTokenRegistryService>,
    remote_repo_reg: Arc<dyn RemoteRepositoryRegistry>,
    current_account_subject: Arc<CurrentAccountSubject>,
    output_config: Arc<OutputConfig>,
}

#[dill::component(pub)]
impl LoginFlowService {
    pub fn new(
        login_service: Arc<odf_server::LoginService>,
        access_token_registry_service: Arc<odf_server::AccessTokenRegistryService>,
        remote_repo_reg: Arc<dyn RemoteRepositoryRegistry>,
        current_account_subject: Arc<CurrentAccountSubject>,
        output_config: Arc<OutputConfig>,
    ) -> Self {
        Self {
            login_service,
            access_token_registry_service,
            remote_repo_reg,
            current_account_subject,
            output_config,
        }
    }

    /// Validate an existing token without obtaining or dropping anything,
    /// trying the URL as either a frontend or a backend one.
    pub async fn check_access_token(&self, server_url: &Url) -> Result<(), CLIError> {
        let Some(token_find_report) = self
            .access_token_registry_service
            .find_by_frontend_or_backend_url(server_url)
        else {
            return Err(CLIError::usage_error(format!(
                "No access token found for: {server_url}",
            )));
        };

        match self.validate_token(&token_find_report).await {
            Ok(()) => {
                self.report_access_token_valid(server_url);
                Ok(())
            }
            Err(odf_server::ValidateAccessTokenError::ExpiredToken(_)) => {
                Err(CLIError::usage_error("Access token expired"))
            }
            Err(odf_server::ValidateAccessTokenError::InvalidToken(_)) => {
                Err(CLIError::usage_error("Access token invalid"))
            }
            Err(odf_server::ValidateAccessTokenError::Internal(e)) => Err(CLIError::critical(e)),
        }
    }

    /// Ensure a valid access token is stored for `options.server_url`,
    /// obtaining one via `options.method` if necessary.
    pub async fn ensure_logged_in(
        &self,
        options: LoginFlowOptions,
    ) -> Result<LoginFlowOutcome, CLIError> {
        self.ensure_logged_in_impl(options, None).await
    }

    /// Like [`Self::ensure_logged_in`], but also probes an already-resolved
    /// backend URL: a token saved by `kamu login oauth|password` has no
    /// frontend URL, so a frontend-keyed lookup alone would miss it.
    pub async fn ensure_logged_in_for_context(
        &self,
        options: LoginFlowOptions,
        resolved_backend_url: &Url,
    ) -> Result<LoginFlowOutcome, CLIError> {
        self.ensure_logged_in_impl(options, Some(resolved_backend_url))
            .await
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    async fn ensure_logged_in_impl(
        &self,
        options: LoginFlowOptions,
        extra_backend_url: Option<&Url>,
    ) -> Result<LoginFlowOutcome, CLIError> {
        // Stored verbatim: there is nothing to look up and nothing to validate
        if let LoginMethod::ExistingToken { access_token } = &options.method {
            return self.login_with_existing_token(&options, access_token.clone());
        }

        let maybe_token_find_report = self.find_existing_token(&options, extra_backend_url);

        let mut reauthenticated = false;

        if let Some(token_find_report) = maybe_token_find_report {
            // Drop by where the token actually is, not by what the caller asked
            // for: lookups search workspace before user, and a token saved with
            // no frontend URL is only matched by its backend URL.
            let drop_scope = token_find_report.scope;
            let drop_url = token_find_report.backend_url.clone();

            match self.validate_token(&token_find_report).await {
                Ok(()) => {
                    self.report_access_token_valid_if(&options, &options.server_url);
                    return Ok(LoginFlowOutcome {
                        backend_url: token_find_report.backend_url,
                        result: LoginFlowResult::AlreadyValid,
                    });
                }
                Err(odf_server::ValidateAccessTokenError::ExpiredToken(_)) => {
                    // Reported against the URL the user typed, dropped by the
                    // URL the token was found under
                    self.handle_token_expired(drop_scope, &options.server_url, &drop_url)?;
                    reauthenticated = true;
                }
                Err(odf_server::ValidateAccessTokenError::InvalidToken(e)) => {
                    self.drop_access_token(drop_scope, &drop_url)?;
                    return Err(CLIError::failure(e));
                }
                Err(odf_server::ValidateAccessTokenError::Internal(e)) => {
                    return Err(CLIError::critical(e));
                }
            }
        }

        let backend_url = match &options.method {
            LoginMethod::Interactive {
                predefined_backend_url,
            } => {
                self.login_interactive(&options, predefined_backend_url.as_ref())
                    .await?
            }
            LoginMethod::OAuth { .. } | LoginMethod::Password { .. } => {
                self.login_silent(&options).await?
            }
            LoginMethod::ExistingToken { .. } => unreachable!(),
        };

        Ok(LoginFlowOutcome {
            backend_url,
            result: if reauthenticated {
                LoginFlowResult::Reauthenticated
            } else {
                LoginFlowResult::LoggedIn
            },
        })
    }

    /// The lookup key differs per method, matching how each token was saved.
    fn find_existing_token(
        &self,
        options: &LoginFlowOptions,
        extra_backend_url: Option<&Url>,
    ) -> Option<odf_server::AccessTokenFindReport> {
        let maybe_report = match &options.method {
            // Only the frontend URL: this path is the interactive browser login,
            // not a non-interactive one
            LoginMethod::Interactive { .. } => self
                .access_token_registry_service
                .find_by_frontend_url(&options.server_url),
            LoginMethod::OAuth { .. } | LoginMethod::Password { .. } => self
                .access_token_registry_service
                .find_by_backend_url(&options.server_url),
            LoginMethod::ExistingToken { .. } => None,
        };

        maybe_report.or_else(|| {
            extra_backend_url
                .and_then(|url| self.access_token_registry_service.find_by_backend_url(url))
        })
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    fn login_with_existing_token(
        &self,
        options: &LoginFlowOptions,
        access_token: String,
    ) -> Result<LoginFlowOutcome, CLIError> {
        // The URL acts as both frontend and backend: there is nothing to discover
        self.save_access_token(
            options.scope,
            Some(&options.server_url),
            &options.server_url,
            access_token,
        )?;

        if self.should_add_repo(options) {
            self.add_repository(options, &options.server_url, &options.server_url)?;
        }

        self.report_login_successful_if(options, &options.server_url);

        Ok(LoginFlowOutcome {
            backend_url: options.server_url.clone(),
            result: LoginFlowResult::LoggedIn,
        })
    }

    async fn login_interactive(
        &self,
        options: &LoginFlowOptions,
        predefined_backend_url: Option<&Url>,
    ) -> Result<Url, CLIError> {
        let login_interactive_response = self
            .login_service
            .login_interactive(&options.server_url, predefined_backend_url, |u| {
                self.report_device_flow_authorization_started(u);
            })
            .await
            .map_err(map_login_error)?;

        self.save_access_token(
            options.scope,
            Some(&options.server_url),
            &login_interactive_response.backend_url,
            login_interactive_response.access_token,
        )?;

        if self.should_add_repo(options) {
            self.add_repository(
                options,
                &options.server_url,
                &login_interactive_response.backend_url,
            )?;
        }

        // Note: the frontend URL is what the user typed, so that is what we echo
        self.report_login_successful_if(options, &options.server_url);

        Ok(login_interactive_response.backend_url)
    }

    async fn login_silent(&self, options: &LoginFlowOptions) -> Result<Url, CLIError> {
        let backend_url = &options.server_url;

        // Resolve the repository name before the network call, so a URL without a
        // host fails without burning a login round-trip
        let maybe_repo_name = if self.should_add_repo(options) {
            Some(self.silent_repo_name(options, backend_url)?)
        } else {
            None
        };

        let login_response = match &options.method {
            LoginMethod::OAuth {
                provider,
                access_token,
            } => {
                let oauth_login_method = match provider.to_ascii_lowercase().as_str() {
                    "github" => Ok(AccountProvider::OAuthGitHub.into()),
                    _ => Err(CLIError::usage_error(
                        "Only 'github' provider is supported at the moment",
                    )),
                }?;

                self.login_service
                    .login_oauth(backend_url, oauth_login_method, access_token)
                    .await
                    .map_err(map_login_error)?
            }
            LoginMethod::Password { login, password } => self
                .login_service
                .login_password(backend_url, login, password)
                .await
                .map_err(map_login_error)?,
            LoginMethod::Interactive { .. } | LoginMethod::ExistingToken { .. } => unreachable!(),
        };

        // Associate the token with the backend URL only, as we have no frontend here
        self.save_access_token(
            options.scope,
            None,
            backend_url,
            login_response.access_token,
        )?;

        if let Some(repo_name) = maybe_repo_name {
            self.add_repository_named(&repo_name, backend_url)?;
        }

        self.report_login_successful_if(options, backend_url);

        Ok(backend_url.clone())
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    async fn validate_token(
        &self,
        token_find_report: &odf_server::AccessTokenFindReport,
    ) -> Result<(), odf_server::ValidateAccessTokenError> {
        self.login_service
            .validate_access_token(
                &token_find_report.backend_url,
                &token_find_report.access_token,
            )
            .await
    }

    fn handle_token_expired(
        &self,
        scope: odf_server::AccessTokenStoreScope,
        reported_url: &Url,
        drop_url: &Url,
    ) -> Result<(), CLIError> {
        eprintln!(
            "{}: {}",
            s("Dropping expired access token").yellow().bold(),
            reported_url
        );

        self.drop_access_token(scope, drop_url)
    }

    fn drop_access_token(
        &self,
        scope: odf_server::AccessTokenStoreScope,
        server_url: &Url,
    ) -> Result<(), CLIError> {
        self.access_token_registry_service
            .drop_access_token(scope, server_url)
            .map_err(CLIError::critical)?;

        Ok(())
    }

    fn save_access_token(
        &self,
        scope: odf_server::AccessTokenStoreScope,
        frontend_url: Option<&Url>,
        backend_url: &Url,
        access_token: String,
    ) -> Result<(), CLIError> {
        // The registry panics on an anonymous subject, so reject it up front
        if let CurrentAccountSubject::Anonymous(_) = self.current_account_subject.as_ref() {
            return Err(CLIError::usage_error(
                "Cannot store an access token for an anonymous session",
            ));
        }

        self.access_token_registry_service.save_access_token(
            scope,
            frontend_url,
            backend_url,
            access_token,
        )?;

        Ok(())
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    fn should_add_repo(&self, options: &LoginFlowOptions) -> bool {
        options.add_repo && options.scope == odf_server::AccessTokenStoreScope::Workspace
    }

    /// Interactive and token logins name the repository after the frontend
    /// host.
    fn add_repository(
        &self,
        options: &LoginFlowOptions,
        frontend_url: &Url,
        backend_url: &Url,
    ) -> Result<(), CLIError> {
        let repo_name = match options.repo_name.clone() {
            Some(repo_name) => repo_name,
            None => odf::RepoName::try_from(frontend_url.host_str().unwrap())
                .map_err(CLIError::failure)?,
        };

        self.add_repository_named(&repo_name, backend_url)
    }

    /// Silent logins name the repository after the backend host, and report a
    /// missing host as a usage error rather than panicking.
    fn silent_repo_name(
        &self,
        options: &LoginFlowOptions,
        backend_url: &Url,
    ) -> Result<odf::RepoName, CLIError> {
        if let Some(repo_name) = options.repo_name.clone() {
            return Ok(repo_name);
        }

        let host = backend_url.host_str().ok_or_else(|| {
            CLIError::usage_error(format!(
                "Server URL does not contain the host part: {}",
                backend_url.as_str()
            ))
        })?;

        odf::RepoName::try_from(host).map_err(CLIError::failure)
    }

    fn add_repository_named(
        &self,
        repo_name: &odf::RepoName,
        backend_url: &Url,
    ) -> Result<(), CLIError> {
        match self.remote_repo_reg.add_repository(
            repo_name,
            backend_url.as_odf_protocol().map_err(CLIError::failure)?,
        ) {
            Ok(_) | Err(AddRepoError::AlreadyExists(_)) => Ok(()),
            Err(e) => Err(CLIError::failure(e)),
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    fn report_login_successful_if(&self, options: &LoginFlowOptions, url: &Url) {
        if options.report_progress {
            eprintln!("{}: {}", s("Login successful").green().bold(), url);
        }
    }

    fn report_access_token_valid_if(&self, options: &LoginFlowOptions, url: &Url) {
        if options.report_progress {
            self.report_access_token_valid(url);
        }
    }

    fn report_access_token_valid(&self, url: &Url) {
        eprintln!("{}: {}", s("Access token valid").green().bold(), url);
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn map_login_error(e: odf_server::LoginError) -> CLIError {
    match e {
        odf_server::LoginError::AccessFailed(e) => CLIError::usage_error(e.to_string()),
        odf_server::LoginError::Internal(e) => CLIError::failure(e),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
