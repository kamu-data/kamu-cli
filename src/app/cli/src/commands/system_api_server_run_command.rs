// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::IpAddr;
use std::sync::Arc;

use console::style as s;
use database_common::DatabaseTransactionRunner;
use dill::Catalog;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_accounts::*;
use kamu_accounts_services::PasswordLoginCredentials;
use kamu_adapter_oauth::*;

use super::{CLIError, Command};
use crate::OutputConfig;

///////////////////////////////////////////////////////////////////////////////

pub struct APIServerRunCommand {
    base_catalog: Catalog,
    cli_catalog: Catalog,
    multi_tenant_workspace: bool,
    output_config: Arc<OutputConfig>,
    address: Option<IpAddr>,
    port: Option<u16>,
    get_token: bool,
    predefined_accounts_config: Arc<PredefinedAccountsConfig>,
    account_subject: Arc<CurrentAccountSubject>,
    github_auth_config: Arc<GithubAuthenticationConfig>,
    is_e2e_testing: bool,
}

impl APIServerRunCommand {
    pub fn new(
        base_catalog: Catalog,
        cli_catalog: Catalog,
        multi_tenant_workspace: bool,
        output_config: Arc<OutputConfig>,
        address: Option<IpAddr>,
        port: Option<u16>,
        get_token: bool,
        predefined_accounts_config: Arc<PredefinedAccountsConfig>,
        account_subject: Arc<CurrentAccountSubject>,
        github_auth_config: Arc<GithubAuthenticationConfig>,
        is_e2e_testing: bool,
    ) -> Self {
        Self {
            base_catalog,
            cli_catalog,
            multi_tenant_workspace,
            output_config,
            address,
            port,
            get_token,
            predefined_accounts_config,
            account_subject,
            github_auth_config,
            is_e2e_testing,
        }
    }

    fn check_required_env_vars(&self) -> Result<(), CLIError> {
        if self.multi_tenant_workspace {
            if self.github_auth_config.client_id.is_empty() {
                return Err(CLIError::missed_env_var(ENV_VAR_KAMU_AUTH_GITHUB_CLIENT_ID));
            }

            if self.github_auth_config.client_secret.is_empty() {
                return Err(CLIError::missed_env_var(
                    ENV_VAR_KAMU_AUTH_GITHUB_CLIENT_SECRET,
                ));
            }
        }

        Ok(())
    }

    async fn get_access_token(&self) -> Result<String, InternalError> {
        let current_account_name = match self.account_subject.as_ref() {
            CurrentAccountSubject::Logged(l) => l.account_name.clone(),
            CurrentAccountSubject::Anonymous(_) => {
                unreachable!("Cannot launch API server with anonymous account")
            }
        };

        let account_config = self
            .predefined_accounts_config
            .find_account_config_by_name(&current_account_name)
            .or_else(|| Some(AccountConfig::from_name(current_account_name.clone())))
            .unwrap();

        let login_credentials = PasswordLoginCredentials {
            login: current_account_name.to_string(),
            password: account_config.get_password(),
        };

        let login_response = DatabaseTransactionRunner::run_transactional_with(
            &self.base_catalog,
            |auth_svc: Arc<dyn AuthenticationService>| async move {
                auth_svc
                    .login(
                        PROVIDER_PASSWORD,
                        serde_json::to_string::<PasswordLoginCredentials>(&login_credentials)
                            .int_err()?,
                    )
                    .await
                    .int_err()
            },
        )
        .await?;

        Ok(login_response.access_token)
    }
}

#[async_trait::async_trait(?Send)]
impl Command for APIServerRunCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        self.check_required_env_vars()?;

        let access_token = self.get_access_token().await?;

        let api_server = crate::explore::APIServer::new(
            self.base_catalog.clone(),
            &self.cli_catalog,
            self.multi_tenant_workspace,
            self.address,
            self.port,
            self.is_e2e_testing,
        );

        tracing::info!(
            "API server is listening on: http://{}",
            api_server.local_addr()
        );

        if self.output_config.is_tty
            && self.output_config.verbosity_level == 0
            && !self.output_config.quiet
        {
            eprintln!(
                "{}\n  {}",
                s("API server is listening on:").green().bold(),
                s(format!("http://{}", api_server.local_addr())).bold(),
            );
            eprintln!("{}", s("Use Ctrl+C to stop the server").yellow());

            if self.get_token {
                eprintln!(
                    "{} {}",
                    s("JWT token:").green().bold(),
                    s(access_token).dim()
                );
            }
        }

        api_server.run().await.map_err(CLIError::critical)?;

        Ok(())
    }
}

///////////////////////////////////////////////////////////////////////////////
