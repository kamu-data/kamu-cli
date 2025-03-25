// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::Arc;

use console::style as s;
use database_common::DatabaseTransactionRunner;
use dill::Catalog;
use internal_error::ResultIntoInternal;
use kamu::domain::TenancyConfig;
use kamu_accounts::*;
use kamu_accounts_services::PasswordLoginCredentials;
use kamu_adapter_http::FileUploadLimitConfig;
use kamu_adapter_oauth::*;
use kamu_datasets::DatasetEnvVarsConfig;
use tracing::Instrument;

use super::{CLIError, Command};
use crate::OutputConfig;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct APIServerRunCommand {
    base_catalog: Catalog,
    cli_catalog: Catalog,
    tenancy_config: TenancyConfig,
    output_config: Arc<OutputConfig>,
    address: Option<IpAddr>,
    port: Option<u16>,
    external_address: Option<IpAddr>,
    get_token: bool,
    predefined_accounts_config: Arc<PredefinedAccountsConfig>,
    file_upload_limit_config: Arc<FileUploadLimitConfig>,
    dataset_env_vars_config: Arc<DatasetEnvVarsConfig>,
    account_subject: Arc<CurrentAccountSubject>,
    github_auth_config: Arc<GithubAuthenticationConfig>,
    e2e_output_data_path: Option<PathBuf>,
}

impl APIServerRunCommand {
    pub fn new(
        base_catalog: Catalog,
        cli_catalog: Catalog,
        tenancy_config: TenancyConfig,
        output_config: Arc<OutputConfig>,
        address: Option<IpAddr>,
        port: Option<u16>,
        external_address: Option<IpAddr>,
        get_token: bool,
        predefined_accounts_config: Arc<PredefinedAccountsConfig>,
        file_upload_limit_config: Arc<FileUploadLimitConfig>,
        dataset_env_vars_config: Arc<DatasetEnvVarsConfig>,
        account_subject: Arc<CurrentAccountSubject>,
        github_auth_config: Arc<GithubAuthenticationConfig>,
        e2e_output_data_path: Option<PathBuf>,
    ) -> Self {
        Self {
            base_catalog,
            cli_catalog,
            tenancy_config,
            output_config,
            address,
            port,
            external_address,
            get_token,
            predefined_accounts_config,
            file_upload_limit_config,
            dataset_env_vars_config,
            account_subject,
            github_auth_config,
            e2e_output_data_path,
        }
    }

    async fn get_access_token(&self) -> Result<String, CLIError> {
        let current_account_name = match self.account_subject.as_ref() {
            CurrentAccountSubject::Logged(l) => l.account_name.clone(),
            CurrentAccountSubject::Anonymous(_) => {
                unreachable!("Cannot launch API server with anonymous account")
            }
        };

        let account_config = self
            .predefined_accounts_config
            .find_account_config_by_name(&current_account_name)
            .or_else(|| {
                Some(AccountConfig::test_config_from_name(
                    current_account_name.clone(),
                ))
            })
            .unwrap();

        let login_credentials = PasswordLoginCredentials {
            login: current_account_name.to_string(),
            password: account_config.get_password(),
        };

        let login_response = DatabaseTransactionRunner::new(self.base_catalog.clone())
            .transactional_with(|auth_svc: Arc<dyn AuthenticationService>| async move {
                auth_svc
                    .login(
                        PROVIDER_PASSWORD,
                        serde_json::to_string::<PasswordLoginCredentials>(&login_credentials)
                            .map_int_err(CLIError::critical)?,
                    )
                    .await
                    .map_int_err(CLIError::critical)
            })
            .instrument(tracing::debug_span!(
                "APIServerRunCommand::get_access_token"
            ))
            .await?;

        Ok(login_response.access_token)
    }
}

#[async_trait::async_trait(?Send)]
impl Command for APIServerRunCommand {
    async fn validate_args(&self) -> Result<(), CLIError> {
        if self.tenancy_config == TenancyConfig::MultiTenant {
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

    async fn run(&self) -> Result<(), CLIError> {
        let access_token = self.get_access_token().await?;

        let api_server = crate::explore::APIServer::new(
            &self.base_catalog,
            &self.cli_catalog,
            self.tenancy_config,
            self.address,
            self.port,
            self.file_upload_limit_config.clone(),
            self.dataset_env_vars_config.is_enabled(),
            self.external_address,
            self.e2e_output_data_path.as_ref(),
        )
        .await?;

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
