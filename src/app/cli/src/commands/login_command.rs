// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu::domain::CurrentAccountSubject;
use opendatafabric::AccountName;
use url::Url;

use crate::services::{RemoteServerCredentialsService, RemoteServerLoginService};
use crate::{
    CLIError,
    Command,
    RemoteServerAccessToken,
    RemoteServerAccountCredentials,
    RemoteServerCredentialsFindDetails,
    RemoteServerCredentialsScope,
    RemoteServerInvalidCredentialsError,
    RemoteServerLoginError,
    RemoteServerValidateLoginError,
    DEFAULT_LOGIN_URL,
};

////////////////////////////////////////////////////////////////////////////////////////

pub struct LoginCommand {
    remote_server_login_service: Arc<RemoteServerLoginService>,
    remote_server_credentials_service: Arc<RemoteServerCredentialsService>,
    current_account_subject: Arc<CurrentAccountSubject>,
    scope: RemoteServerCredentialsScope,
    server: Option<Url>,
}

impl LoginCommand {
    pub fn new(
        remote_server_login_service: Arc<RemoteServerLoginService>,
        remote_server_credentials_service: Arc<RemoteServerCredentialsService>,
        current_account_subject: Arc<CurrentAccountSubject>,
        scope: RemoteServerCredentialsScope,
        server: Option<Url>,
    ) -> Self {
        Self {
            remote_server_login_service,
            remote_server_credentials_service,
            current_account_subject,
            scope,
            server,
        }
    }

    async fn new_login(
        &self,
        remote_server_frontend_url: Url,
        account_name: AccountName,
    ) -> Result<(), CLIError> {
        let login_callback_response = self
            .remote_server_login_service
            .login(&remote_server_frontend_url)
            .await
            .map_err(|e| match e {
                RemoteServerLoginError::CredentialsNotObtained => {
                    CLIError::usage_error(e.to_string())
                }
                RemoteServerLoginError::Internal(e) => CLIError::failure(e),
            })?;

        self.remote_server_credentials_service.save_credentials(
            self.scope,
            &remote_server_frontend_url,
            &login_callback_response.backend_url,
            &account_name,
            RemoteServerAccountCredentials::AccessToken(RemoteServerAccessToken {
                access_token: login_callback_response.access_token,
            }),
        )?;

        eprintln!(
            "{}: {}@{}",
            console::style("Login successful").green().bold(),
            account_name,
            remote_server_frontend_url
        );

        Ok(())
    }

    async fn validate_login(
        &self,
        details: RemoteServerCredentialsFindDetails,
    ) -> Result<(), RemoteServerValidateLoginError> {
        self.remote_server_login_service
            .validate_login_credentials(&details.server_backend_url, &details.account_credentials)
            .await?;
        Ok(())
    }

    async fn handle_credentials_expired(
        &self,
        remote_server_frontend_url: &Url,
        account_name: &AccountName,
    ) -> Result<(), CLIError> {
        eprintln!(
            "{}: {}@{}",
            console::style("Dropping expired credentials")
                .yellow()
                .bold(),
            account_name,
            remote_server_frontend_url
        );

        self.remote_server_credentials_service
            .drop_credentials(self.scope, remote_server_frontend_url, account_name)
            .map_err(|e| CLIError::critical(e))?;

        Ok(())
    }

    fn handle_credentials_invalid(
        &self,
        e: RemoteServerInvalidCredentialsError,
        remote_server_frontend_url: &Url,
        account_name: &AccountName,
    ) -> Result<(), CLIError> {
        self.remote_server_credentials_service
            .drop_credentials(self.scope, remote_server_frontend_url, account_name)
            .map_err(|e| CLIError::critical(e))?;

        Err(CLIError::failure(e))
    }
}

#[async_trait::async_trait(?Send)]
impl Command for LoginCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        let account_name = match self.current_account_subject.as_ref() {
            CurrentAccountSubject::Logged(l) => l.account_name.clone(),
            CurrentAccountSubject::Anonymous(_) => panic!("Anonymous current account unexpected"),
        };

        let remote_server_frontend_url = self
            .server
            .as_ref()
            .map(|u| u.clone())
            .unwrap_or_else(|| Url::parse(DEFAULT_LOGIN_URL).unwrap());

        if let Some(details) = self.remote_server_credentials_service.find_by_frontend_url(
            self.scope,
            &remote_server_frontend_url,
            &account_name,
        ) {
            match self.validate_login(details).await {
                Ok(_) => {
                    eprintln!(
                        "{}: {}@{}",
                        console::style("Credentials valid").green().bold(),
                        account_name,
                        remote_server_frontend_url
                    );
                    Ok(())
                }
                Err(RemoteServerValidateLoginError::ExpiredCredentials(_)) => {
                    self.handle_credentials_expired(&remote_server_frontend_url, &account_name)
                        .await?;
                    self.new_login(remote_server_frontend_url, account_name)
                        .await
                }
                Err(RemoteServerValidateLoginError::InvalidCredentials(e)) => {
                    self.handle_credentials_invalid(e, &remote_server_frontend_url, &account_name)
                }
                Err(RemoteServerValidateLoginError::Internal(e)) => Err(CLIError::critical(e)),
            }
        } else {
            self.new_login(remote_server_frontend_url, account_name)
                .await
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////
