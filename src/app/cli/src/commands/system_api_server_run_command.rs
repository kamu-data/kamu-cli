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
use dill::Catalog;
use internal_error::ResultIntoInternal;
use kamu::domain::auth::AuthenticationService;
use kamu::domain::CurrentAccountSubject;
use kamu::{set_random_jwt_secret, ENV_VAR_KAMU_JWT_SECRET};
use kamu_adapter_oauth::{
    ENV_VAR_KAMU_AUTH_GITHUB_CLIENT_ID,
    ENV_VAR_KAMU_AUTH_GITHUB_CLIENT_SECRET,
};

use super::{CLIError, Command};
use crate::{accounts, check_env_var_set, OutputConfig};

pub struct APIServerRunCommand {
    base_catalog: Catalog,
    cli_catalog: Catalog,
    multi_tenant_workspace: bool,
    output_config: Arc<OutputConfig>,
    address: Option<IpAddr>,
    port: Option<u16>,
    get_token: bool,
    account_subject: Arc<CurrentAccountSubject>,
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
        account_subject: Arc<CurrentAccountSubject>,
    ) -> Self {
        Self {
            base_catalog,
            cli_catalog,
            multi_tenant_workspace,
            output_config,
            address,
            port,
            get_token,
            account_subject,
        }
    }

    fn check_required_env_vars(&self) -> Result<(), CLIError> {
        match check_env_var_set(ENV_VAR_KAMU_JWT_SECRET) {
            Ok(_) => {}
            Err(_) => set_random_jwt_secret(),
        };

        if self.multi_tenant_workspace {
            check_env_var_set(ENV_VAR_KAMU_AUTH_GITHUB_CLIENT_ID)?;
            check_env_var_set(ENV_VAR_KAMU_AUTH_GITHUB_CLIENT_SECRET)?;
        }

        Ok(())
    }

    async fn get_access_token(&self) -> Result<String, CLIError> {
        let current_account_name = match self.account_subject.as_ref() {
            CurrentAccountSubject::Logged(l) => l.account_name.clone(),
            CurrentAccountSubject::Anonymous(_) => {
                unreachable!("Cannot launch API server with anonymous account")
            }
        };
        let login_credentials = accounts::PasswordLoginCredentials {
            login: current_account_name.to_string(),
            // Note: note a mistake, use identical login and password, equal to account name
            password: current_account_name.to_string(),
        };

        let auth_svc = self
            .base_catalog
            .get_one::<dyn AuthenticationService>()
            .unwrap();
        let access_token = auth_svc
            .login(
                accounts::LOGIN_METHOD_PASSWORD,
                serde_json::to_string::<accounts::PasswordLoginCredentials>(&login_credentials)
                    .int_err()?,
            )
            .await
            .int_err()?
            .access_token;

        Ok(access_token)
    }
}

#[async_trait::async_trait(?Send)]
impl Command for APIServerRunCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        // Check required env variables are present before starting API server
        self.check_required_env_vars()?;
        let access_token = self.get_access_token().await?;

        // TODO: Cloning catalog is too expensive currently
        let api_server = crate::explore::APIServer::new(
            self.base_catalog.clone(),
            self.cli_catalog.clone(),
            self.multi_tenant_workspace,
            self.address,
            self.port,
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
