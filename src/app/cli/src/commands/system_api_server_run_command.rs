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
use kamu::{set_random_jwt_secret, ENV_VAR_KAMU_JWT_SECRET};
use kamu_adapter_oauth::{
    ENV_VAR_KAMU_AUTH_GITHUB_CLIENT_ID,
    ENV_VAR_KAMU_AUTH_GITHUB_CLIENT_SECRET,
};
use opendatafabric::AccountName;

use super::{CLIError, Command};
use crate::{check_env_var_set, OutputConfig};

pub struct APIServerRunCommand {
    catalog: Catalog,
    multi_tenant_workspace: bool,
    output_config: Arc<OutputConfig>,
    address: Option<IpAddr>,
    port: Option<u16>,
    get_token: bool,
    account_name: AccountName,
}

impl APIServerRunCommand {
    pub fn new(
        catalog: Catalog,
        multi_tenant_workspace: bool,
        output_config: Arc<OutputConfig>,
        address: Option<IpAddr>,
        port: Option<u16>,
        get_token: bool,
        account_name: AccountName,
    ) -> Self {
        Self {
            catalog,
            multi_tenant_workspace,
            output_config,
            address,
            port,
            get_token,
            account_name,
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
}

#[async_trait::async_trait(?Send)]
impl Command for APIServerRunCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        // Check required env variables are present before starting API server
        self.check_required_env_vars()?;

        // TODO: Cloning catalog is too expensive currently
        let api_server = crate::explore::APIServer::new(
            self.catalog.clone(),
            self.multi_tenant_workspace,
            self.address,
            self.port,
            self.account_name.clone(),
        )
        .await;

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
                    s(api_server.get_access_token()).dim()
                );
            }
        }

        api_server.run().await.map_err(|e| CLIError::critical(e))?;

        Ok(())
    }
}
