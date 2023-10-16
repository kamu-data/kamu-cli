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
use kamu::ENV_VAR_KAMU_JWT_SECRET;
use kamu_adapter_oauth::{
    ENV_VAR_KAMU_AUTH_GITHUB_CLIENT_ID,
    ENV_VAR_KAMU_AUTH_GITHUB_CLIENT_SECRET,
};

use super::{CLIError, Command};
use crate::{ensure_env_var_set, OutputConfig};

pub struct APIServerRunCommand {
    catalog: Catalog,
    multi_tenant_workspace: bool,
    output_config: Arc<OutputConfig>,
    address: Option<IpAddr>,
    port: Option<u16>,
}

impl APIServerRunCommand {
    pub fn new(
        catalog: Catalog,
        multi_tenant_workspace: bool,
        output_config: Arc<OutputConfig>,
        address: Option<IpAddr>,
        port: Option<u16>,
    ) -> Self {
        Self {
            catalog,
            multi_tenant_workspace,
            output_config,
            address,
            port,
        }
    }

    fn check_required_env_vars(&self) -> Result<(), CLIError> {
        ensure_env_var_set(ENV_VAR_KAMU_JWT_SECRET)?;

        if self.multi_tenant_workspace {
            ensure_env_var_set(ENV_VAR_KAMU_AUTH_GITHUB_CLIENT_ID)?;
            ensure_env_var_set(ENV_VAR_KAMU_AUTH_GITHUB_CLIENT_SECRET)?;
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
        }

        api_server.run().await.map_err(|e| CLIError::critical(e))?;

        Ok(())
    }
}
