// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(unused_imports)]
#![allow(dead_code)]

use std::net::IpAddr;
use std::str::FromStr;
use std::sync::Arc;

use console::style as s;
use dill::Catalog;
use kamu::{set_random_jwt_secret, ENV_VAR_KAMU_JWT_SECRET};
use opendatafabric::AccountName;

use super::{CLIError, Command};
use crate::{check_env_var_set, OutputConfig};

pub struct UICommand {
    base_catalog: Catalog,
    current_account_name: AccountName,
    output_config: Arc<OutputConfig>,
    address: Option<IpAddr>,
    port: Option<u16>,
}

impl UICommand {
    pub fn new(
        base_catalog: Catalog,
        current_account_name: AccountName,
        output_config: Arc<OutputConfig>,
        address: Option<IpAddr>,
        port: Option<u16>,
    ) -> Self {
        Self {
            base_catalog,
            current_account_name,
            output_config,
            address,
            port,
        }
    }

    fn ensure_required_env_vars(&self) {
        match check_env_var_set(ENV_VAR_KAMU_JWT_SECRET) {
            Ok(_) => {}
            Err(_) => set_random_jwt_secret(),
        }
    }
}

#[cfg(feature = "web-ui")]
#[async_trait::async_trait(?Send)]
impl Command for UICommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        // Ensure required env variables are present before starting UI server
        self.ensure_required_env_vars();

        let web_server = crate::explore::WebUIServer::new(
            self.base_catalog.clone(),
            self.current_account_name.clone(),
            self.address,
            self.port,
        );

        let web_server_url = format!("http://{}", web_server.local_addr());
        tracing::info!("HTTP server is listening on: {}", web_server_url);

        if self.output_config.is_tty
            && self.output_config.verbosity_level == 0
            && !self.output_config.quiet
        {
            eprintln!(
                "{}\n  {}",
                s("HTTP server is listening on:").green().bold(),
                s(&web_server_url).bold(),
            );
            eprintln!("{}", s("Use Ctrl+C to stop the server").yellow());
        }

        let _ = webbrowser::open(&web_server_url);

        web_server.run().await.map_err(|e| CLIError::critical(e))?;

        Ok(())
    }
}

#[cfg(not(feature = "web-ui"))]
#[async_trait::async_trait(?Send)]
impl Command for UICommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        Err(CLIError::usage_error(
            "This version of kamu was compiled without the embedded Web UI",
        ))
    }
}
