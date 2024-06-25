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
use internal_error::ResultIntoInternal;
use kamu_accounts::PredefinedAccountsConfig;
use kamu_adapter_http::FileUploadLimitConfig;
use opendatafabric::AccountName;

use super::{CLIError, Command};
use crate::OutputConfig;

///////////////////////////////////////////////////////////////////////////////

pub struct UICommand {
    base_catalog: Catalog,
    multi_tenant_workspace: bool,
    current_account_name: AccountName,
    predefined_accounts_config: Arc<PredefinedAccountsConfig>,
    file_upload_limit_config: Arc<FileUploadLimitConfig>,
    output_config: Arc<OutputConfig>,
    address: Option<IpAddr>,
    port: Option<u16>,
    get_token: bool,
}

impl UICommand {
    pub fn new(
        base_catalog: Catalog,
        multi_tenant_workspace: bool,
        current_account_name: AccountName,
        predefined_accounts_config: Arc<PredefinedAccountsConfig>,
        file_upload_limit_config: Arc<FileUploadLimitConfig>,
        output_config: Arc<OutputConfig>,
        address: Option<IpAddr>,
        port: Option<u16>,
        get_token: bool,
    ) -> Self {
        Self {
            base_catalog,
            multi_tenant_workspace,
            current_account_name,
            predefined_accounts_config,
            file_upload_limit_config,
            output_config,
            address,
            port,
            get_token,
        }
    }
}

#[cfg(feature = "web-ui")]
#[async_trait::async_trait(?Send)]
impl Command for UICommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        let web_server = crate::explore::WebUIServer::new(
            self.base_catalog.clone(),
            self.multi_tenant_workspace,
            self.current_account_name.clone(),
            self.predefined_accounts_config.clone(),
            self.file_upload_limit_config.clone(),
            self.address,
            self.port,
        )
        .await;

        let web_server_url = format!("http://{}", web_server.local_addr());
        tracing::info!("HTTP server is listening on: {}", web_server_url);

        if self.get_token {
            tracing::info!(
                token = %web_server.get_access_token(),
                "Issued API server token"
            );
        }

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

            if self.get_token {
                eprintln!(
                    "{} {}",
                    s("JWT token:").green().bold(),
                    s(web_server.get_access_token()).dim()
                );
            }
        }

        let _ = webbrowser::open(&web_server_url);

        web_server.run().await.map_err(CLIError::critical)?;

        Ok(())
    }
}

///////////////////////////////////////////////////////////////////////////////

#[cfg(not(feature = "web-ui"))]
#[async_trait::async_trait(?Send)]
impl Command for UICommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        Err(CLIError::usage_error(
            "This version of kamu was compiled without the embedded Web UI",
        ))
    }
}

///////////////////////////////////////////////////////////////////////////////
