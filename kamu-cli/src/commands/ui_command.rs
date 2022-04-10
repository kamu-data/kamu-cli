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

use super::{CLIError, Command};
use crate::OutputConfig;

use console::style as s;
use dill::Catalog;
use std::{net::IpAddr, str::FromStr, sync::Arc};

pub struct UICommand {
    catalog: Catalog,
    output_config: Arc<OutputConfig>,
    address: Option<IpAddr>,
    port: Option<u16>,
}

impl UICommand {
    pub fn new(
        catalog: Catalog,
        output_config: Arc<OutputConfig>,
        address: Option<IpAddr>,
        port: Option<u16>,
    ) -> Self {
        Self {
            catalog,
            output_config,
            address,
            port,
        }
    }
}

#[cfg(feature = "web-ui")]
#[async_trait::async_trait(?Send)]
impl Command for UICommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        // TODO: Cloning catalog is too expensive currently
        let web_server =
            crate::explore::WebUIServer::new(self.catalog.clone(), self.address, self.port);

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
