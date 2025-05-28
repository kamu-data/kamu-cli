// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use url::Url;

use crate::{CLIError, Command, odf_server};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Command)]
pub struct LogoutCommand {
    access_token_registry_service: Arc<odf_server::AccessTokenRegistryService>,

    #[dill::component(explicit)]
    scope: odf_server::AccessTokenStoreScope,

    #[dill::component(explicit)]
    server_url: Option<Url>,

    #[dill::component(explicit)]
    all: bool,
}

impl LogoutCommand {
    fn get_server_url(&self) -> Url {
        self.server_url
            .clone()
            .unwrap_or_else(|| Url::parse(odf_server::DEFAULT_ODF_FRONTEND_URL).unwrap())
    }
}

#[async_trait::async_trait(?Send)]
impl Command for LogoutCommand {
    async fn run(&self) -> Result<(), CLIError> {
        if self.server_url.is_some() && self.all {
            return Err(CLIError::usage_error(
                "Can't use --all and particular server name simultaneously",
            ));
        }

        if self.all {
            let removed_any = self
                .access_token_registry_service
                .drop_all_access_tokens_in_scope(self.scope)?;
            if removed_any {
                eprintln!(
                    "{}",
                    console::style("Logged out of all servers").green().bold(),
                );
            } else {
                eprintln!(
                    "{}",
                    console::style("Not logged in to any servers").yellow(),
                );
            }
        } else {
            let odf_server_frontend_url = self.get_server_url();

            let removed = self
                .access_token_registry_service
                .drop_access_token(self.scope, &odf_server_frontend_url)?;

            if removed {
                eprintln!(
                    "{} {}",
                    console::style("Logged out of").green().bold(),
                    odf_server_frontend_url
                );
            } else {
                eprintln!(
                    "{} {}",
                    console::style("Not logged in to").yellow(),
                    odf_server_frontend_url
                );
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
