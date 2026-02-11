// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use super::{CLIError, Command};
use crate::config::{ConfigObjectFormat, ConfigScope, ConfigService};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Get
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Command)]
pub struct ConfigGetCommand {
    config_svc: Arc<ConfigService>,

    #[dill::component(explicit)]
    path: Option<String>,

    #[dill::component(explicit)]
    scope: ConfigScope,

    #[dill::component(explicit)]
    with_defaults: bool,

    #[dill::component(explicit)]
    output_format: ConfigObjectFormat,
}

#[async_trait::async_trait(?Send)]
impl Command for ConfigGetCommand {
    async fn run(&self) -> Result<(), CLIError> {
        if let Some(path) = &self.path {
            if let Some(value) =
                self.config_svc
                    .get(path, self.scope, self.with_defaults, self.output_format)?
            {
                println!("{value}");
            } else {
                return Err(CLIError::usage_error(format!("Path {path} not found")));
            }
        } else {
            let result =
                self.config_svc
                    .list(self.scope, self.with_defaults, self.output_format)?;

            println!("{result}");
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Set
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Command)]
pub struct ConfigSetCommand {
    config_svc: Arc<ConfigService>,

    #[dill::component(explicit)]
    path: String,

    #[dill::component(explicit)]
    value: Option<String>,

    #[dill::component(explicit)]
    scope: ConfigScope,

    #[dill::component(explicit)]
    input_format: ConfigObjectFormat,
}

#[async_trait::async_trait(?Send)]
impl Command for ConfigSetCommand {
    async fn run(&self) -> Result<(), CLIError> {
        let scope = self.scope;

        if let Some(value) = &self.value {
            self.config_svc
                .set(&self.path, value, self.input_format, scope)?;

            eprintln!(
                "{} {} {} {} {} {} {}",
                console::style("Set").green().bold(),
                self.path,
                console::style("to").green().bold(),
                value,
                console::style("in").green().bold(),
                format!("{scope:?}").to_lowercase(),
                console::style("scope").green().bold(),
            );
        } else {
            self.config_svc.unset(&self.path, scope)?;

            eprintln!(
                "{} {} {} {} {}",
                console::style("Removed").green().bold(),
                self.path,
                console::style("from").green().bold(),
                format!("{scope:?}").to_lowercase(),
                console::style("scope").green().bold(),
            );
        }
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
