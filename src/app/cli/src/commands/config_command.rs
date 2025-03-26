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
use crate::config::{ConfigScope, ConfigService};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// List
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Command)]
pub struct ConfigListCommand {
    config_svc: Arc<ConfigService>,

    #[dill::component(explicit)]
    user: bool,

    #[dill::component(explicit)]
    with_defaults: bool,
}

#[async_trait::async_trait(?Send)]
impl Command for ConfigListCommand {
    async fn run(&self) -> Result<(), CLIError> {
        let result = self.config_svc.list(
            if self.user {
                ConfigScope::User
            } else {
                ConfigScope::Flattened
            },
            self.with_defaults,
        );

        println!("{result}");

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Get
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Command)]
pub struct ConfigGetCommand {
    config_svc: Arc<ConfigService>,

    #[dill::component(explicit)]
    user: bool,

    #[dill::component(explicit)]
    with_defaults: bool,

    #[dill::component(explicit)]
    key: String,
}

#[async_trait::async_trait(?Send)]
impl Command for ConfigGetCommand {
    async fn run(&self) -> Result<(), CLIError> {
        let scope = if self.user {
            ConfigScope::User
        } else {
            ConfigScope::Flattened
        };

        if let Some(value) = self.config_svc.get(&self.key, scope, self.with_defaults) {
            println!("{value}");
        } else {
            return Err(CLIError::usage_error(format!("Key {} not found", self.key)));
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
    user: bool,

    #[dill::component(explicit)]
    key: String,

    #[dill::component(explicit)]
    value: Option<String>,
}

#[async_trait::async_trait(?Send)]
impl Command for ConfigSetCommand {
    async fn run(&self) -> Result<(), CLIError> {
        let scope = if self.user {
            ConfigScope::User
        } else {
            ConfigScope::Workspace
        };

        if let Some(value) = &self.value {
            self.config_svc.set(&self.key, value, scope)?;

            eprintln!(
                "{} {} {} {} {} {} {}",
                console::style("Set").green().bold(),
                self.key,
                console::style("to").green().bold(),
                value,
                console::style("in").green().bold(),
                format!("{scope:?}").to_lowercase(),
                console::style("scope").green().bold(),
            );
        } else {
            self.config_svc.unset(&self.key, scope)?;

            eprintln!(
                "{} {} {} {} {}",
                console::style("Removed").green().bold(),
                self.key,
                console::style("from").green().bold(),
                format!("{scope:?}").to_lowercase(),
                console::style("scope").green().bold(),
            );
        }
        Ok(())
    }
}
