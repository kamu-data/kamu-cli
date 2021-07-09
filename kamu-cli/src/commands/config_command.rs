use super::{Command, Error};
use crate::config::{ConfigScope, ConfigService};

use std::sync::Arc;

////////////////////////////////////////////////////////////////////////////////////////
// List
////////////////////////////////////////////////////////////////////////////////////////

pub struct ConfigListCommand {
    config_svc: Arc<ConfigService>,
    user: bool,
}

impl ConfigListCommand {
    pub fn new(config_svc: Arc<ConfigService>, user: bool) -> Self {
        Self { config_svc, user }
    }
}

impl Command for ConfigListCommand {
    fn needs_workspace(&self) -> bool {
        false
    }

    fn run(&mut self) -> Result<(), Error> {
        let result = self.config_svc.list(if self.user {
            ConfigScope::User
        } else {
            ConfigScope::Flattened
        });

        println!("{}", result);

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////
// Get
////////////////////////////////////////////////////////////////////////////////////////

pub struct ConfigGetCommand {
    config_svc: Arc<ConfigService>,
    user: bool,
    key: String,
}

impl ConfigGetCommand {
    pub fn new(config_svc: Arc<ConfigService>, user: bool, key: String) -> Self {
        Self {
            config_svc,
            user,
            key,
        }
    }
}

impl Command for ConfigGetCommand {
    fn needs_workspace(&self) -> bool {
        false
    }

    fn run(&mut self) -> Result<(), Error> {
        let scope = if self.user {
            ConfigScope::User
        } else {
            ConfigScope::Flattened
        };

        if let Some(value) = self.config_svc.get(&self.key, scope) {
            println!("{}", value);
        } else {
            return Err(Error::UsageError {
                msg: format!("Key {} not found", self.key),
            });
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////
// Set
////////////////////////////////////////////////////////////////////////////////////////

pub struct ConfigSetCommand {
    config_svc: Arc<ConfigService>,
    user: bool,
    key: String,
    value: Option<String>,
}

impl ConfigSetCommand {
    pub fn new(
        config_svc: Arc<ConfigService>,
        user: bool,
        key: String,
        value: Option<String>,
    ) -> Self {
        Self {
            config_svc,
            user,
            key,
            value,
        }
    }
}

impl Command for ConfigSetCommand {
    fn needs_workspace(&self) -> bool {
        false
    }

    fn run(&mut self) -> Result<(), Error> {
        let scope = if self.user {
            ConfigScope::User
        } else {
            ConfigScope::Workspace
        };

        if let Some(value) = &self.value {
            self.config_svc.set(&self.key, &value, scope)?;

            eprintln!(
                "{} {} {} {} {} {} {}",
                console::style("Set").green().bold(),
                self.key,
                console::style("to").green().bold(),
                value,
                console::style("in").green().bold(),
                format!("{:?}", scope).to_lowercase(),
                console::style("scope").green().bold(),
            );
        } else {
            self.config_svc.unset(&self.key, scope)?;

            eprintln!(
                "{} {} {}",
                console::style("Removed").green().bold(),
                self.key,
                console::style(format!("from {:?} scope", scope).to_lowercase())
                    .green()
                    .bold(),
            );
        }
        Ok(())
    }
}
