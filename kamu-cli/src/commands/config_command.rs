use std::{cell::RefCell, rc::Rc};

use crate::config::{ConfigScope, ConfigService};

use super::{Command, Error};

////////////////////////////////////////////////////////////////////////////////////////
// List
////////////////////////////////////////////////////////////////////////////////////////

pub struct ConfigListCommand {
    config_svc: Rc<RefCell<ConfigService>>,
    user: bool,
}

impl ConfigListCommand {
    pub fn new(config_svc: Rc<RefCell<ConfigService>>, user: bool) -> Self {
        Self {
            config_svc: config_svc,
            user: user,
        }
    }
}

impl Command for ConfigListCommand {
    fn needs_workspace(&self) -> bool {
        false
    }

    fn run(&mut self) -> Result<(), Error> {
        let result = self.config_svc.borrow().list(if self.user {
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
    config_svc: Rc<RefCell<ConfigService>>,
    user: bool,
    key: String,
}

impl ConfigGetCommand {
    pub fn new<S>(config_svc: Rc<RefCell<ConfigService>>, user: bool, key: S) -> Self
    where
        S: AsRef<str>,
    {
        Self {
            config_svc: config_svc,
            user: user,
            key: key.as_ref().to_owned(),
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

        if let Some(value) = self.config_svc.borrow().get(&self.key, scope) {
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
    config_svc: Rc<RefCell<ConfigService>>,
    user: bool,
    key: String,
    value: Option<String>,
}

impl ConfigSetCommand {
    pub fn new<S1, S2>(
        config_svc: Rc<RefCell<ConfigService>>,
        user: bool,
        key: S1,
        value: Option<S2>,
    ) -> Self
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
    {
        Self {
            config_svc: config_svc,
            user: user,
            key: key.as_ref().to_owned(),
            value: value.map(|v| v.as_ref().to_owned()),
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
            self.config_svc.borrow_mut().set(&self.key, &value, scope)?;

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
            self.config_svc.borrow_mut().unset(&self.key, scope)?;

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
