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
use crate::resource_context::{
    CurrentContextStateService,
    LOCAL_CONTEXT_NAME,
    ResourceContextRegistryService,
    ResourceContextStoreScope,
};
use crate::{ContextListCommand, Interact, WorkspaceService};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Command)]
pub struct ContextRemoveCommand {
    resource_context_registry_service: Arc<ResourceContextRegistryService>,
    current_context_state_service: Arc<CurrentContextStateService>,
    interact: Arc<Interact>,
    workspace_service: Arc<WorkspaceService>,

    #[dill::component(explicit)]
    name: Option<String>,

    #[dill::component(explicit)]
    all: bool,

    #[dill::component(explicit)]
    scope: ResourceContextStoreScope,
}

impl ContextRemoveCommand {
    fn report_current_context_switched(&self) {
        match self.current_context_state_service.get_current_context() {
            Some(name) => {
                if let Some(scoped_context) = self
                    .resource_context_registry_service
                    .get_context_with_scope(&name)
                {
                    eprintln!(
                        "{} {} ({})",
                        console::style("Current context switched to").green().bold(),
                        name,
                        ContextListCommand::scope_label(scoped_context.scope).to_lowercase(),
                    );
                } else if self.workspace_service.is_in_workspace() {
                    eprintln!(
                        "{} {}",
                        console::style("Current context switched to").green().bold(),
                        LOCAL_CONTEXT_NAME,
                    );
                } else {
                    eprintln!(
                        "{}",
                        console::style("Current context selection was cleared")
                            .yellow()
                            .bold(),
                    );
                }
            }
            None if self.workspace_service.is_in_workspace() => {
                eprintln!(
                    "{} {}",
                    console::style("Current context switched to").green().bold(),
                    LOCAL_CONTEXT_NAME,
                );
            }
            None => {
                eprintln!(
                    "{}",
                    console::style("Current context selection was cleared")
                        .yellow()
                        .bold(),
                );
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait(?Send)]
impl Command for ContextRemoveCommand {
    async fn validate_args(&self) -> Result<(), CLIError> {
        match (&self.name, self.all) {
            (None, false) => {
                return Err(CLIError::usage_error("Specify context name or pass --all"));
            }
            (Some(_), true) => {
                return Err(CLIError::usage_error(
                    "You can either specify context name or pass --all",
                ));
            }
            _ => {}
        }

        if self.name.as_deref() == Some(LOCAL_CONTEXT_NAME) {
            return Err(CLIError::usage_error("Context name 'local' is reserved"));
        }

        if self.scope == ResourceContextStoreScope::Workspace
            && !self.workspace_service.is_in_workspace()
        {
            return Err(CLIError::usage_error_from(crate::NotInWorkspace));
        }

        Ok(())
    }

    async fn run(&self) -> Result<(), CLIError> {
        let names = self.context_names_to_remove()?;
        self.confirm_removal(&names)?;

        let current_context_name = self
            .current_context_state_service
            .get_current_context_in_scope(self.scope);
        let was_current = current_context_name
            .as_deref()
            .is_some_and(|current| names.iter().any(|name| name == current));

        if self.all {
            let removed = self
                .resource_context_registry_service
                .remove_all_contexts_in_scope(self.scope)
                .map_err(CLIError::critical)?;
            assert_eq!(removed, names.len());
        } else {
            let removed = self
                .resource_context_registry_service
                .remove_context(self.scope, names.first().unwrap())
                .map_err(CLIError::critical)?;
            assert!(removed);
        }

        if was_current {
            self.current_context_state_service
                .set_current_context(self.scope, None)
                .map_err(CLIError::critical)?;
        }

        self.report_removed(&names);

        if was_current {
            self.report_current_context_switched();
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ContextRemoveCommand {
    fn context_names_to_remove(&self) -> Result<Vec<String>, CLIError> {
        if self.all {
            let mut names: Vec<_> = self
                .resource_context_registry_service
                .list_contexts_in_scope(self.scope)
                .into_iter()
                .map(|context| context.name)
                .collect();
            names.sort();

            if names.is_empty() {
                return Err(CLIError::usage_error(format!(
                    "No resource contexts found in {} scope",
                    ContextListCommand::scope_label(self.scope).to_lowercase(),
                )));
            }

            Ok(names)
        } else {
            let name = self.name.as_ref().unwrap();

            if self
                .resource_context_registry_service
                .get_context_in_scope(self.scope, name)
                .is_none()
            {
                return Err(CLIError::usage_error(format!(
                    "Resource context '{}' not found in {} scope",
                    name,
                    ContextListCommand::scope_label(self.scope).to_lowercase(),
                )));
            }

            Ok(vec![name.clone()])
        }
    }

    fn confirm_removal(&self, names: &[String]) -> Result<(), CLIError> {
        let listed_contexts = names
            .iter()
            .map(|name| format!("  {name}"))
            .collect::<Vec<_>>()
            .join("\n");

        self.interact.require_confirmation(format!(
            "{}\n{}\n{}",
            console::style("You are about to delete following context(s):").yellow(),
            listed_contexts,
            console::style("This operation is irreversible!").yellow(),
        ))
    }

    fn report_removed(&self, names: &[String]) {
        if self.all {
            eprintln!(
                "{} {} {} {}",
                console::style("Removed").green().bold(),
                names.len(),
                console::style("contexts from").green().bold(),
                ContextListCommand::scope_label(self.scope).to_lowercase(),
            );
        } else {
            eprintln!(
                "{} {} {} {}",
                console::style("Removed").green().bold(),
                names.first().unwrap(),
                console::style("from").green().bold(),
                ContextListCommand::scope_label(self.scope).to_lowercase(),
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
