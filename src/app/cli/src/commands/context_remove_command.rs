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
    name: String,

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
        if self.name == LOCAL_CONTEXT_NAME {
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
        let exists = self
            .resource_context_registry_service
            .get_context_in_scope(self.scope, &self.name)
            .is_some();

        if !exists {
            return Err(CLIError::usage_error(format!(
                "Resource context '{}' not found in {} scope",
                self.name,
                ContextListCommand::scope_label(self.scope).to_lowercase(),
            )));
        }

        self.interact.require_confirmation(format!(
            "{}\n  {}\n{}",
            console::style("You are about to delete following context(s):").yellow(),
            self.name,
            console::style("This operation is irreversible!").yellow(),
        ))?;

        let removed = self
            .resource_context_registry_service
            .remove_context(self.scope, &self.name)
            .map_err(CLIError::critical)?;

        assert!(removed);

        let was_current = self
            .current_context_state_service
            .get_current_context_in_scope(self.scope)
            .as_deref()
            == Some(self.name.as_str());

        if was_current {
            self.current_context_state_service
                .set_current_context(self.scope, None)
                .map_err(CLIError::critical)?;
        }

        eprintln!(
            "{} {} {} {}",
            console::style("Removed").green().bold(),
            self.name,
            console::style("from").green().bold(),
            ContextListCommand::scope_label(self.scope).to_lowercase(),
        );

        if was_current {
            self.report_current_context_switched();
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
