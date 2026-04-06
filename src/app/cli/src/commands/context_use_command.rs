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
use crate::WorkspaceService;
use crate::resource_context::{
    CurrentContextStateService,
    ResourceContextResolver,
    ResourceContextStoreScope,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Command)]
pub struct ContextUseCommand {
    current_context_state_service: Arc<CurrentContextStateService>,
    resource_context_resolver: Arc<ResourceContextResolver>,
    workspace_service: Arc<WorkspaceService>,

    #[dill::component(explicit)]
    name: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait(?Send)]
impl Command for ContextUseCommand {
    async fn run(&self) -> Result<(), CLIError> {
        self.resource_context_resolver
            .resolve_context_name(&self.name)?;

        let scope = if self.workspace_service.is_in_workspace() {
            ResourceContextStoreScope::Workspace
        } else {
            ResourceContextStoreScope::User
        };

        self.current_context_state_service
            .set_current_context(scope, Some(self.name.clone()))
            .map_err(CLIError::critical)?;

        eprintln!(
            "{} {}",
            console::style("Current context:").green().bold(),
            self.name,
        );

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
