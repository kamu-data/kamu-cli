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
    ResolvedResourceContext,
    ResourceContextResolver,
    ResourceContextStoreScope,
    ResourceContextTestService,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Command)]
pub struct ContextUseCommand {
    current_context_state_service: Arc<CurrentContextStateService>,
    resource_context_resolver: Arc<ResourceContextResolver>,
    resource_context_test_service: Arc<ResourceContextTestService>,
    workspace_service: Arc<WorkspaceService>,

    #[dill::component(explicit)]
    name: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait(?Send)]
impl Command for ContextUseCommand {
    async fn run(&self) -> Result<(), CLIError> {
        let resolved_context = self
            .resource_context_resolver
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

        if let ResolvedResourceContext::RemoteWorkspace { name, backend_url } = resolved_context {
            let test_result = self
                .resource_context_test_service
                .test_remote_context(&name, &backend_url)
                .await?;

            if let Some(warning_message) = test_result.warning_message() {
                eprintln!(
                    "{} {}",
                    console::style("Warning:").yellow().bold(),
                    warning_message,
                );
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
