// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use crate::{CLIError, WorkspaceService, resource_context};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ResourceContextResolver {
    workspace_service: Arc<WorkspaceService>,
    current_context_state_service: Arc<resource_context::CurrentContextStateService>,
    resource_context_registry_service: Arc<resource_context::ResourceContextRegistryService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
impl ResourceContextResolver {
    pub fn new(
        workspace_service: Arc<WorkspaceService>,
        current_context_state_service: Arc<resource_context::CurrentContextStateService>,
        resource_context_registry_service: Arc<resource_context::ResourceContextRegistryService>,
    ) -> Self {
        Self {
            workspace_service,
            current_context_state_service,
            resource_context_registry_service,
        }
    }

    pub fn resolve(
        &self,
        explicit_context_name: Option<&str>,
    ) -> Result<resource_context::ResolvedResourceContext, CLIError> {
        if let Some(context_name) = explicit_context_name {
            return self.resolve_context_name(context_name);
        }

        if let Some(current_context_name) = self.current_context_state_service.get_current_context()
            && let Ok(context) = self.resolve_context_name(&current_context_name)
        {
            return Ok(context);
        }

        if self.workspace_service.is_in_workspace() {
            return Ok(resource_context::ResolvedResourceContext::LocalWorkspace);
        }

        Err(CLIError::usage_error(
            "No resource context selected and no local workspace available",
        ))
    }

    pub fn resolve_context_name(
        &self,
        context_name: &str,
    ) -> Result<resource_context::ResolvedResourceContext, CLIError> {
        if context_name == resource_context::LOCAL_CONTEXT_NAME {
            if self.workspace_service.is_in_workspace() {
                return Ok(resource_context::ResolvedResourceContext::LocalWorkspace);
            }

            return Err(CLIError::usage_error(
                "Context 'local' is only available inside a workspace",
            ));
        }

        let Some(context) = self
            .resource_context_registry_service
            .get_context(context_name)
        else {
            return Err(CLIError::usage_error(format!(
                "Resource context '{context_name}' not found",
            )));
        };

        Ok(resource_context::ResolvedResourceContext::RemoteWorkspace {
            name: context.name,
            backend_url: context.backend_url,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
