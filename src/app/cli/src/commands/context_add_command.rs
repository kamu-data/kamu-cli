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

use super::{CLIError, Command};
use crate::resource_context::{
    LOCAL_CONTEXT_NAME,
    ResourceContextRecord,
    ResourceContextRegistryService,
    ResourceContextStoreScope,
    ResourceContextTestService,
};
use crate::{ContextListCommand, WorkspaceService};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Command)]
pub struct ContextAddCommand {
    login_service: Arc<crate::odf_server::LoginService>,
    resource_context_registry_service: Arc<ResourceContextRegistryService>,
    resource_context_test_service: Arc<ResourceContextTestService>,
    workspace_service: Arc<WorkspaceService>,

    #[dill::component(explicit)]
    name: String,

    #[dill::component(explicit)]
    server_url: Url,

    #[dill::component(explicit)]
    scope: ResourceContextStoreScope,
}

#[async_trait::async_trait(?Send)]
impl Command for ContextAddCommand {
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
        let backend_url = self
            .login_service
            .resolve_odf_server_backend_url(&self.server_url)
            .await
            .unwrap_or_else(|_| self.server_url.clone());

        let existed = self
            .resource_context_registry_service
            .get_context_in_scope(self.scope, &self.name)
            .is_some();

        self.resource_context_registry_service
            .upsert_context(
                self.scope,
                ResourceContextRecord::new(self.name.clone(), backend_url.clone()),
            )
            .map_err(CLIError::critical)?;

        eprintln!(
            "{} {} {} {}",
            console::style(if existed { "Updated" } else { "Added" })
                .green()
                .bold(),
            self.name,
            console::style("in").green().bold(),
            ContextListCommand::scope_label(self.scope).to_lowercase(),
        );

        let test_result = self
            .resource_context_test_service
            .test_remote_context(&self.name, &backend_url)
            .await?;

        if let Some(warning_message) = test_result.warning_message() {
            eprintln!(
                "{} {}",
                console::style("Warning:").yellow().bold(),
                warning_message,
            );
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
