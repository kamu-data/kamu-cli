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
use crate::{ContextListCommand, OutputConfig, WorkspaceService, odf_server};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Command)]
pub struct ContextAddCommand {
    login_service: Arc<odf_server::LoginService>,
    login_flow_service: Arc<odf_server::LoginFlowService>,
    resource_context_registry_service: Arc<ResourceContextRegistryService>,
    resource_context_test_service: Arc<ResourceContextTestService>,
    workspace_service: Arc<WorkspaceService>,
    output_config: Arc<OutputConfig>,

    #[dill::component(explicit)]
    name: String,

    #[dill::component(explicit)]
    server_url: Url,

    #[dill::component(explicit)]
    scope: ResourceContextStoreScope,

    #[dill::component(explicit)]
    login_method: Option<odf_server::LoginMethod>,

    #[dill::component(explicit)]
    no_login: bool,

    #[dill::component(explicit)]
    repo_name: Option<odf::RepoName>,

    #[dill::component(explicit)]
    skip_add_repo: bool,

    #[dill::component(explicit)]
    predefined_odf_backend_url: Option<Url>,
}

impl ContextAddCommand {
    /// Whether to fall back to an interactive login when no credentials were
    /// given. Gated on `is_tty` alone: `quiet` and `verbosity_level` control
    /// whether the login URL is printed, not whether the flow runs.
    pub fn should_auto_login(no_login: bool, is_tty: bool, has_predefined_backend: bool) -> bool {
        !no_login && (is_tty || has_predefined_backend)
    }

    /// Only the interactive flow wants the URL as typed — it discovers the
    /// backend itself. The others are keyed by the URL they are given, and the
    /// context looks its token up by the resolved backend URL.
    pub fn login_server_url(
        login_method: &odf_server::LoginMethod,
        typed_url: &Url,
        resolved_backend_url: &Url,
    ) -> Url {
        match login_method {
            odf_server::LoginMethod::Interactive { .. } => typed_url.clone(),
            odf_server::LoginMethod::ExistingToken { .. }
            | odf_server::LoginMethod::OAuth { .. }
            | odf_server::LoginMethod::Password { .. } => resolved_backend_url.clone(),
        }
    }

    /// Explicit credentials win; otherwise an interactive login only when the
    /// session can actually drive a browser.
    fn resolve_login_method(&self) -> Option<odf_server::LoginMethod> {
        if self.no_login {
            return None;
        }

        if let Some(login_method) = &self.login_method {
            return Some(login_method.clone());
        }

        if Self::should_auto_login(
            self.no_login,
            self.output_config.is_tty,
            self.predefined_odf_backend_url.is_some(),
        ) {
            return Some(odf_server::LoginMethod::Interactive {
                predefined_backend_url: self.predefined_odf_backend_url.clone(),
            });
        }

        None
    }

    async fn login(
        &self,
        login_method: odf_server::LoginMethod,
        backend_url: &Url,
    ) -> Result<(), CLIError> {
        // An existing token is never validated, so a "Login successful" here
        // could be contradicted a line later by the context test
        let report_progress =
            !matches!(login_method, odf_server::LoginMethod::ExistingToken { .. });

        let server_url = Self::login_server_url(&login_method, &self.server_url, backend_url);

        self.login_flow_service
            .ensure_logged_in_for_context(
                odf_server::LoginFlowOptions {
                    server_url,
                    scope: self.scope.into(),
                    method: login_method,
                    add_repo: !self.skip_add_repo,
                    repo_name: self.repo_name.clone(),
                    report_progress,
                },
                backend_url,
            )
            .await?;

        Ok(())
    }
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
        let backend_url = match self
            .login_service
            .resolve_odf_server_backend_url(&self.server_url)
            .await
        {
            Ok(backend_url) => backend_url,
            Err(e) => {
                tracing::warn!(
                    error = ?e,
                    url = %self.server_url,
                    "Could not resolve the ODF server backend URL, using the given URL as-is",
                );
                self.server_url.clone()
            }
        };

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

        // Registered first: that is the user's primary intent and is purely
        // local, so a failed login leaves it in place to retry against
        if let Some(login_method) = self.resolve_login_method()
            && let Err(e) = self.login(login_method, &backend_url).await
        {
            eprintln!(
                "{} Context '{}' was registered, but login did not complete",
                console::style("Warning:").yellow().bold(),
                self.name,
            );
            return Err(e);
        }

        // Runs after the login so the cached status reflects the fresh token
        let test_result = self
            .resource_context_test_service
            .test_remote_context_and_persist(self.scope, &self.name, &backend_url)
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
