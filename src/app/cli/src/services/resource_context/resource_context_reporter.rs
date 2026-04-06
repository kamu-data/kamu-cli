// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use crate::resource_context::{
    LOCAL_CONTEXT_NAME,
    ResolvedResourceContext,
    ResourceContextRegistryService,
    ResourceContextStoreScope,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
pub struct ResourceContextReporter {
    resource_context_registry_service: Arc<ResourceContextRegistryService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceContextReporter {
    pub fn describe(&self, resolved_context: &ResolvedResourceContext) -> (String, String, String) {
        match resolved_context {
            ResolvedResourceContext::LocalWorkspace => (
                LOCAL_CONTEXT_NAME.to_string(),
                "Local".to_string(),
                Self::scope_label(ResourceContextStoreScope::Workspace).to_string(),
            ),
            ResolvedResourceContext::RemoteWorkspace { name, .. } => {
                let scoped_context = self
                    .resource_context_registry_service
                    .get_context_with_scope(name)
                    .expect("Resolved remote resource context must exist in registry");

                (
                    name.clone(),
                    "Remote".to_string(),
                    Self::scope_label(scoped_context.scope).to_string(),
                )
            }
        }
    }

    pub fn report_usage(&self, action: &str, resolved_context: &ResolvedResourceContext) {
        let (name, kind, scope) = self.describe(resolved_context);

        eprintln!(
            "{} {} ({kind}, {scope})",
            console::style(action).dim(),
            console::style(name).bold(),
        );
    }

    fn scope_label(scope: ResourceContextStoreScope) -> &'static str {
        match scope {
            ResourceContextStoreScope::Workspace => "Workspace",
            ResourceContextStoreScope::User => "User",
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
