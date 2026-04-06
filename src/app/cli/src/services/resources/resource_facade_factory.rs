// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_accounts::CurrentAccountSubject;
use kamu_resources_facade::{RemoteGraphqlResourceFacadeImpl, ResourceFacade};

use crate::CLIError;
use crate::odf_server::AccessTokenRegistryService;
use crate::resource_context::{ResolvedResourceContext, ResourceContextResolver};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
pub struct ResourceFacadeFactory {
    local_resource_facade: Arc<dyn ResourceFacade>,
    resource_context_resolver: Arc<ResourceContextResolver>,
    access_token_registry_service: Arc<AccessTokenRegistryService>,
    current_account_subject: Arc<CurrentAccountSubject>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceFacadeFactory {
    pub fn get_resource_facade(
        &self,
        explicit_context_name: Option<&str>,
    ) -> Result<Arc<dyn ResourceFacade>, CLIError> {
        match self
            .resource_context_resolver
            .resolve(explicit_context_name)?
        {
            ResolvedResourceContext::LocalWorkspace => Ok(self.local_resource_facade.clone()),
            ResolvedResourceContext::RemoteWorkspace { backend_url, .. } => {
                let maybe_access_token = match self.current_account_subject.as_ref() {
                    CurrentAccountSubject::Logged(_) => self
                        .access_token_registry_service
                        .find_by_backend_url(&backend_url)
                        .map(|report| report.access_token.access_token),
                    CurrentAccountSubject::Anonymous(_) => None,
                };

                Ok(Arc::new(RemoteGraphqlResourceFacadeImpl::new(
                    &backend_url,
                    maybe_access_token,
                )))
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
