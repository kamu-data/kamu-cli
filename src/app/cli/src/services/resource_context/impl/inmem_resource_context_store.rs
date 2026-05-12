// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Mutex;

use internal_error::InternalError;

use crate::resource_context;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryResourceContextStore {
    workspace_registry: Mutex<resource_context::ResourceContextRegistry>,
    user_registry: Mutex<resource_context::ResourceContextRegistry>,
    workspace_runtime: Mutex<resource_context::ResourceContextsRuntimeState>,
    user_runtime: Mutex<resource_context::ResourceContextsRuntimeState>,
}

#[dill::component(pub)]
#[dill::interface(dyn resource_context::ResourceContextStore)]
#[dill::scope(dill::Singleton)]
impl InMemoryResourceContextStore {
    pub fn new() -> Self {
        Self {
            workspace_registry: Mutex::new(Vec::new()),
            user_registry: Mutex::new(Vec::new()),
            workspace_runtime: Mutex::new(resource_context::ResourceContextsRuntimeState::default()),
            user_runtime: Mutex::new(resource_context::ResourceContextsRuntimeState::default()),
        }
    }

    fn registry_for(
        &self,
        scope: resource_context::ResourceContextStoreScope,
    ) -> &Mutex<resource_context::ResourceContextRegistry> {
        match scope {
            resource_context::ResourceContextStoreScope::Workspace => &self.workspace_registry,
            resource_context::ResourceContextStoreScope::User => &self.user_registry,
        }
    }

    fn runtime_for(
        &self,
        scope: resource_context::ResourceContextStoreScope,
    ) -> &Mutex<resource_context::ResourceContextsRuntimeState> {
        match scope {
            resource_context::ResourceContextStoreScope::Workspace => &self.workspace_runtime,
            resource_context::ResourceContextStoreScope::User => &self.user_runtime,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl resource_context::ResourceContextStore for InMemoryResourceContextStore {
    fn read_context_registry(
        &self,
        scope: resource_context::ResourceContextStoreScope,
    ) -> Result<resource_context::ResourceContextRegistry, InternalError> {
        Ok(self.registry_for(scope).lock().unwrap().clone())
    }

    fn write_context_registry(
        &self,
        scope: resource_context::ResourceContextStoreScope,
        registry: &resource_context::ResourceContextRegistry,
    ) -> Result<(), InternalError> {
        self.registry_for(scope)
            .lock()
            .unwrap()
            .clone_from(registry);
        Ok(())
    }

    fn read_account_runtime_state(
        &self,
        scope: resource_context::ResourceContextStoreScope,
        account_name: &odf::AccountName,
    ) -> Result<resource_context::AccountContextRuntimeState, InternalError> {
        let key: &str = account_name.as_ref();
        Ok(self
            .runtime_for(scope)
            .lock()
            .unwrap()
            .accounts
            .get(key)
            .cloned()
            .unwrap_or_default())
    }

    fn write_account_runtime_state(
        &self,
        scope: resource_context::ResourceContextStoreScope,
        account_name: &odf::AccountName,
        state: &resource_context::AccountContextRuntimeState,
    ) -> Result<(), InternalError> {
        let key: &str = account_name.as_ref();
        self.runtime_for(scope)
            .lock()
            .unwrap()
            .accounts
            .insert(key.to_owned(), state.clone());
        Ok(())
    }

    fn read_current_context_state(
        &self,
        scope: resource_context::ResourceContextStoreScope,
        account_name: &odf::AccountName,
    ) -> Result<resource_context::CurrentResourceContextState, InternalError> {
        self.read_account_runtime_state(scope, account_name)
            .map(|s| resource_context::CurrentResourceContextState {
                current_context_name: s.current_context_name,
            })
    }

    fn write_current_context_state(
        &self,
        scope: resource_context::ResourceContextStoreScope,
        account_name: &odf::AccountName,
        state: &resource_context::CurrentResourceContextState,
    ) -> Result<(), InternalError> {
        let mut account_state = self.read_account_runtime_state(scope, account_name)?;
        account_state
            .current_context_name
            .clone_from(&state.current_context_name);
        self.write_account_runtime_state(scope, account_name, &account_state)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
