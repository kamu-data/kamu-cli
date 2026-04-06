// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use internal_error::InternalError;

use crate::resource_context;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ResourceContextRegistryService {
    store: Arc<dyn resource_context::ResourceContextStore>,
    workspace_registry: Mutex<resource_context::ResourceContextRegistry>,
    user_registry: Mutex<resource_context::ResourceContextRegistry>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
impl ResourceContextRegistryService {
    pub fn new(store: Arc<dyn resource_context::ResourceContextStore>) -> Self {
        let workspace_registry = store
            .read_context_registry(resource_context::ResourceContextStoreScope::Workspace)
            .unwrap();
        let user_registry = store
            .read_context_registry(resource_context::ResourceContextStoreScope::User)
            .unwrap();

        Self {
            store,
            workspace_registry: Mutex::new(workspace_registry),
            user_registry: Mutex::new(user_registry),
        }
    }

    pub fn get_context_in_scope(
        &self,
        scope: resource_context::ResourceContextStoreScope,
        name: &str,
    ) -> Option<resource_context::ResourceContextRecord> {
        let registry = self
            .registry_for_scope(scope)
            .lock()
            .expect("Could not lock resource context registry");

        registry.iter().find(|ctx| ctx.name == name).cloned()
    }

    pub fn get_context(&self, name: &str) -> Option<resource_context::ResourceContextRecord> {
        self.get_context_in_scope(resource_context::ResourceContextStoreScope::Workspace, name)
            .or_else(|| {
                self.get_context_in_scope(resource_context::ResourceContextStoreScope::User, name)
            })
    }

    pub fn get_context_with_scope(
        &self,
        name: &str,
    ) -> Option<resource_context::ScopedResourceContextRecord> {
        self.get_context_in_scope(resource_context::ResourceContextStoreScope::Workspace, name)
            .map(|context| resource_context::ScopedResourceContextRecord {
                scope: resource_context::ResourceContextStoreScope::Workspace,
                context,
            })
            .or_else(|| {
                self.get_context_in_scope(resource_context::ResourceContextStoreScope::User, name)
                    .map(|context| resource_context::ScopedResourceContextRecord {
                        scope: resource_context::ResourceContextStoreScope::User,
                        context,
                    })
            })
    }

    pub fn list_effective_contexts(&self) -> Vec<resource_context::ResourceContextRecord> {
        self.list_effective_contexts_with_scope()
            .into_iter()
            .map(|item| item.context)
            .collect()
    }

    pub fn list_effective_contexts_with_scope(
        &self,
    ) -> Vec<resource_context::ScopedResourceContextRecord> {
        let workspace_registry = self
            .workspace_registry
            .lock()
            .expect("Could not lock resource context registry");
        let user_registry = self
            .user_registry
            .lock()
            .expect("Could not lock resource context registry");

        let mut seen = HashSet::new();
        let mut contexts = Vec::new();

        for (scope, registry) in [
            (
                resource_context::ResourceContextStoreScope::Workspace,
                &*workspace_registry,
            ),
            (
                resource_context::ResourceContextStoreScope::User,
                &*user_registry,
            ),
        ] {
            for ctx in registry {
                if seen.insert(ctx.name.clone()) {
                    contexts.push(resource_context::ScopedResourceContextRecord {
                        scope,
                        context: ctx.clone(),
                    });
                }
            }
        }

        contexts
    }

    pub fn list_contexts_in_scope(
        &self,
        scope: resource_context::ResourceContextStoreScope,
    ) -> Vec<resource_context::ResourceContextRecord> {
        self.lock_registry_for_scope(scope).clone()
    }

    pub fn upsert_context(
        &self,
        scope: resource_context::ResourceContextStoreScope,
        context: resource_context::ResourceContextRecord,
    ) -> Result<(), InternalError> {
        let mut registry = self
            .registry_for_scope(scope)
            .lock()
            .expect("Could not lock resource context registry");

        if let Some(position) = registry
            .iter()
            .position(|existing| existing.name == context.name)
        {
            registry[position] = context;
        } else {
            registry.push(context);
        }

        self.store.write_context_registry(scope, &registry)
    }

    pub fn set_context_last_test_result(
        &self,
        scope: resource_context::ResourceContextStoreScope,
        name: &str,
        last_test_result: resource_context::ResourceContextLastTestResult,
    ) -> Result<bool, InternalError> {
        let mut registry = self.lock_registry_for_scope(scope);

        let position = if let Some(position) =
            registry.iter().position(|existing| existing.name == name)
        {
            position
        } else {
            *registry = self.store.read_context_registry(scope)?;

            let Some(position) = registry.iter().position(|existing| existing.name == name) else {
                return Ok(false);
            };

            position
        };

        registry[position].last_test_result = Some(last_test_result);
        self.store.write_context_registry(scope, &registry)?;

        Ok(true)
    }

    pub fn set_effective_context_last_test_result(
        &self,
        name: &str,
        last_test_result: resource_context::ResourceContextLastTestResult,
    ) -> Result<bool, InternalError> {
        if self.set_context_last_test_result(
            resource_context::ResourceContextStoreScope::Workspace,
            name,
            last_test_result.clone(),
        )? {
            return Ok(true);
        }

        self.set_context_last_test_result(
            resource_context::ResourceContextStoreScope::User,
            name,
            last_test_result,
        )
    }

    pub fn remove_context(
        &self,
        scope: resource_context::ResourceContextStoreScope,
        name: &str,
    ) -> Result<bool, InternalError> {
        let mut registry = self
            .registry_for_scope(scope)
            .lock()
            .expect("Could not lock resource context registry");

        let Some(position) = registry.iter().position(|existing| existing.name == name) else {
            return Ok(false);
        };

        registry.remove(position);
        self.store.write_context_registry(scope, &registry)?;

        Ok(true)
    }

    pub fn remove_all_contexts_in_scope(
        &self,
        scope: resource_context::ResourceContextStoreScope,
    ) -> Result<usize, InternalError> {
        let mut registry = self.lock_registry_for_scope(scope);
        let removed = registry.len();

        if removed == 0 {
            return Ok(0);
        }

        registry.clear();
        self.store.write_context_registry(scope, &registry)?;

        Ok(removed)
    }

    fn registry_for_scope(
        &self,
        scope: resource_context::ResourceContextStoreScope,
    ) -> &Mutex<resource_context::ResourceContextRegistry> {
        match scope {
            resource_context::ResourceContextStoreScope::Workspace => &self.workspace_registry,
            resource_context::ResourceContextStoreScope::User => &self.user_registry,
        }
    }

    fn lock_registry_for_scope(
        &self,
        scope: resource_context::ResourceContextStoreScope,
    ) -> std::sync::MutexGuard<'_, resource_context::ResourceContextRegistry> {
        self.registry_for_scope(scope)
            .lock()
            .expect("Could not lock resource context registry")
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
