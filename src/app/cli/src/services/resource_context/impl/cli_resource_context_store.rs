// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;

use internal_error::{InternalError, ResultIntoInternal};
use odf::metadata::serde::yaml::legacy::Manifest;

use crate::{NotInWorkspace, WorkspaceLayout, resource_context};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const KAMU_CONTEXTS_STORE: &str = ".kamucontexts";
const KAMU_CONTEXTS_STATE_STORE: &str = ".kamucontexts.state";
const KAMU_CONTEXTS_STORE_VERSION: i32 = 1;
const KAMU_CONTEXTS_STORE_MANIFEST_KIND: &str = "KamuResourceContexts";
const KAMU_CONTEXTS_STATE_STORE_VERSION: i32 = 1;
const KAMU_CONTEXTS_STATE_STORE_MANIFEST_KIND: &str = "KamuResourceContextsState";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct CLIResourceContextStore {
    user_contexts_path: PathBuf,
    user_contexts_state_path: PathBuf,
    workspace_contexts_path: PathBuf,
    workspace_contexts_state_path: PathBuf,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn resource_context::ResourceContextStore)]
impl CLIResourceContextStore {
    pub fn new(workspace_layout: &WorkspaceLayout) -> Self {
        let home_dir = dirs::home_dir().expect("Cannot determine user home directory");

        Self {
            user_contexts_path: home_dir.join(KAMU_CONTEXTS_STORE),
            user_contexts_state_path: home_dir.join(KAMU_CONTEXTS_STATE_STORE),
            workspace_contexts_path: workspace_layout.root_dir.join(KAMU_CONTEXTS_STORE),
            workspace_contexts_state_path: workspace_layout
                .root_dir
                .join(KAMU_CONTEXTS_STATE_STORE),
        }
    }

    fn registry_store_path_for_scope(
        &self,
        scope: resource_context::ResourceContextStoreScope,
    ) -> &PathBuf {
        match scope {
            resource_context::ResourceContextStoreScope::Workspace => &self.workspace_contexts_path,
            resource_context::ResourceContextStoreScope::User => &self.user_contexts_path,
        }
    }

    fn runtime_state_store_path_for_scope(
        &self,
        scope: resource_context::ResourceContextStoreScope,
    ) -> &PathBuf {
        match scope {
            resource_context::ResourceContextStoreScope::Workspace => {
                &self.workspace_contexts_state_path
            }
            resource_context::ResourceContextStoreScope::User => &self.user_contexts_state_path,
        }
    }

    fn ensure_scope_writable(
        &self,
        scope: resource_context::ResourceContextStoreScope,
    ) -> Result<(), InternalError> {
        if scope == resource_context::ResourceContextStoreScope::Workspace
            && !self.workspace_contexts_path.parent().unwrap().exists()
        {
            return Err(InternalError::new(NotInWorkspace));
        }

        Ok(())
    }

    fn read_config_state(
        &self,
        scope: resource_context::ResourceContextStoreScope,
    ) -> Result<resource_context::ResourceContextsState, InternalError> {
        let store_path = self.registry_store_path_for_scope(scope);
        if !store_path.exists() {
            return Ok(resource_context::ResourceContextsState::default());
        }

        let file = std::fs::OpenOptions::new()
            .read(true)
            .open(store_path)
            .int_err()?;

        let manifest: Manifest<resource_context::ResourceContextsState> =
            serde_yaml::from_reader(file).int_err()?;

        if manifest.kind != KAMU_CONTEXTS_STORE_MANIFEST_KIND {
            return InternalError::bail(format!(
                "Unexpected contexts manifest kind '{}', expected '{}'",
                manifest.kind, KAMU_CONTEXTS_STORE_MANIFEST_KIND
            ));
        }
        if manifest.version != KAMU_CONTEXTS_STORE_VERSION {
            return InternalError::bail(format!(
                "Unsupported contexts manifest version {}, expected {}",
                manifest.version, KAMU_CONTEXTS_STORE_VERSION
            ));
        }

        Ok(manifest.content)
    }

    fn write_config_state(
        &self,
        scope: resource_context::ResourceContextStoreScope,
        state: &resource_context::ResourceContextsState,
    ) -> Result<(), InternalError> {
        self.ensure_scope_writable(scope)?;

        let store_path = self.registry_store_path_for_scope(scope);
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(store_path)
            .int_err()?;

        let manifest = Manifest {
            kind: KAMU_CONTEXTS_STORE_MANIFEST_KIND.to_owned(),
            version: KAMU_CONTEXTS_STORE_VERSION,
            content: state,
        };

        serde_yaml::to_writer(file, &manifest).int_err()
    }

    fn read_runtime_state(
        &self,
        scope: resource_context::ResourceContextStoreScope,
    ) -> Result<resource_context::ResourceContextsRuntimeState, InternalError> {
        let store_path = self.runtime_state_store_path_for_scope(scope);
        if !store_path.exists() {
            return Ok(resource_context::ResourceContextsRuntimeState::default());
        }

        let file = std::fs::OpenOptions::new()
            .read(true)
            .open(store_path)
            .int_err()?;

        let manifest: Manifest<resource_context::ResourceContextsRuntimeState> =
            serde_yaml::from_reader(file).int_err()?;

        if manifest.kind != KAMU_CONTEXTS_STATE_STORE_MANIFEST_KIND {
            return InternalError::bail(format!(
                "Unexpected contexts state manifest kind '{}', expected '{}'",
                manifest.kind, KAMU_CONTEXTS_STATE_STORE_MANIFEST_KIND
            ));
        }
        if manifest.version != KAMU_CONTEXTS_STATE_STORE_VERSION {
            return InternalError::bail(format!(
                "Unsupported contexts state manifest version {}, expected {}",
                manifest.version, KAMU_CONTEXTS_STATE_STORE_VERSION
            ));
        }

        Ok(manifest.content)
    }

    fn write_runtime_state(
        &self,
        scope: resource_context::ResourceContextStoreScope,
        state: &resource_context::ResourceContextsRuntimeState,
    ) -> Result<(), InternalError> {
        self.ensure_scope_writable(scope)?;

        let store_path = self.runtime_state_store_path_for_scope(scope);
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(store_path)
            .int_err()?;

        let manifest = Manifest {
            kind: KAMU_CONTEXTS_STATE_STORE_MANIFEST_KIND.to_owned(),
            version: KAMU_CONTEXTS_STATE_STORE_VERSION,
            content: state,
        };

        serde_yaml::to_writer(file, &manifest).int_err()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl resource_context::ResourceContextStore for CLIResourceContextStore {
    fn read_context_registry(
        &self,
        scope: resource_context::ResourceContextStoreScope,
    ) -> Result<resource_context::ResourceContextRegistry, InternalError> {
        self.read_config_state(scope).map(|state| state.contexts)
    }

    fn write_context_registry(
        &self,
        scope: resource_context::ResourceContextStoreScope,
        registry: &resource_context::ResourceContextRegistry,
    ) -> Result<(), InternalError> {
        let mut state = self.read_config_state(scope)?;
        state.contexts.clone_from(registry);
        self.write_config_state(scope, &state)
    }

    fn read_account_runtime_state(
        &self,
        scope: resource_context::ResourceContextStoreScope,
        account_name: &odf::AccountName,
    ) -> Result<resource_context::AccountContextRuntimeState, InternalError> {
        let key: &str = account_name.as_ref();
        self.read_runtime_state(scope)
            .map(|state| state.accounts.get(key).cloned().unwrap_or_default())
    }

    fn write_account_runtime_state(
        &self,
        scope: resource_context::ResourceContextStoreScope,
        account_name: &odf::AccountName,
        account_state: &resource_context::AccountContextRuntimeState,
    ) -> Result<(), InternalError> {
        let key: &str = account_name.as_ref();
        let mut state = self.read_runtime_state(scope)?;
        state.accounts.insert(key.to_owned(), account_state.clone());
        self.write_runtime_state(scope, &state)
    }

    fn read_current_context_state(
        &self,
        scope: resource_context::ResourceContextStoreScope,
        account_name: &odf::AccountName,
    ) -> Result<resource_context::CurrentResourceContextState, InternalError> {
        self.read_account_runtime_state(scope, account_name)
            .map(
                |account_state| resource_context::CurrentResourceContextState {
                    current_context_name: account_state.current_context_name,
                },
            )
    }

    fn write_current_context_state(
        &self,
        scope: resource_context::ResourceContextStoreScope,
        account_name: &odf::AccountName,
        state: &resource_context::CurrentResourceContextState,
    ) -> Result<(), InternalError> {
        let key: &str = account_name.as_ref();
        let mut runtime_state = self.read_runtime_state(scope)?;
        runtime_state
            .accounts
            .entry(key.to_owned())
            .or_default()
            .current_context_name
            .clone_from(&state.current_context_name);
        self.write_runtime_state(scope, &runtime_state)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
