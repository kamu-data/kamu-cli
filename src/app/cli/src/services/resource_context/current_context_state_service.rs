// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, Mutex};

use internal_error::InternalError;

use crate::resource_context;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct CurrentContextStateService {
    store: Arc<dyn resource_context::ResourceContextStore>,
    workspace_state: Mutex<resource_context::CurrentResourceContextState>,
    user_state: Mutex<resource_context::CurrentResourceContextState>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
impl CurrentContextStateService {
    pub fn new(store: Arc<dyn resource_context::ResourceContextStore>) -> Self {
        let workspace_state = store
            .read_current_context_state(resource_context::ResourceContextStoreScope::Workspace)
            .unwrap();
        let user_state = store
            .read_current_context_state(resource_context::ResourceContextStoreScope::User)
            .unwrap();

        Self {
            store,
            workspace_state: Mutex::new(workspace_state),
            user_state: Mutex::new(user_state),
        }
    }

    pub fn get_current_context(&self) -> Option<String> {
        self.get_current_context_in_scope(resource_context::ResourceContextStoreScope::Workspace)
            .or_else(|| {
                self.get_current_context_in_scope(resource_context::ResourceContextStoreScope::User)
            })
    }

    pub fn get_current_context_in_scope(
        &self,
        scope: resource_context::ResourceContextStoreScope,
    ) -> Option<String> {
        self.state_for_scope(scope)
            .lock()
            .expect("Could not lock current resource context state")
            .current_context_name
            .clone()
    }

    pub fn set_current_context(
        &self,
        scope: resource_context::ResourceContextStoreScope,
        context_name: Option<String>,
    ) -> Result<(), InternalError> {
        let mut state = self
            .state_for_scope(scope)
            .lock()
            .expect("Could not lock current resource context state");
        state.current_context_name = context_name;
        self.store.write_current_context_state(scope, &state)
    }

    fn state_for_scope(
        &self,
        scope: resource_context::ResourceContextStoreScope,
    ) -> &Mutex<resource_context::CurrentResourceContextState> {
        match scope {
            resource_context::ResourceContextStoreScope::Workspace => &self.workspace_state,
            resource_context::ResourceContextStoreScope::User => &self.user_state,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
