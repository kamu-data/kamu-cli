// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Mutex;

use dill::*;
use internal_error::InternalError;
use kamu_resources::{ResourceID, ResourceLabelProjectionRepository};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub struct InMemoryResourceLabelProjectionRepository {
    entries_by_resource_id: Mutex<HashMap<ResourceID, Vec<(String, String)>>>,
}

#[component(pub)]
#[interface(dyn ResourceLabelProjectionRepository)]
#[scope(Singleton)]
impl InMemoryResourceLabelProjectionRepository {
    pub fn new() -> Self {
        Self::default()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ResourceLabelProjectionRepository for InMemoryResourceLabelProjectionRepository {
    async fn replace_entries(
        &self,
        resource_id: &ResourceID,
        entries: &[(String, String)],
    ) -> Result<(), InternalError> {
        let mut guard = self.entries_by_resource_id.lock().unwrap();

        if entries.is_empty() {
            guard.remove(resource_id);
        } else {
            let mut sorted_entries = entries.to_vec();
            sorted_entries.sort();
            guard.insert(*resource_id, sorted_entries);
        }

        Ok(())
    }

    async fn find_entries(
        &self,
        resource_id: &ResourceID,
    ) -> Result<Vec<(String, String)>, InternalError> {
        let guard = self.entries_by_resource_id.lock().unwrap();

        Ok(guard.get(resource_id).cloned().unwrap_or_default())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
