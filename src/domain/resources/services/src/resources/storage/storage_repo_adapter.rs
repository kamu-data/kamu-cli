// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use event_sourcing::{LoadError, SaveError};

use crate::domain::{
    ReconcilableResourceRepository,
    StorageEventStore,
    StorageID,
    StorageResource,
    StorageState,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn ReconcilableResourceRepository<StorageResource>)]
pub struct StorageResourceRepositoryAdapter {
    event_store: Arc<dyn StorageEventStore>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ReconcilableResourceRepository<StorageResource> for StorageResourceRepositoryAdapter {
    async fn load(&self, id: &StorageID) -> Result<StorageResource, LoadError<StorageState>> {
        StorageResource::load(id, self.event_store.as_ref()).await
    }

    async fn save(&self, resource: &mut StorageResource) -> Result<(), SaveError> {
        resource.save(self.event_store.as_ref()).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
