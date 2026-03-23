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
use kamu_resources::ResourceID;

use crate::domain::{
    ReconcilableResourceRepository,
    SecretSetEventStore,
    SecretSetResource,
    SecretSetState,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn ReconcilableResourceRepository<SecretSetResource>)]
pub struct SecretSetResourceRepositoryAdapter {
    event_store: Arc<dyn SecretSetEventStore>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ReconcilableResourceRepository<SecretSetResource> for SecretSetResourceRepositoryAdapter {
    async fn load(&self, id: &ResourceID) -> Result<SecretSetResource, LoadError<SecretSetState>> {
        SecretSetResource::load(id, self.event_store.as_ref()).await
    }

    async fn save(&self, resource: &mut SecretSetResource) -> Result<(), SaveError> {
        resource.save(self.event_store.as_ref()).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
