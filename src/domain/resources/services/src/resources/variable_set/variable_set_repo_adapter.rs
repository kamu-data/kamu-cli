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
    VariableSetEventStore,
    VariableSetID,
    VariableSetResource,
    VariableSetState,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn ReconcilableResourceRepository<VariableSetResource>)]
pub struct VariableSetResourceRepositoryAdapter {
    event_store: Arc<dyn VariableSetEventStore>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ReconcilableResourceRepository<VariableSetResource> for VariableSetResourceRepositoryAdapter {
    async fn load(
        &self,
        id: &VariableSetID,
    ) -> Result<VariableSetResource, LoadError<VariableSetState>> {
        VariableSetResource::load(id, self.event_store.as_ref()).await
    }

    async fn save(&self, resource: &mut VariableSetResource) -> Result<(), SaveError> {
        resource.save(self.event_store.as_ref()).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
