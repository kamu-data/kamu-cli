// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::{ManagedResource, ResourceController};
use kamu_storage::StorageResource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn ResourceController<StorageResource>)]
pub struct StorageControllerImpl {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ResourceController<StorageResource> for StorageControllerImpl {
    async fn reconcile(
        &self,
        _resource: &StorageResource,
    ) -> Result<(), <StorageResource as ManagedResource>::ReconcileError> {
        // TODO: actually synchronize storage and resolve references

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
