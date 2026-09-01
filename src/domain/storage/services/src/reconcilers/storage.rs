// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::{ReconcilableResource, Reconciler};
use kamu_storage::{StorageReconcileSuccess, StorageResource};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Reconciler<StorageResource>)]
pub struct StorageReconcilerImpl {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl Reconciler<StorageResource> for StorageReconcilerImpl {
    async fn reconcile(
        &self,
        _resource: &StorageResource,
    ) -> Result<
        <StorageResource as ReconcilableResource>::ReconcileSuccess,
        <StorageResource as ReconcilableResource>::ReconcileError,
    > {
        // TODO: actually synchronize storage and resolve references

        Ok(StorageReconcileSuccess {})
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
