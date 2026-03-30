// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_configuration::{
    ReplaceProjectionEntriesError,
    VariableSetEntry,
    VariableSetProjectionRepository,
    VariableSetReconcileError,
    VariableSetReconcileSuccess,
    VariableSetResource,
    VariableSetStats,
};
use kamu_resources::{DeclarativeResource, ReconcilableResource, Reconciler};
use time_source::SystemTimeSource;
use uuid::Uuid;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Reconciler<VariableSetResource>)]
pub struct VariableSetReconcilerImpl {
    variable_set_projection_repository: Arc<dyn VariableSetProjectionRepository>,
    time_source: Arc<dyn SystemTimeSource>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl Reconciler<VariableSetResource> for VariableSetReconcilerImpl {
    async fn reconcile(
        &self,
        resource: &VariableSetResource,
    ) -> Result<
        <VariableSetResource as ReconcilableResource>::ReconcileSuccess,
        <VariableSetResource as ReconcilableResource>::ReconcileError,
    > {
        let total = resource.spec().variables.len();
        let now = self.time_source.now();
        let resource_id = *resource.resource_id();
        let resource_generation = resource.metadata().generation;
        let account_id = resource.metadata().account.clone();

        let entries: Vec<_> = resource
            .spec()
            .variables
            .iter()
            .map(|(key, variable)| VariableSetEntry {
                entry_id: Uuid::new_v4(),
                account_id: account_id.clone(),
                key: key.clone(),
                value: variable.value.clone(),
                updated_at: now,
            })
            .collect();

        self.variable_set_projection_repository
            .replace_entries(&resource_id, resource_generation, &entries)
            .await
            .map_err(|e| match e {
                ReplaceProjectionEntriesError::ConcurrentModification(err) => {
                    VariableSetReconcileError::ConcurrentModification(err)
                }
                ReplaceProjectionEntriesError::Internal(err) => {
                    VariableSetReconcileError::Internal(err)
                }
            })?;

        Ok(VariableSetReconcileSuccess {
            stats: VariableSetStats {
                total_variables: total,
                valid_variables: total,
                invalid_variables: 0,
            },
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
