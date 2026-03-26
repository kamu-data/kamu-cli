// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_configuration::{VariableSetReconcileSuccess, VariableSetResource, VariableSetStats};
use kamu_resources::{DeclarativeResource, ReconcilableResource, Reconciler};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Reconciler<VariableSetResource>)]
pub struct VariableSetReconcilerImpl {}

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

        // TODO: actually synchronize variables

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
