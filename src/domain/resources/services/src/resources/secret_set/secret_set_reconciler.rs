// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::{ReconcilableResource, SecretSetReconcileSuccess};

use crate::domain::{DeclarativeResource, Reconciler, SecretSetResource, SecretSetStats};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SecretSetReconciler {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl Reconciler<SecretSetResource> for SecretSetReconciler {
    async fn reconcile(
        &self,
        resource: &SecretSetResource,
    ) -> Result<
        <SecretSetResource as ReconcilableResource>::ReconcileSuccess,
        <SecretSetResource as ReconcilableResource>::ReconcicleError,
    > {
        let total = resource.spec().secrets.len();

        // TODO: actually synchronize secrets

        Ok(SecretSetReconcileSuccess {
            stats: SecretSetStats {
                total_secrets: total,
                valid_secrets: total,
                invalid_secrets: 0,
            },
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
