// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::ReconcileError;

use crate::domain::{DeclarativeResource, Reconciler, SecretSetResource, SecretSetStats};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SecretSetReconciler {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl Reconciler<SecretSetResource> for SecretSetReconciler {
    type Success = SecretSetReconcileSuccess;
    type Error = SecretSetReconcileError;

    async fn reconcile(&self, resource: &SecretSetResource) -> Result<Self::Success, Self::Error> {
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

pub struct SecretSetReconcileSuccess {
    pub stats: SecretSetStats,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum SecretSetReconcileError {
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        internal_error::InternalError,
    ),
}

impl ReconcileError for SecretSetReconcileError {
    fn reason_code(&self) -> &'static str {
        match self {
            SecretSetReconcileError::Internal(_) => "internal_error",
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
