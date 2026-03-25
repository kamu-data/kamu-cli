// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use serde::{Deserialize, Serialize};

use crate::{
    ResourceReconcileError,
    StorageFailureDetails,
    StorageLifecycleError,
    StorageProviderKind,
    StorageReferenceStatus,
    StorageResource,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageReconcileSuccess {
    pub provider_kind: StorageProviderKind,
    pub references: StorageReferenceStatus,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub enum StorageReconcileError {
    #[error("referenced variable '{name}' not found")]
    MissingVariableRef { name: String },

    #[error("referenced secret '{name}' not found")]
    MissingSecretRef { name: String },

    #[error("storage provider configuration is invalid: {message}")]
    InvalidConfiguration { message: String },

    #[error("storage backend dependency unavailable: {message}")]
    DependencyUnavailable { message: String },

    #[error(transparent)]
    Internal(InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceReconcileError for StorageReconcileError {
    fn reason_code(&self) -> &'static str {
        match self {
            Self::MissingVariableRef { .. } => "MissingVariableRef",
            Self::MissingSecretRef { .. } => "MissingSecretRef",
            Self::InvalidConfiguration { .. } => "InvalidConfiguration",
            Self::DependencyUnavailable { .. } => "DependencyUnavailable",
            Self::Internal(_) => "InternalError",
        }
    }

    fn user_message(&self) -> String {
        self.to_string()
    }

    fn is_transient(&self) -> bool {
        matches!(self, Self::DependencyUnavailable { .. } | Self::Internal(_))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

crate::impl_reconcilable_event_sourced_resource!(
    resource = StorageResource,
    reconcile_success = StorageReconcileSuccess,
    reconcile_error = StorageReconcileError,
    reconcile_failure_details = StorageFailureDetails,
    lifecycle_error = StorageLifecycleError,
    reconcile_failure_details_fn = |_error| {
        StorageFailureDetails {
            references: StorageReferenceStatus::default(),
        }
    }
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
