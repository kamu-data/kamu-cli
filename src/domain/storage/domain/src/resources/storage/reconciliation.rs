// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use kamu_resources::{RECONCILIATION_ERROR_CODE_INTERNAL_ERROR, ResourceReconcileError};

use crate::{StorageLifecycleError, StorageResource};

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
    fn code(&self) -> &'static str {
        match self {
            Self::MissingVariableRef { .. } => "missing-variable-ref",
            Self::MissingSecretRef { .. } => "missing-secret-ref",
            Self::InvalidConfiguration { .. } => "invalid-configuration",
            Self::DependencyUnavailable { .. } => "dependency-unavailable",
            Self::Internal(_) => RECONCILIATION_ERROR_CODE_INTERNAL_ERROR,
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

kamu_resources::impl_event_sourced_resource!(
    resource = StorageResource,
    reconcile_error = StorageReconcileError,
    lifecycle_error = StorageLifecycleError,
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
