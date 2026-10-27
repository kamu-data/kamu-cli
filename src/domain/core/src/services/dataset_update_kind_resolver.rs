// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use thiserror::Error;

use crate::PullError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Decides how a local dataset is brought up to date.
///
/// A dataset with one pull alias is synced from it, whatever its kind. Otherwise a root dataset is
/// updated by polling ingest and a derivative by transform. Several pull aliases are ambiguous.
#[cfg_attr(feature = "testing", mockall::automock)]
#[async_trait::async_trait]
pub trait DatasetUpdateKindResolver: Send + Sync {
    async fn resolve_update_kind(
        &self,
        target: &odf::DatasetHandle,
    ) -> Result<DatasetUpdateKind, ResolveDatasetUpdateKindError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatasetUpdateKind {
    PollingIngest,
    Transform,
    SyncFrom { remote_ref: odf::DatasetRefRemote },
}

impl DatasetUpdateKind {
    pub fn is_sync_from(&self) -> bool {
        match self {
            Self::SyncFrom { .. } => true,
            Self::PollingIngest | Self::Transform => false,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ResolveDatasetUpdateKindError {
    #[error("Cannot choose between multiple pull aliases")]
    AmbiguousSource,

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

impl From<ResolveDatasetUpdateKindError> for PullError {
    fn from(value: ResolveDatasetUpdateKindError) -> Self {
        match value {
            ResolveDatasetUpdateKindError::AmbiguousSource => Self::AmbiguousSource,
            ResolveDatasetUpdateKindError::Internal(e) => Self::Internal(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
