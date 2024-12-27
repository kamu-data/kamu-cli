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

use super::sync_service::*;
use super::{RemoteTarget, RepositoryNotFoundError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Service
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait PushRequestPlanner: Send + Sync {
    async fn collect_plan(
        &self,
        dataset_handles: &[odf::DatasetHandle],
        push_target: Option<&odf::DatasetPushTarget>,
    ) -> (Vec<PushItem>, Vec<PushResponse>);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Eq, PartialEq)]
pub struct PushItem {
    pub local_handle: odf::DatasetHandle,
    pub remote_target: RemoteTarget,
    pub push_target: Option<odf::DatasetPushTarget>,
}

impl PushItem {
    pub fn as_response(&self, result: Result<SyncResult, SyncError>) -> PushResponse {
        PushResponse {
            local_handle: Some(self.local_handle.clone()),
            target: self.push_target.clone(),
            result: result.map_err(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PushResponse {
    /// Local dataset handle, if resolved
    pub local_handle: Option<odf::DatasetHandle>,
    /// Destination reference, if resolved
    pub target: Option<odf::DatasetPushTarget>,
    /// Result of the push operation
    pub result: Result<SyncResult, PushError>,
}

impl std::fmt::Display for PushResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (&self.local_handle, &self.target) {
            (Some(l), None) => write!(f, "{l}"),
            (None, Some(r)) => write!(f, "{r}"),
            (Some(l), Some(r)) => write!(f, "{l} to {r}"),
            (None, None) => write!(f, "???"),
        }
    }
}

impl std::fmt::Debug for PushResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PushResponse(local_handle={:?}, target={:?}, result=",
            self.local_handle, self.target
        )?;
        match &self.result {
            Ok(sync_result) => write!(f, "Ok({sync_result:?})"),
            Err(e) => write!(f, "Err({e:?})"),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct PushMultiOptions {
    /// Push all dataset dependencies recursively in depth-first order
    pub recursive: bool,
    /// Push all known datasets
    pub all: bool,
    /// Add remote aliases to datasets if they don't already exist
    pub add_aliases: bool,
    /// Sync options
    pub sync_options: SyncOptions,
    /// Destination reference, if resolved
    pub remote_target: Option<odf::DatasetPushTarget>,
}

impl Default for PushMultiOptions {
    fn default() -> Self {
        Self {
            recursive: false,
            all: false,
            add_aliases: true,
            sync_options: SyncOptions::default(),
            remote_target: None,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum PushError {
    #[error(transparent)]
    SourceNotFound(
        #[from]
        #[backtrace]
        odf::dataset::DatasetNotFoundError,
    ),
    #[error("Destination is not specified and there is no associated push alias")]
    NoTarget,
    #[error(transparent)]
    DestinationNotFound(
        #[from]
        #[backtrace]
        RepositoryNotFoundError,
    ),
    #[error("Cannot choose between multiple push aliases")]
    AmbiguousTarget,
    #[error(transparent)]
    SyncError(
        #[from]
        #[backtrace]
        SyncError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

impl From<odf::dataset::GetDatasetError> for PushError {
    fn from(v: odf::dataset::GetDatasetError) -> Self {
        match v {
            odf::dataset::GetDatasetError::NotFound(e) => e.into(),
            odf::dataset::GetDatasetError::Internal(e) => e.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
