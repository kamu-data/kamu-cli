// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::InternalError;
use opendatafabric::*;
use thiserror::Error;

use super::sync_service::*;
use crate::{DatasetNotFoundError, GetDatasetError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Service
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait PushService: Send + Sync {
    async fn push_multi(
        &self,
        dataset_refs: Vec<DatasetRef>,
        options: PushMultiOptions,
        sync_listener: Option<Arc<dyn SyncMultiListener>>,
    ) -> Vec<PushResponse>;
}

#[derive(Debug)]
pub struct PushResponse {
    /// Local dataset handle, if resolved
    pub local_handle: Option<DatasetHandle>,
    /// Destination reference, if resolved
    pub target: Option<DatasetPushTarget>,
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
    pub remote_target: Option<DatasetPushTarget>,
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
        DatasetNotFoundError,
    ),
    #[error("Destination is not specified and there is no associated push alias")]
    NoTarget,
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

impl From<GetDatasetError> for PushError {
    fn from(v: GetDatasetError) -> Self {
        match v {
            GetDatasetError::NotFound(e) => e.into(),
            GetDatasetError::Internal(e) => e.into(),
        }
    }
}
