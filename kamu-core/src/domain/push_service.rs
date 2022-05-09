// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{sync_service::*, DatasetNotFoundError, GetDatasetError};
use crate::domain::InternalError;
use opendatafabric::*;

use std::sync::Arc;
use thiserror::Error;

///////////////////////////////////////////////////////////////////////////////
// Service
///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait(?Send)]
pub trait PushService: Send + Sync {
    async fn push_multi(
        &self,
        dataset_refs: &mut dyn Iterator<Item = DatasetRefAny>,
        options: PushOptions,
        sync_listener: Option<Arc<dyn SyncMultiListener>>,
    ) -> Vec<PushResponse>;

    async fn push_multi_ext(
        &self,
        items: &mut dyn Iterator<Item = PushRequest>,
        options: PushOptions,
        sync_listener: Option<Arc<dyn SyncMultiListener>>,
    ) -> Vec<PushResponse>;
}

#[derive(Debug, Clone)]
pub struct PushRequest {
    pub local_ref: Option<DatasetRefLocal>,
    pub remote_ref: Option<DatasetRefRemote>,
}

#[derive(Debug)]
pub struct PushResponse {
    /// Parameters passed into the call
    pub original_request: PushRequest,
    /// Local dataset handle, if resolved
    pub local_handle: Option<DatasetHandle>,
    /// Destination reference, if resolved
    pub remote_ref: Option<DatasetRefRemote>,
    /// Result of the push operation
    pub result: Result<SyncResult, PushError>,
}

#[derive(Debug, Clone)]
pub struct PushOptions {
    /// Push all dataset dependencies recursively in depth-first order
    pub recursive: bool,
    /// Push all known datasets
    pub all: bool,
    /// Add remote aliases to datasets if they don't already exist
    pub add_aliases: bool,
    /// Sync options
    pub sync_options: SyncOptions,
}

impl Default for PushOptions {
    fn default() -> Self {
        Self {
            recursive: false,
            all: false,
            add_aliases: true,
            sync_options: SyncOptions::default(),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////
// Errors
///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum PushError {
    #[error(transparent)]
    SourceNotFound(#[from] DatasetNotFoundError),
    #[error("Destination is not specified and there is no associated push alias")]
    NoTarget,
    #[error("Cannot choose between multiple push aliases")]
    AmbiguousTarget,
    #[error(transparent)]
    SyncError(#[from] SyncError),
    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl From<GetDatasetError> for PushError {
    fn from(v: GetDatasetError) -> Self {
        match v {
            GetDatasetError::NotFound(e) => e.into(),
            GetDatasetError::Internal(e) => e.into(),
        }
    }
}
