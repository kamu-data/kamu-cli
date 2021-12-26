// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::sync_service::*;
use crate::domain::DomainError;
use opendatafabric::{DatasetHandle, DatasetRefAny, Multihash, RemoteDatasetHandle};

use std::sync::Arc;
use thiserror::Error;

///////////////////////////////////////////////////////////////////////////////
// Service
///////////////////////////////////////////////////////////////////////////////

pub trait PushService: Send + Sync {
    fn push_multi(
        &self,
        dataset_refs: &mut dyn Iterator<Item = DatasetRefAny>,
        options: PushOptions,
        sync_listener: Option<Arc<dyn SyncMultiListener>>,
    ) -> Vec<(PushInfo, Result<PushResult, PushError>)>;
}

#[derive(Debug, Clone)]
pub struct PushOptions {
    /// Push all dataset dependencies recursively in depth-first order
    pub recursive: bool,
    /// Push all known datasets
    pub all: bool,
    /// Sync options
    pub sync_options: SyncOptions,
}

impl Default for PushOptions {
    fn default() -> Self {
        Self {
            recursive: false,
            all: false,
            sync_options: SyncOptions::default(),
        }
    }
}

#[derive(Debug)]
pub struct PushInfo {
    pub original_ref: DatasetRefAny,
    pub local_handle: Option<DatasetHandle>,
    pub remote_handle: Option<RemoteDatasetHandle>,
}

#[derive(Debug)]
pub enum PushResult {
    UpToDate,
    Updated {
        old_head: Option<Multihash>,
        new_head: Multihash,
        num_blocks: usize,
    },
}

///////////////////////////////////////////////////////////////////////////////
// Errors
///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum PushError {
    #[error("No associated push alias")]
    NoTarget,
    #[error("Cannot choose between multiple push aliases")]
    AmbiguousTarget,
    #[error("Domain error: {0}")]
    DomainError(#[from] DomainError),
    #[error("Sync error: {0}")]
    SyncError(#[from] SyncError),
}
