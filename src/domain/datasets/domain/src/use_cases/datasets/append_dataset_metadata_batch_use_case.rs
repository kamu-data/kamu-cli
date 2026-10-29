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

use crate::ResolvedDataset;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait AppendDatasetMetadataBatchUseCase: Send + Sync {
    /// Appends the blocks to the metadata chain of `target` and moves its
    /// `HEAD` to the last one, in the current transaction. Returns the new
    /// `HEAD`, or `None` for an empty batch.
    ///
    /// Fails with `SetRefError(CASFailed)` when `HEAD` is not where
    /// `options.set_ref_check_ref_mode` expects it.
    async fn execute(
        &self,
        target: &ResolvedDataset,
        new_blocks_into_it: Box<
            dyn Iterator<Item = odf::dataset::HashedMetadataBlockBytesDecoded> + Send,
        >,
        options: AppendDatasetMetadataBatchUseCaseOptions,
    ) -> Result<Option<odf::Multihash>, AppendDatasetMetadataBatchUseCaseError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Which `HEAD` the batch commit expects to replace
#[derive(Default)]
pub enum SetRefCheckRefMode {
    /// The block the batch is appended to, so the batch must extend `HEAD`
    #[default]
    ExtendsBase,
    /// The given head; `None` means the dataset has no `HEAD` yet
    Explicit(Option<odf::Multihash>),
}

impl SetRefCheckRefMode {
    /// The `HEAD` that committing a batch appended to `base_head` replaces
    pub fn expected_head(self, base_head: Option<odf::Multihash>) -> Option<odf::Multihash> {
        match self {
            Self::ExtendsBase => base_head,
            Self::Explicit(head) => head,
        }
    }
}

#[derive(Default)]
pub struct AppendDatasetMetadataBatchUseCaseOptions {
    pub set_ref_check_ref_mode: SetRefCheckRefMode,
    pub append_opts: odf::dataset::AppendBatchOpts,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum AppendDatasetMetadataBatchUseCaseError {
    #[error(transparent)]
    BlockAppendError(#[from] odf::dataset::AppendBatchError),
    #[error(transparent)]
    SetRefError(#[from] odf::dataset::SetChainRefError),
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
