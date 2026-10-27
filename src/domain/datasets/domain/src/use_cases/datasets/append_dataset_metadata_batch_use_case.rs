// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use odf::dataset::MetadataChainExt as _;
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
pub enum SetRefCheckRefMode {
    /// `false`: the block the batch is appended to. `true`: whatever `HEAD`
    /// is in the current transaction, so a diverged chain is overwritten.
    /// With `true` the commit does not check a head the caller observed
    /// earlier; use `Explicit` for that.
    ForceUpdateIfDiverged(bool),
    /// The given head; `None` means the dataset has no `HEAD` yet
    Explicit(Option<odf::Multihash>),
}

impl SetRefCheckRefMode {
    /// The `HEAD` that committing a batch appended to `base_head` replaces
    pub async fn expected_head(
        self,
        metadata_chain: &dyn odf::MetadataChain,
        base_head: Option<odf::Multihash>,
    ) -> Result<Option<odf::Multihash>, InternalError> {
        match self {
            Self::ForceUpdateIfDiverged(false) => Ok(base_head),
            Self::ForceUpdateIfDiverged(true) => {
                metadata_chain.try_get_ref(&odf::BlockRef::Head).await
            }
            Self::Explicit(head) => Ok(head),
        }
    }
}

impl Default for SetRefCheckRefMode {
    fn default() -> Self {
        Self::ForceUpdateIfDiverged(false)
    }
}

#[derive(Default)]
pub struct AppendDatasetMetadataBatchUseCaseOptions {
    pub set_ref_check_ref_mode: SetRefCheckRefMode,
    pub append_opts: odf::dataset::AppendBatchOpts,
    pub on_append_metadata_block_callback: Option<Box<dyn Fn() + Send>>,
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
