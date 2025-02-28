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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait AppendDatasetMetadataBatchUseCase: Send + Sync {
    async fn execute(
        &self,
        dataset: &dyn odf::Dataset,
        new_blocks_into_it: Box<dyn Iterator<Item = odf::dataset::HashedMetadataBlock> + Send>,
        options: AppendDatasetMetadataBatchUseCaseOptions,
    ) -> Result<(), AppendDatasetMetadataBatchUseCaseError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub enum SetRefCheckRefMode {
    ForceUpdateIfDiverged(bool),
    Explicit(Option<odf::Multihash>),
}

#[derive(Default)]
pub struct AppendDatasetMetadataBatchUseCaseOptions {
    pub set_ref_check_ref_mode: Option<SetRefCheckRefMode>,
    pub trust_source_hashes: Option<bool>,
    pub append_validation: Option<odf::dataset::AppendValidation>,
    pub on_append_metadata_block_callback: Option<Box<dyn Fn() + Send>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
#[error("{append_error}")]
pub struct AppendDatasetMetadataBatchUseCaseBlockAppendError {
    pub append_error: odf::dataset::AppendError,
    pub block_sequence_number: u64,
    pub block_hash: odf::Multihash,
}

#[derive(Error, Debug)]
pub enum AppendDatasetMetadataBatchUseCaseError {
    #[error(transparent)]
    BlockAppendError(#[from] AppendDatasetMetadataBatchUseCaseBlockAppendError),
    #[error(transparent)]
    SetRefError(#[from] odf::dataset::SetChainRefError),
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
