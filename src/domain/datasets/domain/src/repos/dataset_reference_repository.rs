// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use opendatafabric as odf;
use thiserror::Error;

use crate::BlockPointer;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DatasetReferenceRepository: Send + Sync {
    async fn set_dataset_reference(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref_name: &str,
        block_ptr: BlockPointer,
    ) -> Result<(), InternalError>;

    async fn get_dataset_reference(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref_name: &str,
    ) -> Result<BlockPointer, GetDatasetReferenceError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum GetDatasetReferenceError {
    #[error(transparent)]
    NotFound(#[from] DatasetReferenceNotFoundError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
#[error("Reference '{block_ref_name}' could not be found for dataset '{dataset_id}'")]
pub struct DatasetReferenceNotFoundError {
    pub dataset_id: odf::DatasetID,
    pub block_ref_name: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
