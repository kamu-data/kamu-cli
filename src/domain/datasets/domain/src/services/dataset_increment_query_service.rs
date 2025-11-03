// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use odf::dataset::MetadataChainIncrementInterval;
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DatasetIncrementQueryService: Sync + Send {
    /// Computes incremental stats between two given blocks of the dataset
    async fn get_increment_between<'a>(
        &'a self,
        dataset_id: &'a odf::DatasetID,
        old_head: Option<&'a odf::Multihash>,
        new_head: &'a odf::Multihash,
    ) -> Result<MetadataChainIncrementInterval, GetIncrementError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetIncrementError {
    #[error(transparent)]
    DatasetNotFound(odf::DatasetNotFoundError),

    #[error(transparent)]
    RefNotFound(odf::storage::RefNotFoundError),

    #[error(transparent)]
    BlockNotFound(odf::storage::BlockNotFoundError),

    #[error(transparent)]
    InvalidInterval(odf::dataset::InvalidIntervalError),

    #[error(transparent)]
    Access(odf::AccessError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl From<odf::dataset::GetIncrementError> for GetIncrementError {
    fn from(e: odf::dataset::GetIncrementError) -> Self {
        match e {
            odf::dataset::GetIncrementError::BlockNotFound(e) => {
                GetIncrementError::BlockNotFound(e)
            }
            odf::dataset::GetIncrementError::InvalidInterval(e) => {
                GetIncrementError::InvalidInterval(e)
            }
            odf::dataset::GetIncrementError::Internal(e) => GetIncrementError::Internal(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
