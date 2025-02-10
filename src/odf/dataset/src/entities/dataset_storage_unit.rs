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
use odf_metadata::*;
use thiserror::Error;

use crate::{Dataset, DatasetIDStream};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Abstraction of datasets storage unit, a bunch of datasets stored together
#[cfg_attr(feature = "testing", mockall::automock)]
#[async_trait::async_trait]
pub trait DatasetStorageUnit: Sync + Send {
    fn stored_dataset_ids(&self) -> DatasetIDStream<'_>;

    async fn get_stored_dataset_by_id(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<Arc<dyn Dataset>, GetStoredDatasetError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Clone, PartialEq, Eq, Debug)]
#[error("Dataset id unresolved: {dataset_id}")]
pub struct DatasetUnresolvedIdError {
    pub dataset_id: DatasetID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetStoredDatasetError {
    #[error(transparent)]
    UnresolvedId(#[from] DatasetUnresolvedIdError),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
