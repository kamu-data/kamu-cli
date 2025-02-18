// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin::Pin;
use std::sync::Arc;

use internal_error::InternalError;
use odf_metadata::*;
use thiserror::Error;
use tokio_stream::Stream;

use crate::Dataset;

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

// TODO: move from here
pub type DatasetHandleStream<'a> =
    Pin<Box<dyn Stream<Item = Result<DatasetHandle, InternalError>> + Send + 'a>>;

pub type DatasetIDStream<'a> =
    Pin<Box<dyn Stream<Item = Result<DatasetID, InternalError>> + Send + 'a>>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: redesign, split between use cases and storage
pub struct CreateDatasetResult {
    pub dataset_handle: DatasetHandle,
    pub dataset: Arc<dyn Dataset>,
    pub head: Multihash,
}

impl CreateDatasetResult {
    pub fn new(dataset_handle: DatasetHandle, dataset: Arc<dyn Dataset>, head: Multihash) -> Self {
        Self {
            dataset_handle,
            dataset,
            head,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Clone, PartialEq, Eq, Debug)]
#[error("Dataset not found: {dataset_ref}")]
pub struct DatasetNotFoundError {
    // TODO: only ID-based errors are normal here
    pub dataset_ref: DatasetRef,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetStoredDatasetError {
    #[error(transparent)]
    NotFound(#[from] DatasetNotFoundError),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
