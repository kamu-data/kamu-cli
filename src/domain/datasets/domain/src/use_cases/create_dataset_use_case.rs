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
use thiserror::Error;

use crate::DatasetReferenceCASError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait CreateDatasetUseCase: Send + Sync {
    async fn execute(
        &self,
        dataset_alias: &odf::DatasetAlias,
        seed_block: odf::MetadataBlockTyped<odf::metadata::Seed>,
        options: CreateDatasetUseCaseOptions,
    ) -> Result<CreateDatasetResult, CreateDatasetError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone, Default)]
pub struct CreateDatasetUseCaseOptions {
    pub dataset_visibility: odf::DatasetVisibility,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct CreateDatasetResult {
    pub dataset_handle: odf::DatasetHandle,
    pub dataset: Arc<dyn odf::Dataset>,
    pub head: odf::Multihash,
}

impl CreateDatasetResult {
    pub fn new(
        dataset_handle: odf::DatasetHandle,
        dataset: Arc<dyn odf::Dataset>,
        head: odf::Multihash,
    ) -> Self {
        Self {
            dataset_handle,
            dataset,
            head,
        }
    }

    pub fn from_stored(stored: odf::dataset::StoreDatasetResult, alias: odf::DatasetAlias) -> Self {
        Self {
            dataset_handle: odf::DatasetHandle::new(stored.dataset_id, alias, stored.dataset_kind),
            dataset: stored.dataset,
            head: stored.seed,
        }
    }
}

impl std::fmt::Debug for CreateDatasetResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CreateDatasetResult(dataset_handle={:?}, head={:?}",
            self.dataset_handle, self.head
        )?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum CreateDatasetError {
    #[error("Dataset is empty")]
    EmptyDataset,

    #[error(transparent)]
    NameCollision(#[from] NameCollisionError),

    #[error(transparent)]
    RefCollision(#[from] odf::dataset::RefCollisionError),

    #[error(transparent)]
    CASFailed(#[from] Box<DatasetReferenceCASError>),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Clone, PartialEq, Eq, Debug)]
#[error("Dataset with name {alias} already exists")]
pub struct NameCollisionError {
    pub alias: odf::DatasetAlias,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
