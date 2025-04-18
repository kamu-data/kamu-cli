// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::sync::Arc;

use internal_error::InternalError;
use odf_metadata::{DatasetAlias, DatasetHandle, DatasetRef};
use thiserror::Error;

use crate::{DatasetUnresolvedIdError, GetStoredDatasetError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DatasetHandleResolver: Send + Sync {
    async fn resolve_dataset_handle_by_ref(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Result<DatasetHandle, DatasetRefUnresolvedError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Wraps another resolver while also taking a predefined mapping between
/// dataset aliases and IDs. This is useful when e.g. resolving dependencies of
/// a derivative dataset that depends on datasets that are being created within
/// the same transaction.
pub struct DatasetHandleResolverWithPredefined {
    inner: Option<Arc<dyn DatasetHandleResolver>>,
    predefined: BTreeMap<DatasetAlias, DatasetHandle>,
}

impl DatasetHandleResolverWithPredefined {
    pub fn new(
        inner: Option<Arc<dyn DatasetHandleResolver>>,
        predefined: BTreeMap<DatasetAlias, DatasetHandle>,
    ) -> Self {
        Self { inner, predefined }
    }
}

#[async_trait::async_trait]
impl DatasetHandleResolver for DatasetHandleResolverWithPredefined {
    async fn resolve_dataset_handle_by_ref(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Result<DatasetHandle, DatasetRefUnresolvedError> {
        if let Some(res) = match dataset_ref {
            DatasetRef::Alias(alias) => {
                // TODO: PERF: Sanity check that we don't mix single- and multi-tenant aliases
                for v in self.predefined.keys() {
                    assert!(
                        v.is_multi_tenant() == alias.is_multi_tenant(),
                        "Mixing single- and multi-tenant aliases is not allowed"
                    );
                }
                self.predefined.get(alias)
            }
            DatasetRef::ID(id) => {
                // Slow case
                self.predefined.iter().find(|v| v.1.id == *id).map(|v| v.1)
            }
            DatasetRef::Handle(hdl) => Some(hdl),
        } {
            return Ok(res.clone());
        }

        if let Some(inner) = &self.inner {
            return inner.resolve_dataset_handle_by_ref(dataset_ref).await;
        }

        Err(DatasetRefUnresolvedError::NotFound(
            crate::DatasetNotFoundError {
                dataset_ref: dataset_ref.clone(),
            },
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum DatasetRefUnresolvedError {
    #[error(transparent)]
    NotFound(#[from] DatasetNotFoundError),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

impl From<GetStoredDatasetError> for DatasetRefUnresolvedError {
    fn from(value: GetStoredDatasetError) -> Self {
        match value {
            GetStoredDatasetError::UnresolvedId(e) => Self::NotFound(e.into()),
            GetStoredDatasetError::Internal(e) => Self::Internal(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Clone, PartialEq, Eq, Debug)]
#[error("Dataset not found: {dataset_ref}")]
pub struct DatasetNotFoundError {
    pub dataset_ref: odf_metadata::DatasetRef,
}

impl DatasetNotFoundError {
    pub fn new(dataset_ref: odf_metadata::DatasetRef) -> Self {
        Self { dataset_ref }
    }
}

impl From<DatasetUnresolvedIdError> for DatasetNotFoundError {
    fn from(value: DatasetUnresolvedIdError) -> Self {
        Self {
            dataset_ref: value.dataset_id.as_local_ref(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
