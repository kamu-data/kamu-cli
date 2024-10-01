// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;
use internal_error::InternalError;
use opendatafabric::*;
use thiserror::Error;

use crate::*;

#[async_trait]
pub trait RemoteAliasesRegistry: Send + Sync {
    async fn get_remote_aliases(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Result<Box<dyn RemoteAliases>, GetAliasesError>;
}

#[derive(Error, Debug)]
pub enum GetAliasesError {
    #[error(transparent)]
    DatasetNotFound(
        #[from]
        #[backtrace]
        DatasetNotFoundError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

impl From<GetDatasetError> for GetAliasesError {
    fn from(v: GetDatasetError) -> Self {
        match v {
            GetDatasetError::NotFound(e) => Self::DatasetNotFound(e),
            GetDatasetError::Internal(e) => Self::Internal(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// RemoteAliasResolver
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
pub trait RemoteAliasResolver: Send + Sync {
    async fn resolve_remote_alias(
        &self,
        dataset_ref_maybe: Option<&DatasetRef>,
        transfer_dataset_ref_maybe: Option<TransferDatasetRef>,
        remote_alias_kind: &RemoteAliasKind,
    ) -> Result<DatasetRefRemote, ResolveAliasError>;
}

#[derive(Error, Debug)]
pub enum ResolveAliasError {
    #[error(transparent)]
    RepositoryNotFound(
        #[from]
        #[backtrace]
        RepositoryNotFoundError,
    ),
    #[error(transparent)]
    AmbiguousRepository(
        #[from]
        #[backtrace]
        AmbiguousRepositoryError,
    ),
    #[error(transparent)]
    EmptyRepositoryList(
        #[from]
        #[backtrace]
        EmptyRepositoryListError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

#[derive(Error, Debug)]
#[error("Cannot choose between multiple repositories")]
pub struct AmbiguousRepositoryError {}

#[derive(Error, Debug)]
#[error("Repositories list is empty")]
pub struct EmptyRepositoryListError {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum GetRemoteAccountError {
    #[error(transparent)]
    InvalidResponse(InvalidGQLResponseError),

    #[error(transparent)]
    Internal(InternalError),
}

#[derive(Debug, Error)]
#[error("Invalid gql response: {response}")]
pub struct InvalidGQLResponseError {
    pub response: String,
}

impl From<InternalError> for GetRemoteAccountError {
    fn from(value: InternalError) -> Self {
        Self::Internal(value)
    }
}
