// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;
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
    MultiTenantRefUnexpected(
        #[from]
        #[backtrace]
        MultiTenantRefUnexpectedError,
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
            GetDatasetError::MultiTenantRefUnexpected(e) => Self::MultiTenantRefUnexpected(e),
            GetDatasetError::Internal(e) => Self::Internal(e),
        }
    }
}
