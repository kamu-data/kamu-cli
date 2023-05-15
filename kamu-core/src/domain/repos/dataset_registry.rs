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
use url::Url;

use crate::domain::*;

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
pub trait DatasetRegistry: Send + Sync {
    async fn get_dataset_url(&self, dataset_ref: &DatasetRef) -> Result<Url, GetDatasetUrlError>;
}

/////////////////////////////////////////////////////////////////////////////////////////
// Errors
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetDatasetUrlError {
    #[error(transparent)]
    NotFound(
        #[from]
        #[backtrace]
        DatasetNotFoundError,
    ),
    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        AccessError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

impl From<GetDatasetError> for GetDatasetUrlError {
    fn from(v: GetDatasetError) -> Self {
        match v {
            GetDatasetError::NotFound(e) => Self::NotFound(e),
            GetDatasetError::Internal(e) => Self::Internal(e),
        }
    }
}
