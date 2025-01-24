// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use odf::dataset::DatasetNotFoundError;
use thiserror::Error;

use crate::auth::DatasetActionUnauthorizedError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Private Datasets: tests
#[async_trait::async_trait]
pub trait ViewDatasetUseCase: Send + Sync {
    async fn execute(
        &self,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<odf::DatasetHandle, ViewDatasetUseCaseError>;

    async fn execute_multi(
        &self,
        dataset_refs: Vec<odf::DatasetRef>,
    ) -> Result<ViewMultiResult, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct ViewMultiResult {
    pub viewable_resolved_refs: Vec<(odf::DatasetRef, odf::DatasetHandle)>,
    pub inaccessible_refs: Vec<(odf::DatasetRef, ViewDatasetUseCaseError)>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum NotAccessibleError {
    #[error(transparent)]
    NotFound(#[from] DatasetNotFoundError),

    #[error(transparent)]
    Unauthorized(#[from] DatasetActionUnauthorizedError),
}

#[derive(Error, Debug)]
pub enum ViewDatasetUseCaseError {
    #[error(transparent)]
    NotAccessible(#[from] NotAccessibleError),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
