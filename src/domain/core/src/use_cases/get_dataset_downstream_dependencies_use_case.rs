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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Private Datasets: tests
#[async_trait::async_trait]
pub trait GetDatasetDownstreamDependenciesUseCase: Send + Sync {
    async fn execute(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<Vec<DownstreamDependency>, GetDatasetDownstreamDependenciesError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct ResolvedDownstreamDependency {
    pub dataset_handle: odf::DatasetHandle,
    pub owner_id: odf::AccountID,
    pub owner_name: odf::AccountName,
}

#[derive(Debug)]
pub enum DownstreamDependency {
    Resolved(ResolvedDownstreamDependency),
    Unresolved(odf::DatasetID),
}

impl DownstreamDependency {
    pub fn resolved(
        dataset_handle: odf::DatasetHandle,
        owner_id: odf::AccountID,
        owner_name: odf::AccountName,
    ) -> Self {
        Self::Resolved(ResolvedDownstreamDependency {
            dataset_handle,
            owner_id,
            owner_name,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetDatasetDownstreamDependenciesError {
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
