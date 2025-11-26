// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::Ordering;

use internal_error::InternalError;
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait GetDatasetDownstreamDependenciesUseCase: Send + Sync {
    async fn execute(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<Vec<DatasetDependency>, GetDatasetDownstreamDependenciesError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, PartialEq, Eq)]
pub struct ResolvedDatasetDependency {
    pub dataset_handle: odf::DatasetHandle,
    pub owner_id: odf::AccountID,
    pub owner_name: odf::AccountName,
}

#[derive(Debug, PartialEq, Eq)]
pub enum DatasetDependency {
    Resolved(ResolvedDatasetDependency),
    Unresolved(odf::DatasetID),
}

impl DatasetDependency {
    pub fn id(&self) -> &odf::DatasetID {
        match self {
            DatasetDependency::Resolved(r) => &r.dataset_handle.id,
            DatasetDependency::Unresolved(id) => id,
        }
    }
}

impl DatasetDependency {
    pub fn resolved(
        dataset_handle: odf::DatasetHandle,
        owner_id: odf::AccountID,
        owner_name: odf::AccountName,
    ) -> Self {
        Self::Resolved(ResolvedDatasetDependency {
            dataset_handle,
            owner_id,
            owner_name,
        })
    }

    pub fn unresolved(dataset_id: odf::DatasetID) -> Self {
        Self::Unresolved(dataset_id)
    }
}

impl PartialOrd for DatasetDependency {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DatasetDependency {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id().cmp(other.id())
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
