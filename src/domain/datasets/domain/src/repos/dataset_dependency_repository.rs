// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin::Pin;

use internal_error::InternalError;
use thiserror::Error;
use tokio_stream::Stream;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DatasetDependencyRepository: Send + Sync {
    async fn stores_any_dependencies(&self) -> Result<bool, InternalError>;

    fn list_all_dependencies(&self) -> DatasetDependenciesIDStream;

    async fn add_upstream_dependencies(
        &self,
        downstream_dataset_id: &odf::DatasetID,
        new_upstream_dataset_ids: &[&odf::DatasetID],
    ) -> Result<(), AddDependenciesError>;

    async fn remove_upstream_dependencies(
        &self,
        downstream_dataset_id: &odf::DatasetID,
        upstream_dataset_ids: &[&odf::DatasetID],
    ) -> Result<(), RemoveDependenciesError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct DatasetDependencies {
    pub downstream_dataset_id: odf::DatasetID,
    pub upstream_dataset_ids: Vec<odf::DatasetID>,
}

pub type DatasetDependenciesIDStream<'a> =
    Pin<Box<dyn Stream<Item = Result<DatasetDependencies, InternalError>> + Send + 'a>>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum AddDependenciesError {
    #[error(transparent)]
    Duplicate(#[from] AddDependencyDuplicateError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum RemoveDependenciesError {
    #[error(transparent)]
    NotFound(#[from] RemoveDependencyMissingError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("Upstream dependency duplicate for dataset '{downstream_dataset_id}'")]
pub struct AddDependencyDuplicateError {
    pub downstream_dataset_id: odf::DatasetID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("Upstream dependency not found for dataset '{downstream_dataset_id}'")]
pub struct RemoveDependencyMissingError {
    pub downstream_dataset_id: odf::DatasetID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
