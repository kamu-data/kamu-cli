// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use opendatafabric::DatasetID;
use thiserror::Error;
use tokio_stream::Stream;

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DependencyGraphService: Sync + Send {
    /// Iterates over 1st level of dataset's downstream dependencies
    async fn get_downstream_dependencies(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<DatasetIDStream, GetDownstreamDependenciesError>;

    /// Tracks a dependency between upstream and downstream dataset
    ///
    /// TODO: connect to event bus
    async fn add_dependency(
        &self,
        dataset_upstream_id: &DatasetID,
        dataset_downstream_id: &DatasetID,
    ) -> Result<(), AddDependencyError>;

    /// Removes tracked dependency between updstream and downstream dataset
    ///
    /// TODO: connect to event bus
    async fn remove_dependency(
        &self,
        dataset_upstream_id: &DatasetID,
        dataset_downstream_id: &DatasetID,
    ) -> Result<(), RemoveDependencyError>;

    /// Removes dataset node and downstream nodes completely
    async fn remove_dataset(&self, dataset_id: &DatasetID) -> Result<(), RemoveDatasetError>;
}

/////////////////////////////////////////////////////////////////////////////////////////

pub type DatasetIDStream<'a> = std::pin::Pin<Box<dyn Stream<Item = DatasetID> + Send + 'a>>;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetDownstreamDependenciesError {
    #[error(transparent)]
    DatasetNotFound(#[from] DatasetNodeNotFoundError),
}

#[derive(Error, Debug)]
pub enum AddDependencyError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(Error, Debug)]
pub enum RemoveDependencyError {
    #[error(transparent)]
    Internal(#[from] InternalError),
    #[error(transparent)]
    NotFound(#[from] DependencyEdgeNotFoundError),
}

#[derive(Error, Debug)]
pub enum RemoveDatasetError {
    #[error(transparent)]
    Internal(#[from] InternalError),
    #[error(transparent)]
    DatasetNotFound(#[from] DatasetNodeNotFoundError),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("Dependency between {dataset_upstream_id} and {dataset_downstream_id} not found")]
pub struct DependencyEdgeNotFoundError {
    pub dataset_upstream_id: DatasetID,
    pub dataset_downstream_id: DatasetID,
}

#[derive(Error, Debug)]
#[error("Dataset {dataset_id} not found")]
pub struct DatasetNodeNotFoundError {
    pub dataset_id: DatasetID,
}

/////////////////////////////////////////////////////////////////////////////////////////
