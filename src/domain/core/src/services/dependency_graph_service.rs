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

use crate::DependencyGraphRepository;

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DependencyGraphService: Sync + Send {
    /// Forces initialization of graph data, if it wasn't initialized already.
    /// Ignored if called multiple times
    async fn eager_initialization(
        &self,
        repository: &dyn DependencyGraphRepository,
    ) -> Result<(), InternalError>;

    /// Iterates over 1st level of dataset's downstream dependencies
    async fn get_downstream_dependencies(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<DatasetIDStream, GetDownstreamDependenciesError>;

    /// Iterates over 1st level of dataset's upstream dependencies
    async fn get_upstream_dependencies(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<DatasetIDStream, GetUpstreamDependenciesError>;
}

/////////////////////////////////////////////////////////////////////////////////////////

pub type DatasetIDStream<'a> = std::pin::Pin<Box<dyn Stream<Item = DatasetID> + Send + 'a>>;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetDownstreamDependenciesError {
    #[error(transparent)]
    Internal(InternalError),

    #[error(transparent)]
    DatasetNotFound(#[from] DatasetNodeNotFoundError),
}

#[derive(Error, Debug)]
pub enum GetUpstreamDependenciesError {
    #[error(transparent)]
    Internal(InternalError),

    #[error(transparent)]
    DatasetNotFound(#[from] DatasetNodeNotFoundError),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("Dataset {dataset_id} not found")]
pub struct DatasetNodeNotFoundError {
    pub dataset_id: DatasetID,
}

/////////////////////////////////////////////////////////////////////////////////////////
