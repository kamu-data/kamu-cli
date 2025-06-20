// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use thiserror::Error;
use tokio_stream::Stream;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DependencyGraphService: Sync + Send {
    /// Iterates over 1st level of dataset's downstream dependencies
    async fn get_downstream_dependencies(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> DependencyDatasetIDStream;

    /// Iterates over 1st level of dataset's upstream dependencies
    async fn get_upstream_dependencies(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> DependencyDatasetIDStream;

    /// Iterates over all levels of dataset's upstream dependencies
    /// and return reversed result including passed parameters
    async fn get_recursive_upstream_dependencies(
        &self,
        dataset_ids: Vec<odf::DatasetID>,
    ) -> Result<DependencyDatasetIDStream, GetDependenciesError>;

    /// Iterates over all levels of dataset's downstream dependencies
    /// and return result including passed parameters
    async fn get_recursive_downstream_dependencies(
        &self,
        dataset_ids: Vec<odf::DatasetID>,
    ) -> Result<DependencyDatasetIDStream, GetDependenciesError>;

    /// Given a set of dataset IDs this will sort them in depth-first or
    /// breadth-first graph traversal order which is useful for operations that
    /// require upstream datasets to be processed before downstream or vice
    /// versa
    async fn in_dependency_order(
        &self,
        dataset_ids: Vec<odf::DatasetID>,
        order: DependencyOrder,
    ) -> Result<Vec<odf::DatasetID>, GetDependenciesError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type DependencyDatasetIDStream<'a> =
    std::pin::Pin<Box<dyn Stream<Item = odf::DatasetID> + Send + 'a>>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum DependencyOrder {
    BreadthFirst,
    DepthFirst,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetDependenciesError {
    #[error(transparent)]
    Internal(InternalError),

    #[error(transparent)]
    DatasetNotFound(#[from] DatasetNodeNotFoundError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("Dataset {dataset_id} not found")]
pub struct DatasetNodeNotFoundError {
    pub dataset_id: odf::DatasetID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
