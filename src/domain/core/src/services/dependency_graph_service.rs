// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin::Pin;
use std::sync::Arc;

use internal_error::{InternalError, ResultIntoInternal};
use opendatafabric::DatasetID;
use thiserror::Error;
use tokio_stream::Stream;

use crate::{DatasetRepository, GetSummaryOpts};

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DependencyGraphService: Sync + Send {
    /// Forces initialization of graph data, if it wasn't initialized already.
    /// Ignored if called multiple times
    async fn eager_initialization(
        &self,
        initializer: &DependencyGraphServiceInitializer,
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

#[dill::component(pub)]
pub struct DependencyGraphServiceInitializer {
    dataset_repo: Arc<dyn DatasetRepository>,
}

impl DependencyGraphServiceInitializer {
    pub fn new(dataset_repo: Arc<dyn DatasetRepository>) -> Self {
        Self { dataset_repo }
    }

    pub fn browse_dependencies_of_all_datasets(&self) -> DatasetDependenciesIDStream {
        use tokio_stream::StreamExt;

        Box::pin(async_stream::try_stream! {
            let mut datasets_stream = self.dataset_repo.get_all_datasets();
            while let Some(Ok(dataset_handle)) = datasets_stream.next().await {
                tracing::debug!(dataset=%dataset_handle, "Scanning dataset dependencies");

                let summary = self
                    .dataset_repo
                    .get_dataset(&dataset_handle.as_local_ref())
                    .await
                    .int_err()?
                    .get_summary(GetSummaryOpts::default())
                    .await
                    .int_err()?;

                let mut upstream_dataset_ids = Vec::new();
                for transform_input in summary.dependencies.iter() {
                    if let Some(input_id) = &transform_input.id {
                        upstream_dataset_ids.push(input_id.clone());
                    }
                }

                yield (dataset_handle.id, upstream_dataset_ids);
            }
        })
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

pub type DatasetIDStream<'a> = std::pin::Pin<Box<dyn Stream<Item = DatasetID> + Send + 'a>>;

pub type DatasetDependenciesIDStream<'a> =
    Pin<Box<dyn Stream<Item = Result<(DatasetID, Vec<DatasetID>), InternalError>> + Send + 'a>>;

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
