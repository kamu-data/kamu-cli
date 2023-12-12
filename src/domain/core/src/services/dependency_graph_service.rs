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

use crate::DatasetRepository;

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DependencyGraphService: Sync + Send {
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

#[async_trait::async_trait]
pub trait DependencyGraphServiceInitializer: Send + Sync {
    /// Runs full scan of the dataset dependencies, and builds the initial
    /// graph. Note: passing dataset repository as an argument, as it
    /// requires system `CurrentAccountSubject``.
    ///
    /// If scanning has already succeedded, the request is ignored, unless
    /// `force` flag is specified.
    /// It's an error to execute multiple full scans in parallel.
    async fn full_scan(
        &self,
        dataset_repo: &dyn DatasetRepository,
        force: bool,
    ) -> Result<(), DependenciesScanError>;
}

/////////////////////////////////////////////////////////////////////////////////////////

pub type DatasetIDStream<'a> = std::pin::Pin<Box<dyn Stream<Item = DatasetID> + Send + 'a>>;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum DependenciesScanError {
    #[error(transparent)]
    AlreadyScanning(#[from] DependenciesAlreadyScanningError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(Error, Debug)]
pub enum GetDownstreamDependenciesError {
    #[error(transparent)]
    NotScanned(DependenciesNotScannedError),

    #[error(transparent)]
    DatasetNotFound(#[from] DatasetNodeNotFoundError),
}

#[derive(Error, Debug)]
pub enum GetUpstreamDependenciesError {
    #[error(transparent)]
    NotScanned(DependenciesNotScannedError),

    #[error(transparent)]
    DatasetNotFound(#[from] DatasetNodeNotFoundError),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("No dependencies data initialized")]
pub struct DependenciesNotScannedError {}

#[derive(Error, Debug)]
#[error("Dependencies are already being scanned at the moment")]
pub struct DependenciesAlreadyScanningError {}

#[derive(Error, Debug)]
#[error("Dataset {dataset_id} not found")]
pub struct DatasetNodeNotFoundError {
    pub dataset_id: DatasetID,
}

/////////////////////////////////////////////////////////////////////////////////////////
