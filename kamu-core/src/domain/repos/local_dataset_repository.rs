// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::*;
use opendatafabric::*;

use async_trait::async_trait;
use std::pin::Pin;
use std::sync::Arc;
use thiserror::Error;
use tokio_stream::Stream;

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
pub trait LocalDatasetRepository: Sync + Send {
    async fn resolve_dataset_ref(
        &self,
        dataset_ref: &DatasetRefLocal,
    ) -> Result<DatasetHandle, GetDatasetError>;

    fn get_all_datasets<'s>(&'s self) -> DatasetListStream<'s>;

    async fn get_dataset(
        &self,
        dataset_ref: &DatasetRefLocal,
    ) -> Result<Arc<dyn Dataset>, GetDatasetError>;

    async fn create_dataset(
        &self,
        dataset_name: &DatasetName,
    ) -> Result<Box<dyn DatasetBuilder>, BeginCreateDatasetError>;

    async fn get_or_create_dataset(
        &self,
        dataset_ref: &DatasetRefLocal,
    ) -> Result<Box<dyn DatasetBuilder>, GetDatasetError> {
        match self.resolve_dataset_ref(dataset_ref).await {
            Ok(hdl) => {
                let ds = self.get_dataset(&hdl.as_local_ref()).await?;
                Ok(Box::new(NullDatasetBuilder::new(hdl, ds)))
            }
            Err(e @ GetDatasetError::NotFound(_)) => match dataset_ref.name() {
                None => Err(e),
                Some(name) => match self.create_dataset(name).await {
                    Ok(b) => Ok(b),
                    Err(BeginCreateDatasetError::Internal(e)) => Err(GetDatasetError::Internal(e)),
                },
            },
            Err(GetDatasetError::Internal(e)) => Err(GetDatasetError::Internal(e)),
        }
    }

    async fn create_dataset_from_snapshot(
        &self,
        snapshot: DatasetSnapshot,
    ) -> Result<(DatasetHandle, Multihash), CreateDatasetFromSnapshotError>;

    async fn create_datasets_from_snapshots(
        &self,
        snapshots: Vec<DatasetSnapshot>,
    ) -> Vec<(
        DatasetName,
        Result<(DatasetHandle, Multihash), CreateDatasetFromSnapshotError>,
    )>;

    async fn delete_dataset(&self, dataset_ref: &DatasetRefLocal)
        -> Result<(), DeleteDatasetError>;
}
/////////////////////////////////////////////////////////////////////////////////////////

pub type DatasetListStream<'a> =
    Pin<Box<dyn Stream<Item = Result<DatasetHandle, InternalError>> + Send + 'a>>;

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
pub trait DatasetBuilder: Send + Sync {
    fn as_dataset(&self) -> &dyn Dataset;
    async fn finish(&self) -> Result<DatasetHandle, CreateDatasetError>;
    async fn discard(&self) -> Result<(), InternalError>;
}

/////////////////////////////////////////////////////////////////////////////////////////
// Errors
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Clone, PartialEq, Eq, Debug)]
#[error("dataset not found: {dataset_ref}")]
pub struct DatasetNotFoundError {
    pub dataset_ref: DatasetRefLocal,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Clone, PartialEq, Eq, Debug)]
#[error("dataset {dataset_ref} is referencing non-existing inputs: {missing_inputs:?}")]
pub struct MissingInputsError {
    pub dataset_ref: DatasetRefLocal,
    pub missing_inputs: Vec<DatasetRefLocal>,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Clone, PartialEq, Eq, Debug)]
#[error("dataset {dataset_handle} is referenced by: {children:?}")]
pub struct DanglingReferenceError {
    pub dataset_handle: DatasetHandle,
    pub children: Vec<DatasetHandle>,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Clone, PartialEq, Eq, Debug)]
#[error("new dataset {new_dataset:?} collides with existing dataset {existing_dataset:?}")]
pub struct CollisionError {
    pub new_dataset: DatasetHandle,
    pub existing_dataset: DatasetHandle,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Clone, PartialEq, Eq, Debug)]
#[error("invalid snapshot: {reason}")]
pub struct InvalidSnapshotError {
    pub reason: String,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetDatasetError {
    #[error(transparent)]
    NotFound(#[from] DatasetNotFoundError),
    #[error(transparent)]
    Internal(#[from] InternalError),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum BeginCreateDatasetError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(Error, Debug)]
pub enum CreateDatasetError {
    #[error("dataset is empty")]
    EmptyDataset,
    #[error(transparent)]
    Collision(#[from] CollisionError),
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(Error, Debug)]
pub enum CreateDatasetFromSnapshotError {
    #[error(transparent)]
    InvalidSnapshot(#[from] InvalidSnapshotError),
    #[error(transparent)]
    MissingInputs(#[from] MissingInputsError),
    #[error(transparent)]
    Collision(#[from] CollisionError),
    #[error(transparent)]
    Internal(#[from] InternalError),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum DeleteDatasetError {
    #[error(transparent)]
    NotFound(#[from] DatasetNotFoundError),
    #[error(transparent)]
    DanglingReference(#[from] DanglingReferenceError),
    #[error(transparent)]
    Internal(#[from] InternalError),
}

/////////////////////////////////////////////////////////////////////////////////////////

impl From<BeginCreateDatasetError> for CreateDatasetFromSnapshotError {
    fn from(v: BeginCreateDatasetError) -> Self {
        match v {
            BeginCreateDatasetError::Internal(e) => Self::Internal(e),
        }
    }
}

impl From<CreateDatasetError> for CreateDatasetFromSnapshotError {
    fn from(v: CreateDatasetError) -> Self {
        match v {
            CreateDatasetError::EmptyDataset => unreachable!(),
            CreateDatasetError::Collision(e) => Self::Collision(e),
            CreateDatasetError::Internal(e) => Self::Internal(e),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

struct NullDatasetBuilder {
    hdl: DatasetHandle,
    dataset: Arc<dyn Dataset>,
}

impl NullDatasetBuilder {
    pub fn new(hdl: DatasetHandle, dataset: Arc<dyn Dataset>) -> Self {
        Self { hdl, dataset }
    }
}

#[async_trait]
impl DatasetBuilder for NullDatasetBuilder {
    fn as_dataset(&self) -> &dyn Dataset {
        self.dataset.as_ref()
    }

    async fn finish(&self) -> Result<DatasetHandle, CreateDatasetError> {
        Ok(self.hdl.clone())
    }

    async fn discard(&self) -> Result<(), InternalError> {
        Ok(())
    }
}
