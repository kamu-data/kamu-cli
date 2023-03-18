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

#[derive(Debug)]
pub struct CreateDatasetResult {
    pub dataset_handle: DatasetHandle,
    pub head: Multihash,
    pub head_sequence_number: i32,
}

impl CreateDatasetResult {
    pub fn new(dataset_handle: DatasetHandle, head: Multihash, head_sequence_number: i32) -> Self {
        Self {
            dataset_handle,
            head,
            head_sequence_number,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
pub trait DatasetRepository: DatasetRegistry + Sync + Send {
    async fn resolve_dataset_ref(
        &self,
        dataset_ref: &DatasetRefLocal,
    ) -> Result<DatasetHandle, GetDatasetError>;

    fn get_all_datasets<'s>(&'s self) -> DatasetHandleStream<'s>;

    async fn get_dataset(
        &self,
        dataset_ref: &DatasetRefLocal,
    ) -> Result<Arc<dyn Dataset>, GetDatasetError>;

    async fn create_dataset(
        &self,
        dataset_name: &DatasetName,
    ) -> Result<Box<dyn DatasetBuilder>, BeginCreateDatasetError>;

    async fn rename_dataset(
        &self,
        dataset_ref: &DatasetRefLocal,
        new_name: &DatasetName,
    ) -> Result<(), RenameDatasetError>;

    async fn delete_dataset(&self, dataset_ref: &DatasetRefLocal)
        -> Result<(), DeleteDatasetError>;

    fn get_downstream_dependencies<'s>(
        &'s self,
        dataset_ref: &'s DatasetRefLocal,
    ) -> DatasetHandleStream<'s>;
}

/////////////////////////////////////////////////////////////////////////////////////////

pub type DatasetHandleStream<'a> =
    Pin<Box<dyn Stream<Item = Result<DatasetHandle, InternalError>> + Send + 'a>>;

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
pub trait DatasetBuilder: Send + Sync {
    fn as_dataset(&self) -> &dyn Dataset;
    async fn finish(&self) -> Result<DatasetHandle, CreateDatasetError>;
    async fn discard(&self) -> Result<(), InternalError>;
}

/////////////////////////////////////////////////////////////////////////////////////////
// Extensions
/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
pub trait DatasetRepositoryExt: DatasetRepository {
    async fn try_resolve_dataset_ref(
        &self,
        dataset_ref: &DatasetRefLocal,
    ) -> Result<Option<DatasetHandle>, InternalError> {
        match self.resolve_dataset_ref(dataset_ref).await {
            Ok(hdl) => Ok(Some(hdl)),
            Err(GetDatasetError::NotFound(_)) => Ok(None),
            Err(GetDatasetError::Internal(e)) => Err(e),
        }
    }

    async fn try_get_dataset(
        &self,
        dataset_ref: &DatasetRefLocal,
    ) -> Result<Option<Arc<dyn Dataset>>, InternalError> {
        match self.get_dataset(dataset_ref).await {
            Ok(ds) => Ok(Some(ds)),
            Err(GetDatasetError::NotFound(_)) => Ok(None),
            Err(GetDatasetError::Internal(e)) => Err(e),
        }
    }

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

    async fn create_dataset_from_blocks<IT>(
        &self,
        dataset_name: &DatasetName,
        blocks: IT,
    ) -> Result<CreateDatasetResult, CreateDatasetError>
    where
        IT: IntoIterator<Item = MetadataBlock> + Send,
        IT::IntoIter: Send,
    {
        let ds = self.create_dataset(dataset_name).await?;
        let mut hash = None;
        let mut sequence_number = -1;
        for mut block in blocks {
            sequence_number += 1;
            block.prev_block_hash = hash.clone();
            block.sequence_number = sequence_number;
            hash = Some(
                ds.as_dataset()
                    .as_metadata_chain()
                    .append(block, AppendOpts::default())
                    .await
                    .int_err()?,
            );
        }
        let hdl = ds.finish().await?;
        Ok(CreateDatasetResult::new(
            hdl,
            hash.unwrap(),
            sequence_number,
        ))
    }
}

impl<T> DatasetRepositoryExt for T
where
    T: DatasetRepository,
    T: ?Sized,
{
}

/////////////////////////////////////////////////////////////////////////////////////////
// Errors
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Clone, PartialEq, Eq, Debug)]
#[error("Dataset not found: {dataset_ref}")]
pub struct DatasetNotFoundError {
    pub dataset_ref: DatasetRefLocal,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Clone, PartialEq, Eq, Debug)]
pub struct MissingInputsError {
    pub dataset_ref: DatasetRefLocal,
    pub missing_inputs: Vec<DatasetRefLocal>,
}

impl std::fmt::Display for MissingInputsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Dataset {} is referencing non-existing inputs: ",
            self.dataset_ref
        )?;
        for (i, h) in self.missing_inputs.iter().enumerate() {
            if i != 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", h)?;
        }
        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Clone, PartialEq, Eq, Debug)]
pub struct DanglingReferenceError {
    pub dataset_handle: DatasetHandle,
    pub children: Vec<DatasetHandle>,
}

impl std::fmt::Display for DanglingReferenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Dataset {} is referenced by: ", self.dataset_handle)?;
        for (i, h) in self.children.iter().enumerate() {
            if i != 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", h)?;
        }
        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Clone, PartialEq, Eq, Debug)]
#[error("Dataset with name {name} already exists")]
pub struct NameCollisionError {
    pub name: DatasetName,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Clone, PartialEq, Eq, Debug)]
#[error("Invalid snapshot: {reason}")]
pub struct InvalidSnapshotError {
    pub reason: String,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetDatasetError {
    #[error(transparent)]
    NotFound(#[from] DatasetNotFoundError),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum BeginCreateDatasetError {
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

#[derive(Error, Debug)]
pub enum CreateDatasetError {
    #[error("Dataset is empty")]
    EmptyDataset,
    #[error(transparent)]
    NameCollision(#[from] NameCollisionError),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

#[derive(Error, Debug)]
pub enum CreateDatasetFromSnapshotError {
    #[error(transparent)]
    InvalidSnapshot(#[from] InvalidSnapshotError),
    #[error(transparent)]
    MissingInputs(#[from] MissingInputsError),
    #[error(transparent)]
    NameCollision(#[from] NameCollisionError),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

impl From<BeginCreateDatasetError> for CreateDatasetError {
    fn from(v: BeginCreateDatasetError) -> Self {
        match v {
            BeginCreateDatasetError::Internal(e) => Self::Internal(e),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum RenameDatasetError {
    #[error(transparent)]
    NotFound(#[from] DatasetNotFoundError),
    #[error(transparent)]
    NameCollision(#[from] NameCollisionError),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

impl From<GetDatasetError> for RenameDatasetError {
    fn from(v: GetDatasetError) -> Self {
        match v {
            GetDatasetError::NotFound(e) => Self::NotFound(e),
            GetDatasetError::Internal(e) => Self::Internal(e),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum DeleteDatasetError {
    #[error(transparent)]
    NotFound(#[from] DatasetNotFoundError),
    #[error(transparent)]
    DanglingReference(#[from] DanglingReferenceError),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
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
            CreateDatasetError::NameCollision(e) => Self::NameCollision(e),
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
