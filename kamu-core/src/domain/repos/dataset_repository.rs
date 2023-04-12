// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::*;
use chrono::Utc;
use opendatafabric::*;

use async_trait::async_trait;
use std::collections::{HashSet, LinkedList};
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
        dataset_ref: &DatasetRef,
    ) -> Result<DatasetHandle, GetDatasetError>;

    fn get_all_datasets<'s>(&'s self) -> DatasetHandleStream<'s>;

    async fn get_dataset(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Result<Arc<dyn Dataset>, GetDatasetError>;

    async fn create_dataset(
        &self,
        dataset_alias: &DatasetAlias,
    ) -> Result<Box<dyn DatasetBuilder>, BeginCreateDatasetError>;

    async fn rename_dataset(
        &self,
        dataset_ref: &DatasetRef,
        new_alias: &DatasetAlias,
    ) -> Result<(), RenameDatasetError>;

    async fn delete_dataset(&self, dataset_ref: &DatasetRef) -> Result<(), DeleteDatasetError>;

    fn get_downstream_dependencies<'s>(
        &'s self,
        dataset_ref: &'s DatasetRef,
    ) -> DatasetHandleStream<'s>;
}

/////////////////////////////////////////////////////////////////////////////////////////

pub type DatasetHandleStream<'a> =
    Pin<Box<dyn Stream<Item = Result<DatasetHandle, InternalError>> + Send + 'a>>;

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
pub trait DatasetBuilder: Send + Sync {
    fn as_dataset(&self) -> &dyn Dataset;
    fn get_staging_name(&self) -> &str;
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
        dataset_ref: &DatasetRef,
    ) -> Result<Option<DatasetHandle>, InternalError>;

    async fn try_get_dataset(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Result<Option<Arc<dyn Dataset>>, InternalError>;

    async fn get_or_create_dataset(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Result<Box<dyn DatasetBuilder>, GetDatasetError>;

    async fn create_dataset_from_blocks<IT>(
        &self,
        dataset_alias: &DatasetAlias,
        blocks: IT,
    ) -> Result<CreateDatasetResult, CreateDatasetError>
    where
        IT: IntoIterator<Item = MetadataBlock> + Send,
        IT::IntoIter: Send;

    async fn create_dataset_from_snapshot(
        &self,
        mut snapshot: DatasetSnapshot,
    ) -> Result<CreateDatasetResult, CreateDatasetFromSnapshotError>;

    async fn create_datasets_from_snapshots(
        &self,
        snapshots: Vec<DatasetSnapshot>,
    ) -> Vec<(
        DatasetName,
        Result<CreateDatasetResult, CreateDatasetFromSnapshotError>,
    )>;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl<T> DatasetRepositoryExt for T
where
    T: DatasetRepository,
    T: ?Sized,
{
    async fn try_resolve_dataset_ref(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Result<Option<DatasetHandle>, InternalError> {
        match self.resolve_dataset_ref(dataset_ref).await {
            Ok(hdl) => Ok(Some(hdl)),
            Err(GetDatasetError::NotFound(_)) => Ok(None),
            Err(GetDatasetError::Internal(e)) => Err(e),
        }
    }

    async fn try_get_dataset(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Result<Option<Arc<dyn Dataset>>, InternalError> {
        match self.get_dataset(dataset_ref).await {
            Ok(ds) => Ok(Some(ds)),
            Err(GetDatasetError::NotFound(_)) => Ok(None),
            Err(GetDatasetError::Internal(e)) => Err(e),
        }
    }

    async fn get_or_create_dataset(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Result<Box<dyn DatasetBuilder>, GetDatasetError> {
        match self.resolve_dataset_ref(dataset_ref).await {
            Ok(hdl) => {
                let ds = self.get_dataset(&hdl.as_local_ref()).await?;
                Ok(Box::new(NullDatasetBuilder::new(hdl, ds)))
            }
            Err(e @ GetDatasetError::NotFound(_)) => match dataset_ref.alias() {
                None => Err(e),
                Some(alias) => match self.create_dataset(alias).await {
                    Ok(b) => Ok(b),
                    Err(BeginCreateDatasetError::Internal(e)) => Err(GetDatasetError::Internal(e)),
                },
            },
            Err(GetDatasetError::Internal(e)) => Err(GetDatasetError::Internal(e)),
        }
    }

    async fn create_dataset_from_blocks<IT>(
        &self,
        dataset_alias: &DatasetAlias,
        blocks: IT,
    ) -> Result<CreateDatasetResult, CreateDatasetError>
    where
        IT: IntoIterator<Item = MetadataBlock> + Send,
        IT::IntoIter: Send,
    {
        let ds = self.create_dataset(dataset_alias).await?;
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

    async fn create_dataset_from_snapshot(
        &self,
        mut snapshot: DatasetSnapshot,
    ) -> Result<CreateDatasetResult, CreateDatasetFromSnapshotError> {
        // Validate / resolve events
        for event in snapshot.metadata.iter_mut() {
            match event {
                MetadataEvent::Seed(_) => Err(InvalidSnapshotError {
                    reason: "Seed event is generated and cannot be specified explicitly".to_owned(),
                }
                .into()),
                MetadataEvent::SetPollingSource(_) => {
                    if snapshot.kind != DatasetKind::Root {
                        Err(InvalidSnapshotError {
                            reason: "SetPollingSource is only allowed on root datasets".to_owned(),
                        }
                        .into())
                    } else {
                        Ok(())
                    }
                }
                MetadataEvent::SetTransform(e) => {
                    if snapshot.kind != DatasetKind::Derivative {
                        Err(InvalidSnapshotError {
                            reason: "SetTransform is only allowed on derivative datasets"
                                .to_owned(),
                        }
                        .into())
                    } else {
                        resolve_transform_inputs(self, &snapshot.name, &mut e.inputs).await
                    }
                }
                MetadataEvent::SetAttachments(_)
                | MetadataEvent::SetInfo(_)
                | MetadataEvent::SetLicense(_)
                | MetadataEvent::SetVocab(_) => Ok(()),
                MetadataEvent::AddData(_)
                | MetadataEvent::ExecuteQuery(_)
                | MetadataEvent::SetWatermark(_) => Err(InvalidSnapshotError {
                    reason: format!(
                        "Event is not allowed to appear in a DatasetSnapshot: {:?}",
                        event
                    ),
                }
                .into()),
            }?;
        }

        let system_time = Utc::now();

        let alias = DatasetAlias::new(None, snapshot.name);
        let builder = self.create_dataset(&alias).await?;
        let chain = builder.as_dataset().as_metadata_chain();

        // We are generating a key pair and deriving a dataset ID from it.
        // The key pair is discarded for now, but in future can be used for
        // proof of control over dataset and metadata signing.
        let (_keypair, dataset_id) = DatasetID::from_new_keypair_ed25519();

        let mut sequence_number = 0;

        let mut head = chain
            .append(
                MetadataBlock {
                    system_time,
                    prev_block_hash: None,
                    event: MetadataEvent::Seed(Seed {
                        dataset_id,
                        dataset_kind: snapshot.kind,
                    }),
                    sequence_number: sequence_number,
                },
                AppendOpts::default(),
            )
            .await
            .int_err()?;

        for event in snapshot.metadata {
            sequence_number += 1;
            head = chain
                .append(
                    MetadataBlock {
                        system_time,
                        prev_block_hash: Some(head),
                        event,
                        sequence_number: sequence_number,
                    },
                    AppendOpts::default(),
                )
                .await
                .int_err()?;
        }

        let hdl = builder.finish().await?;
        Ok(CreateDatasetResult::new(hdl, head, sequence_number))
    }

    async fn create_datasets_from_snapshots(
        &self,
        snapshots: Vec<DatasetSnapshot>,
    ) -> Vec<(
        DatasetName,
        Result<CreateDatasetResult, CreateDatasetFromSnapshotError>,
    )> {
        let snapshots_ordered = sort_snapshots_in_dependency_order(snapshots.into_iter().collect());

        let mut ret = Vec::new();
        for snapshot in snapshots_ordered {
            let name = snapshot.name.clone();
            let res = self.create_dataset_from_snapshot(snapshot).await;
            ret.push((name, res));
        }
        ret
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

async fn resolve_transform_inputs<T>(
    repo: &T,
    dataset_name: &DatasetName,
    inputs: &mut Vec<TransformInput>,
) -> Result<(), CreateDatasetFromSnapshotError>
where
    T: DatasetRepository,
    T: ?Sized,
{
    for input in inputs.iter_mut() {
        if let Some(input_id) = &input.id {
            // Input is referenced by ID - in this case we allow any name
            match repo.resolve_dataset_ref(&input_id.as_local_ref()).await {
                Ok(_) => Ok(()),
                Err(GetDatasetError::NotFound(_)) => Err(
                    CreateDatasetFromSnapshotError::MissingInputs(MissingInputsError {
                        dataset_ref: dataset_name.into(),
                        missing_inputs: vec![input_id.as_local_ref()],
                    }),
                ),
                Err(GetDatasetError::Internal(e)) => Err(e.into()),
            }?;
        } else {
            // TODO: Input should be a multitenant alias
            let input_alias = DatasetAlias::new(None, input.name.clone());

            // When ID is not specified we try resolving it by name
            let hdl = match repo.resolve_dataset_ref(&input_alias.as_local_ref()).await {
                Ok(hdl) => Ok(hdl),
                Err(GetDatasetError::NotFound(_)) => Err(
                    CreateDatasetFromSnapshotError::MissingInputs(MissingInputsError {
                        dataset_ref: dataset_name.into(),
                        missing_inputs: vec![input_alias.into_local_ref()],
                    }),
                ),
                Err(GetDatasetError::Internal(e)) => Err(e.into()),
            }?;

            input.id = Some(hdl.id);
        }
    }
    Ok(())
}

/////////////////////////////////////////////////////////////////////////////////////////

fn sort_snapshots_in_dependency_order(
    mut snapshots: LinkedList<DatasetSnapshot>,
) -> Vec<DatasetSnapshot> {
    let mut ordered = Vec::with_capacity(snapshots.len());
    let mut pending: HashSet<DatasetName> = snapshots.iter().map(|s| s.name.clone()).collect();
    let mut added: HashSet<DatasetName> = HashSet::new();

    // TODO: cycle detection
    while !snapshots.is_empty() {
        let snapshot = snapshots.pop_front().unwrap();

        let transform = snapshot
            .metadata
            .iter()
            .find_map(|e| e.as_variant::<SetTransform>());

        let has_pending_deps = if let Some(transform) = transform {
            transform
                .inputs
                .iter()
                .any(|input| pending.contains(&input.name))
        } else {
            false
        };

        if !has_pending_deps {
            pending.remove(&snapshot.name);
            added.insert(snapshot.name.clone());
            ordered.push(snapshot);
        } else {
            snapshots.push_back(snapshot);
        }
    }
    ordered
}

/////////////////////////////////////////////////////////////////////////////////////////
// Errors
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Clone, PartialEq, Eq, Debug)]
#[error("Dataset not found: {dataset_ref}")]
pub struct DatasetNotFoundError {
    pub dataset_ref: DatasetRef,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Clone, PartialEq, Eq, Debug)]
pub struct MissingInputsError {
    pub dataset_ref: DatasetRef,
    pub missing_inputs: Vec<DatasetRef>,
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
#[error("Dataset with name {alias} already exists")]
pub struct NameCollisionError {
    pub alias: DatasetAlias,
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

    fn get_staging_name(&self) -> &str {
        self.hdl.name.as_str()
    }

    async fn finish(&self) -> Result<DatasetHandle, CreateDatasetError> {
        Ok(self.hdl.clone())
    }

    async fn discard(&self) -> Result<(), InternalError> {
        Ok(())
    }
}
