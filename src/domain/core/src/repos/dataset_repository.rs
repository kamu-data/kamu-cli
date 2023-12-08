// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashSet, LinkedList};
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use internal_error::InternalError;
use opendatafabric::*;
use thiserror::Error;
use tokio_stream::Stream;

use crate::auth::DatasetActionUnauthorizedError;
use crate::*;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct CreateDatasetResult {
    pub dataset_handle: DatasetHandle,
    pub dataset: Arc<dyn Dataset>,
    pub head: Multihash,
}

impl CreateDatasetResult {
    pub fn new(dataset_handle: DatasetHandle, dataset: Arc<dyn Dataset>, head: Multihash) -> Self {
        Self {
            dataset_handle,
            dataset,
            head,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
pub trait DatasetRepository: DatasetRegistry + Sync + Send {
    fn is_multi_tenant(&self) -> bool;

    async fn resolve_dataset_ref(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Result<DatasetHandle, GetDatasetError>;

    fn get_all_datasets<'s>(&'s self) -> DatasetHandleStream<'s>;

    fn get_datasets_by_owner<'s>(&'s self, account_name: AccountName) -> DatasetHandleStream<'s>;

    async fn get_dataset(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Result<Arc<dyn Dataset>, GetDatasetError>;

    async fn create_dataset(
        &self,
        dataset_alias: &DatasetAlias,
        seed_block: MetadataBlockTyped<Seed>,
    ) -> Result<CreateDatasetResult, CreateDatasetError>;

    async fn rename_dataset(
        &self,
        dataset_ref: &DatasetRef,
        new_name: &DatasetName,
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

    async fn create_dataset_from_snapshot(
        &self,
        account_name: Option<AccountName>,
        mut snapshot: DatasetSnapshot,
    ) -> Result<CreateDatasetResult, CreateDatasetFromSnapshotError>;

    async fn create_datasets_from_snapshots(
        &self,
        account_name: Option<AccountName>,
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

    async fn create_dataset_from_snapshot(
        &self,
        account_name: Option<AccountName>,
        mut snapshot: DatasetSnapshot,
    ) -> Result<CreateDatasetResult, CreateDatasetFromSnapshotError> {
        // Validate / resolve events
        // TODO: Move some of the validation to MetadataChain
        for event in snapshot.metadata.iter_mut() {
            match event {
                MetadataEvent::Seed(_) => Err(InvalidSnapshotError::new(
                    "Seed event is generated and cannot be specified explicitly",
                )
                .into()),
                MetadataEvent::SetPollingSource(_) | MetadataEvent::AddPushSource(_) => {
                    if snapshot.kind != DatasetKind::Root {
                        Err(InvalidSnapshotError {
                            reason: format!("Event is only allowed on root datasets: {:?}", event),
                        }
                        .into())
                    } else {
                        Ok(())
                    }
                }
                MetadataEvent::SetTransform(e) => {
                    if snapshot.kind != DatasetKind::Derivative {
                        Err(InvalidSnapshotError::new(
                            "SetTransform is only allowed on derivative datasets",
                        )
                        .into())
                    } else {
                        resolve_transform_inputs(self, &snapshot.name, &mut e.inputs).await
                    }
                }
                MetadataEvent::SetDataSchema(_) => {
                    // It shouldn't be common to provide schema as part of the snapshot. In most
                    // cases it will inferred upon first ingest/transform. But no reason not to
                    // allow it.
                    Ok(())
                }
                MetadataEvent::SetAttachments(_)
                | MetadataEvent::SetInfo(_)
                | MetadataEvent::SetLicense(_)
                | MetadataEvent::SetVocab(_) => Ok(()),
                MetadataEvent::AddData(_)
                | MetadataEvent::ExecuteQuery(_)
                | MetadataEvent::SetWatermark(_)
                | MetadataEvent::DisablePollingSource(_)
                | MetadataEvent::DisablePushSource(_) => Err(InvalidSnapshotError::new(format!(
                    "Event is not allowed to appear in a DatasetSnapshot: {:?}",
                    event
                ))
                .into()),
            }?;
        }

        // We are generating a key pair and deriving a dataset ID from it.
        // The key pair is discarded for now, but in future can be used for
        // proof of control over dataset and metadata signing.
        let (_keypair, dataset_id) = DatasetID::from_new_keypair_ed25519();

        let system_time = Utc::now();

        let create_result = self
            .create_dataset(
                &DatasetAlias::new(account_name, snapshot.name),
                MetadataBlockTyped {
                    system_time,
                    prev_block_hash: None,
                    event: Seed {
                        dataset_id,
                        dataset_kind: snapshot.kind,
                    },
                    sequence_number: 0,
                },
            )
            .await?;

        let chain = create_result.dataset.as_metadata_chain();
        let mut head = create_result.head.clone();
        let mut sequence_number = 1;

        for event in snapshot.metadata {
            head = chain
                .append(
                    MetadataBlock {
                        system_time,
                        prev_block_hash: Some(head),
                        event,
                        sequence_number,
                    },
                    AppendOpts {
                        update_ref: None,
                        ..AppendOpts::default()
                    },
                )
                .await
                .int_err()?;

            sequence_number += 1;
        }

        chain
            .set_ref(
                &BlockRef::Head,
                &head,
                SetRefOpts {
                    validate_block_present: false,
                    check_ref_is: Some(Some(&create_result.head)),
                },
            )
            .await
            .int_err()?;

        Ok(CreateDatasetResult {
            head,
            ..create_result
        })
    }

    async fn create_datasets_from_snapshots(
        &self,
        account_name: Option<AccountName>,
        snapshots: Vec<DatasetSnapshot>,
    ) -> Vec<(
        DatasetName,
        Result<CreateDatasetResult, CreateDatasetFromSnapshotError>,
    )> {
        let snapshots_ordered = sort_snapshots_in_dependency_order(snapshots.into_iter().collect());

        let mut ret = Vec::new();
        for snapshot in snapshots_ordered {
            let name = snapshot.name.clone();
            let res = self
                .create_dataset_from_snapshot(account_name.clone(), snapshot)
                .await;
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
            // When ID is not specified we try resolving it by name or a reference

            // When reference is available, it dominates
            let input_local_ref = if let Some(dataset_ref) = &input.dataset_ref {
                match dataset_ref.as_local_ref(|_| !repo.is_multi_tenant()) {
                    Ok(local_ref) => local_ref,
                    Err(_) => {
                        unimplemented!("Deriving from remote dataset is not supported yet");
                    }
                }
            } else {
                // Derive reference purely from a name assuming a default account
                let input_alias = DatasetAlias::new(None, input.name.clone());
                input_alias.as_local_ref()
            };

            let hdl = match repo.resolve_dataset_ref(&input_local_ref).await {
                Ok(hdl) => Ok(hdl),
                Err(GetDatasetError::NotFound(_)) => Err(
                    CreateDatasetFromSnapshotError::MissingInputs(MissingInputsError {
                        dataset_ref: dataset_name.into(),
                        missing_inputs: vec![input_local_ref],
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

impl InvalidSnapshotError {
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }
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

#[derive(Error, Debug)]
pub enum RenameDatasetError {
    #[error(transparent)]
    NotFound(#[from] DatasetNotFoundError),
    #[error(transparent)]
    NameCollision(#[from] NameCollisionError),
    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        AccessError,
    ),
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

impl From<DatasetActionUnauthorizedError> for RenameDatasetError {
    fn from(v: DatasetActionUnauthorizedError) -> Self {
        match v {
            DatasetActionUnauthorizedError::Access(e) => Self::Access(e),
            DatasetActionUnauthorizedError::Internal(e) => Self::Internal(e),
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
    Access(
        #[from]
        #[backtrace]
        AccessError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

impl From<DatasetActionUnauthorizedError> for DeleteDatasetError {
    fn from(v: DatasetActionUnauthorizedError) -> Self {
        match v {
            DatasetActionUnauthorizedError::Access(e) => Self::Access(e),
            DatasetActionUnauthorizedError::Internal(e) => Self::Internal(e),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
