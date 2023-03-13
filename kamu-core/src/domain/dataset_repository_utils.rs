// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashSet, LinkedList};

use chrono::Utc;
use opendatafabric::{
    DatasetID, DatasetKind, DatasetName, DatasetSnapshot, EnumWithVariants, MetadataBlock,
    MetadataEvent, Seed, SetTransform, TransformInput,
};

use crate::domain::{
    AppendOpts, CreateDatasetFromSnapshotError, CreateDatasetResult, DatasetRepository,
    GetDatasetError, InvalidSnapshotError, MissingInputsError, ResultIntoInternal,
};

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn create_dataset_from_snapshot(
    repo: &dyn DatasetRepository,
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
                        reason: "SetTransform is only allowed on derivative datasets".to_owned(),
                    }
                    .into())
                } else {
                    resolve_transform_inputs(repo, &snapshot.name, &mut e.inputs).await
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

    let builder = repo.create_dataset(&snapshot.name).await?;
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

pub async fn create_datasets_from_snapshots(
    repo: &dyn DatasetRepository,
    snapshots: Vec<DatasetSnapshot>,
) -> Vec<(
    DatasetName,
    Result<CreateDatasetResult, CreateDatasetFromSnapshotError>,
)> {
    let snapshots_ordered = sort_snapshots_in_dependency_order(snapshots.into_iter().collect());

    use tokio_stream::StreamExt;
    futures::stream::iter(snapshots_ordered)
        .then(|s| async {
            let name = s.name.clone();
            let res = create_dataset_from_snapshot(repo, s).await;
            (name, res)
        })
        .collect()
        .await
}

async fn resolve_transform_inputs(
    repo: &dyn DatasetRepository,
    dataset_name: &DatasetName,
    inputs: &mut Vec<TransformInput>,
) -> Result<(), CreateDatasetFromSnapshotError> {
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
            // When ID is not specified we try resolving it by name
            let hdl = match repo.resolve_dataset_ref(&input.name.as_local_ref()).await {
                Ok(hdl) => Ok(hdl),
                Err(GetDatasetError::NotFound(_)) => Err(
                    CreateDatasetFromSnapshotError::MissingInputs(MissingInputsError {
                        dataset_ref: dataset_name.into(),
                        missing_inputs: vec![input.name.as_local_ref()],
                    }),
                ),
                Err(GetDatasetError::Internal(e)) => Err(e.into()),
            }?;

            input.id = Some(hdl.id);
        }
    }
    Ok(())
}

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
