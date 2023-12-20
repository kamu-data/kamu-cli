// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/////////////////////////////////////////////////////////////////////////////////////////

use chrono::Utc;
use event_bus::EventBus;
use internal_error::ResultIntoInternal;
use kamu_core::events::DatasetEventDependenciesUpdated;
use kamu_core::{
    AppendOpts,
    BlockRef,
    CreateDatasetFromSnapshotError,
    CreateDatasetResult,
    DatasetRepository,
    DatasetRepositoryExt,
    GetDatasetError,
    InvalidSnapshotError,
    MissingInputsError,
    SetRefOpts,
};
use opendatafabric::*;

pub fn get_staging_name() -> String {
    use rand::distributions::Alphanumeric;
    use rand::Rng;

    let mut name = String::with_capacity(16);
    name.push_str(".pending-");
    name.extend(
        rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(10)
            .map(char::from),
    );

    name
}

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn create_dataset_from_snapshot_impl(
    dataset_repo: &dyn DatasetRepositoryExt,
    event_bus: &EventBus,
    account_name: Option<AccountName>,
    mut snapshot: DatasetSnapshot,
) -> Result<CreateDatasetResult, CreateDatasetFromSnapshotError> {
    // Validate / resolve events
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
                    resolve_transform_inputs(dataset_repo, &snapshot.name, &mut e.inputs).await
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
    let (_keypair, dataset_id) = DatasetID::new_generated_ed25519();

    let system_time = Utc::now();

    let create_result = dataset_repo
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
    let mut new_upstream_ids: Vec<DatasetID> = vec![];

    for event in snapshot.metadata {
        if let MetadataEvent::SetTransform(transform) = &event {
            // Collect only the latest upstream dataset IDs
            new_upstream_ids.clear();
            for new_input in transform.inputs.iter() {
                // Note: the IDs have been checked in `resolve_transform_inputs`
                new_upstream_ids.push(new_input.id.clone().unwrap());
            }
        }

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

    // TODO: encapsulate this inside dataset/chain
    if !new_upstream_ids.is_empty() {
        event_bus
            .dispatch_event(DatasetEventDependenciesUpdated {
                dataset_id: create_result.dataset_handle.id.clone(),
                new_upstream_ids,
            })
            .await
            .int_err()?;
    }

    Ok(CreateDatasetResult {
        head,
        ..create_result
    })
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
