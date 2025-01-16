// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use internal_error::*;
use kamu_core::*;
use opendatafabric::*;
use random_names::get_random_name;

use crate::DatasetRepositoryWriter;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn get_staging_name() -> String {
    let prefix = ".pending-";
    get_random_name(Some(prefix), 16)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn create_dataset_from_snapshot_impl<
    TRepository: DatasetRepository + DatasetRepositoryWriter,
>(
    dataset_repo: &TRepository,
    mut snapshot: DatasetSnapshot,
    system_time: DateTime<Utc>,
    new_dataset_id: DatasetID,
) -> Result<CreateDatasetFromSnapshotResult, CreateDatasetFromSnapshotError> {
    // Validate / resolve events
    for event in &mut snapshot.metadata {
        match event {
            MetadataEvent::Seed(_) => Err(CreateDatasetFromSnapshotError::InvalidSnapshot(
                InvalidSnapshotError::new(
                    "Seed event is generated and cannot be specified explicitly",
                ),
            )),
            MetadataEvent::SetPollingSource(e) => {
                if snapshot.kind != DatasetKind::Root {
                    Err(InvalidSnapshotError {
                        reason: "SetPollingSource event is only allowed on root datasets"
                            .to_string(),
                    }
                    .into())
                } else {
                    if let Some(transform) = &mut e.preprocess {
                        normalize_transform(transform)?;
                    }
                    Ok(())
                }
            }
            MetadataEvent::AddPushSource(e) => {
                if snapshot.kind != DatasetKind::Root {
                    Err(InvalidSnapshotError {
                        reason: "AddPushSource event is only allowed on root datasets".to_string(),
                    }
                    .into())
                } else {
                    if let Some(transform) = &mut e.preprocess {
                        normalize_transform(transform)?;
                    }
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
                    resolve_transform_inputs(&mut e.inputs, dataset_repo, &snapshot.name).await?;
                    normalize_transform(&mut e.transform)?;
                    Ok(())
                }
            }
            MetadataEvent::SetDataSchema(_) => {
                // It shouldn't be common to provide schema as part of the snapshot. In most
                // cases, it will be inferred upon first ingest/transform. But no reason not to
                // allow it.
                Ok(())
            }
            MetadataEvent::SetAttachments(_)
            | MetadataEvent::SetInfo(_)
            | MetadataEvent::SetLicense(_)
            | MetadataEvent::SetVocab(_) => Ok(()),
            MetadataEvent::AddData(_)
            | MetadataEvent::ExecuteTransform(_)
            | MetadataEvent::DisablePollingSource(_)
            | MetadataEvent::DisablePushSource(_) => Err(InvalidSnapshotError::new(format!(
                "Event is not allowed to appear in a DatasetSnapshot: {event:?}"
            ))
            .into()),
        }?;
    }

    let create_result = dataset_repo
        .create_dataset(
            &snapshot.name,
            MetadataBlockTyped {
                system_time,
                prev_block_hash: None,
                event: Seed {
                    dataset_id: new_dataset_id,
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
            for new_input in &transform.inputs {
                // Note: We already resolved all references to IDs above in
                // `resolve_transform_inputs`
                new_upstream_ids.push(new_input.dataset_ref.id().cloned().unwrap());
            }
        }

        head = match chain
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
        {
            Ok(head) => Ok(head),
            Err(e) => {
                // Attempt to clean up dataset
                let _ = dataset_repo
                    .delete_dataset(&create_result.dataset_handle)
                    .await;
                Err(e)
            }
        }?;

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

    Ok(CreateDatasetFromSnapshotResult {
        create_dataset_result: CreateDatasetResult {
            head,
            ..create_result
        },
        new_upstream_ids,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn normalize_transform(transform: &mut Transform) -> Result<(), CreateDatasetFromSnapshotError> {
    let Transform::Sql(sql) = transform;

    if let Some(query) = &sql.query {
        if sql.queries.is_some() {
            return Err(InvalidSnapshotError::new(
                "Cannot specify both 'query' and 'queries' in SetTransform",
            )
            .into());
        }

        sql.queries = Some(vec![SqlQueryStep {
            alias: None,
            query: query.clone(),
        }]);

        sql.query = None;
    }

    let nameless_queries = sql
        .queries
        .as_ref()
        .unwrap()
        .iter()
        .map(|q| &q.alias)
        .filter(|a| a.is_none())
        .count();

    if nameless_queries > 1 {
        return Err(InvalidSnapshotError::new(
            "Transform has multiple queries without an alias, only one is such query is allowed \
             to be treated as output",
        )
        .into());
    }

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Resolves dataset references in transform intputs and ensures that:
/// - input datasets are always references by unique IDs
/// - that query alias is populated (manually or from the initial reference)
async fn resolve_transform_inputs(
    inputs: &mut [TransformInput],
    repo: &dyn DatasetRepository,
    output_dataset_alias: &DatasetAlias,
) -> Result<(), CreateDatasetFromSnapshotError> {
    let mut missing_inputs = Vec::new();

    for input in inputs.iter_mut() {
        let hdl = match repo.resolve_dataset_handle_by_ref(&input.dataset_ref).await {
            Ok(hdl) => Ok(hdl),
            Err(GetDatasetError::NotFound(_)) => {
                // Accumulate errors to report as one
                missing_inputs.push(input.dataset_ref.clone());
                continue;
            }
            Err(GetDatasetError::Internal(e)) => Err(CreateDatasetFromSnapshotError::Internal(e)),
        }?;

        if input.alias.is_none() {
            input.alias = Some(input.dataset_ref.to_string());
        }

        input.dataset_ref = DatasetRef::ID(hdl.id);
    }

    if !missing_inputs.is_empty() {
        Err(CreateDatasetFromSnapshotError::MissingInputs(
            MissingInputsError {
                dataset_ref: output_dataset_alias.into(),
                missing_inputs,
            },
        ))
    } else {
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
