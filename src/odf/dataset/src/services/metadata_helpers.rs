// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use internal_error::InternalError;
use odf_metadata::*;
use thiserror::Error;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn make_seed_block(
    dataset_id: DatasetID,
    dataset_kind: DatasetKind,
    system_time: DateTime<Utc>,
) -> MetadataBlockTyped<Seed> {
    MetadataBlockTyped {
        system_time,
        prev_block_hash: None,
        event: Seed {
            dataset_id,
            dataset_kind,
        },
        sequence_number: 0,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn append_snapshot_metadata_to_dataset(
    metadata: Vec<MetadataEvent>,
    dataset: &dyn Dataset,
    current_head: &Multihash,
    system_time: DateTime<Utc>,
) -> Result<AppendResult, AppendError> {
    let chain = dataset.as_metadata_chain();
    let mut head = current_head.clone();
    let mut sequence_number = 1;

    for event in metadata {
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
            .await?;

        sequence_number += 1;
    }

    Ok(AppendResult {
        existing_head: Some(current_head.clone()),
        proposed_head: head,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn normalize_and_validate_dataset_snapshot(
    dataset_handle_resolver: &dyn DatasetHandleResolver,
    snapshot: &mut DatasetSnapshot,
) -> Result<(), ValidateDatasetSnapshotError> {
    // Validate / resolve events
    for event in &mut snapshot.metadata {
        match event {
            MetadataEvent::Seed(_) => Err(InvalidSnapshotError::new(
                "Seed event is generated and cannot be specified explicitly",
            )),
            MetadataEvent::SetPollingSource(e) => {
                if snapshot.kind != DatasetKind::Root {
                    Err(InvalidSnapshotError {
                        reason: "SetPollingSource event is only allowed on root datasets"
                            .to_string(),
                    })
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
                    })
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
                    ))
                } else {
                    resolve_transform_inputs(
                        &mut e.inputs,
                        dataset_handle_resolver,
                        &snapshot.name,
                    )
                    .await?;
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
            ))),
        }?;
    }

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn normalize_transform(transform: &mut Transform) -> Result<(), InvalidSnapshotError> {
    let Transform::Sql(sql) = transform;

    if let Some(query) = &sql.query {
        if sql.queries.is_some() {
            return Err(InvalidSnapshotError::new(
                "Cannot specify both 'query' and 'queries' in SetTransform",
            ));
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
        ));
    }

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Resolves dataset references in transform intputs and ensures that:
/// - input datasets are always references by unique IDs
/// - that query alias is populated (manually or from the initial reference)
async fn resolve_transform_inputs(
    inputs: &mut [TransformInput],
    dataset_handle_resolver: &dyn DatasetHandleResolver,
    output_dataset_alias: &DatasetAlias,
) -> Result<(), ValidateDatasetSnapshotError> {
    let mut missing_inputs = Vec::new();

    for input in inputs.iter_mut() {
        let hdl = match dataset_handle_resolver
            .resolve_dataset_handle_by_ref(&input.dataset_ref)
            .await
        {
            Ok(hdl) => Ok(hdl),
            Err(GetDatasetError::NotFound(_)) => {
                // Accumulate errors to report as one
                missing_inputs.push(input.dataset_ref.clone());
                continue;
            }
            Err(GetDatasetError::Internal(e)) => Err(ValidateDatasetSnapshotError::Internal(e)),
        }?;

        if input.alias.is_none() {
            input.alias = Some(input.dataset_ref.to_string());
        }

        input.dataset_ref = DatasetRef::ID(hdl.id);
    }

    if !missing_inputs.is_empty() {
        Err(ValidateDatasetSnapshotError::MissingInputs(
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

#[derive(Error, Debug)]
pub enum ValidateDatasetSnapshotError {
    #[error(transparent)]
    InvalidSnapshot(#[from] InvalidSnapshotError),

    #[error(transparent)]
    MissingInputs(#[from] MissingInputsError),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
