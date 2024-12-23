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
use random_names::get_random_name;

use crate::DatasetStorageUnitWriter;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn get_staging_name() -> String {
    let prefix = ".pending-";
    get_random_name(Some(prefix), 16)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn create_dataset_from_snapshot_impl<
    TStorageUnit: odf::DatasetStorageUnit + DatasetStorageUnitWriter,
>(
    dataset_storage_unit: &TStorageUnit,
    mut snapshot: odf::DatasetSnapshot,
    system_time: DateTime<Utc>,
    new_dataset_id: odf::DatasetID,
) -> Result<odf::CreateDatasetFromSnapshotResult, odf::dataset::CreateDatasetFromSnapshotError> {
    // Validate / resolve events
    for event in &mut snapshot.metadata {
        match event {
            odf::MetadataEvent::Seed(_) => Err(
                odf::dataset::CreateDatasetFromSnapshotError::InvalidSnapshot(
                    odf::dataset::InvalidSnapshotError::new(
                        "Seed event is generated and cannot be specified explicitly",
                    ),
                ),
            ),
            odf::MetadataEvent::SetPollingSource(e) => {
                if snapshot.kind != odf::DatasetKind::Root {
                    Err(odf::dataset::InvalidSnapshotError {
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
            odf::MetadataEvent::AddPushSource(e) => {
                if snapshot.kind != odf::DatasetKind::Root {
                    Err(odf::dataset::InvalidSnapshotError {
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
            odf::MetadataEvent::SetTransform(e) => {
                if snapshot.kind != odf::DatasetKind::Derivative {
                    Err(odf::dataset::InvalidSnapshotError::new(
                        "SetTransform is only allowed on derivative datasets",
                    )
                    .into())
                } else {
                    resolve_transform_inputs(&mut e.inputs, dataset_storage_unit, &snapshot.name)
                        .await?;
                    normalize_transform(&mut e.transform)?;
                    Ok(())
                }
            }
            odf::MetadataEvent::SetDataSchema(_) => {
                // It shouldn't be common to provide schema as part of the snapshot. In most
                // cases, it will be inferred upon first ingest/transform. But no reason not to
                // allow it.
                Ok(())
            }
            odf::MetadataEvent::SetAttachments(_)
            | odf::MetadataEvent::SetInfo(_)
            | odf::MetadataEvent::SetLicense(_)
            | odf::MetadataEvent::SetVocab(_) => Ok(()),
            odf::MetadataEvent::AddData(_)
            | odf::MetadataEvent::ExecuteTransform(_)
            | odf::MetadataEvent::DisablePollingSource(_)
            | odf::MetadataEvent::DisablePushSource(_) => {
                Err(odf::dataset::InvalidSnapshotError::new(format!(
                    "Event is not allowed to appear in a DatasetSnapshot: {event:?}"
                ))
                .into())
            }
        }?;
    }

    let create_result = dataset_storage_unit
        .create_dataset(
            &snapshot.name,
            odf::MetadataBlockTyped {
                system_time,
                prev_block_hash: None,
                event: odf::metadata::Seed {
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
    let mut new_upstream_ids: Vec<odf::DatasetID> = vec![];

    for event in snapshot.metadata {
        if let odf::MetadataEvent::SetTransform(transform) = &event {
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
                odf::MetadataBlock {
                    system_time,
                    prev_block_hash: Some(head),
                    event,
                    sequence_number,
                },
                odf::dataset::AppendOpts {
                    update_ref: None,
                    ..odf::dataset::AppendOpts::default()
                },
            )
            .await
        {
            Ok(head) => Ok(head),
            Err(e) => {
                // Attempt to clean up dataset
                let _ = dataset_storage_unit
                    .delete_dataset(&create_result.dataset_handle)
                    .await;
                Err(e)
            }
        }?;

        sequence_number += 1;
    }

    chain
        .set_ref(
            &odf::BlockRef::Head,
            &head,
            odf::dataset::SetRefOpts {
                validate_block_present: false,
                check_ref_is: Some(Some(&create_result.head)),
            },
        )
        .await
        .int_err()?;

    Ok(odf::CreateDatasetFromSnapshotResult {
        create_dataset_result: odf::CreateDatasetResult {
            head,
            ..create_result
        },
        new_upstream_ids,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn normalize_transform(
    transform: &mut odf::metadata::Transform,
) -> Result<(), odf::dataset::CreateDatasetFromSnapshotError> {
    let odf::metadata::Transform::Sql(sql) = transform;

    if let Some(query) = &sql.query {
        if sql.queries.is_some() {
            return Err(odf::dataset::InvalidSnapshotError::new(
                "Cannot specify both 'query' and 'queries' in SetTransform",
            )
            .into());
        }

        sql.queries = Some(vec![odf::metadata::SqlQueryStep {
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
        return Err(odf::dataset::InvalidSnapshotError::new(
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
    inputs: &mut [odf::metadata::TransformInput],
    repo: &dyn odf::DatasetStorageUnit,
    output_dataset_alias: &odf::DatasetAlias,
) -> Result<(), odf::dataset::CreateDatasetFromSnapshotError> {
    let mut missing_inputs = Vec::new();

    for input in inputs.iter_mut() {
        let hdl = match repo
            .resolve_stored_dataset_handle_by_ref(&input.dataset_ref)
            .await
        {
            Ok(hdl) => Ok(hdl),
            Err(odf::dataset::GetDatasetError::NotFound(_)) => {
                // Accumulate errors to report as one
                missing_inputs.push(input.dataset_ref.clone());
                continue;
            }
            Err(odf::dataset::GetDatasetError::Internal(e)) => {
                Err(odf::dataset::CreateDatasetFromSnapshotError::Internal(e))
            }
        }?;

        if input.alias.is_none() {
            input.alias = Some(input.dataset_ref.to_string());
        }

        input.dataset_ref = odf::DatasetRef::ID(hdl.id);
    }

    if !missing_inputs.is_empty() {
        Err(odf::dataset::CreateDatasetFromSnapshotError::MissingInputs(
            odf::dataset::MissingInputsError {
                dataset_ref: output_dataset_alias.into(),
                missing_inputs,
            },
        ))
    } else {
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
