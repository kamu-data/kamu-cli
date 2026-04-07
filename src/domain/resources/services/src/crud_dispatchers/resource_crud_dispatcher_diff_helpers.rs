// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;

use chrono::SubsecRound;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_resources::{
    ApplyManifestChange,
    ApplyManifestChangeKind,
    ApplyResourceAction,
    GenericResourceQueryService,
    ResourceView,
};
use serde::Serialize;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn load_previous_resource_view(
    action: ApplyResourceAction,
    uid: kamu_resources::ResourceUID,
    generic_resource_query_service: &dyn GenericResourceQueryService,
) -> Result<Option<ResourceView>, InternalError> {
    match action {
        ApplyResourceAction::Create => Ok(None),
        ApplyResourceAction::Update | ApplyResourceAction::Untouched => {
            let snapshot = generic_resource_query_service
                .get_snapshot_by_uid(&uid)
                .await?;
            let snapshot = match snapshot {
                Some(snapshot) => snapshot,
                None => InternalError::bail(format!(
                    "Failed to build apply dry-run diff for resource {uid}: current snapshot is \
                     missing"
                ))?,
            };

            Ok(Some(
                super::resource_crud_dispatcher_helpers::resource_snapshot_to_view(snapshot),
            ))
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn make_apply_manifest_changes(
    before: Option<&ResourceView>,
    after: &ResourceView,
) -> Result<Vec<ApplyManifestChange>, InternalError> {
    let mut changes = Vec::new();

    push_metadata_change(
        &mut changes,
        ApplyManifestChangeKind::Generation,
        "metadata.generation",
        before.map(|view| view.metadata.generation),
        Some(after.metadata.generation),
    )?;
    append_metadata_changes(&mut changes, before, after)?;

    changes.extend(make_spec_changes(
        before.map(|view| &view.spec),
        &after.spec,
    ));

    Ok(changes)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn append_metadata_changes(
    changes: &mut Vec<ApplyManifestChange>,
    before: Option<&ResourceView>,
    after: &ResourceView,
) -> Result<(), InternalError> {
    push_metadata_change(
        changes,
        ApplyManifestChangeKind::Metadata,
        "metadata.uid",
        before.map(|view| view.metadata.uid),
        Some(after.metadata.uid),
    )?;
    push_metadata_change(
        changes,
        ApplyManifestChangeKind::Metadata,
        "metadata.name",
        before.map(|view| &view.metadata.name),
        Some(&after.metadata.name),
    )?;
    push_metadata_change(
        changes,
        ApplyManifestChangeKind::Metadata,
        "metadata.description",
        before.and_then(|view| view.metadata.description.as_ref()),
        after.metadata.description.as_ref(),
    )?;
    push_metadata_change(
        changes,
        ApplyManifestChangeKind::Metadata,
        "metadata.labels",
        before.map(|view| &view.metadata.labels),
        Some(&after.metadata.labels),
    )?;
    push_metadata_change(
        changes,
        ApplyManifestChangeKind::Metadata,
        "metadata.annotations",
        before.map(|view| &view.metadata.annotations),
        Some(&after.metadata.annotations),
    )?;
    push_metadata_change(
        changes,
        ApplyManifestChangeKind::Metadata,
        "metadata.createdAt",
        before.map(|view| normalize_timestamp_precision(view.metadata.created_at)),
        Some(normalize_timestamp_precision(after.metadata.created_at)),
    )?;
    push_metadata_change(
        changes,
        ApplyManifestChangeKind::Metadata,
        "metadata.updatedAt",
        before.map(|view| normalize_timestamp_precision(view.metadata.updated_at)),
        Some(normalize_timestamp_precision(after.metadata.updated_at)),
    )?;
    push_metadata_change(
        changes,
        ApplyManifestChangeKind::Metadata,
        "metadata.deletedAt",
        before.and_then(|view| view.metadata.deleted_at),
        after.metadata.deleted_at,
    )?;

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn push_metadata_change<T>(
    changes: &mut Vec<ApplyManifestChange>,
    kind: ApplyManifestChangeKind,
    path: &'static str,
    before: Option<T>,
    after: Option<T>,
) -> Result<(), InternalError>
where
    T: Serialize + PartialEq,
{
    if before == after {
        return Ok(());
    }

    changes.push(ApplyManifestChange {
        kind,
        path: path.to_string(),
        before: serialize_optional_value(before)?,
        after: serialize_optional_value(after)?,
    });

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn serialize_optional_value<T>(value: Option<T>) -> Result<Option<serde_json::Value>, InternalError>
where
    T: Serialize,
{
    value
        .map(|value| serde_json::to_value(value).int_err())
        .transpose()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn normalize_timestamp_precision(
    value: chrono::DateTime<chrono::Utc>,
) -> chrono::DateTime<chrono::Utc> {
    value.trunc_subsecs(6)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn make_spec_changes(
    before: Option<&serde_json::Value>,
    after: &serde_json::Value,
) -> Vec<ApplyManifestChange> {
    match (
        before.and_then(serde_json::Value::as_object),
        after.as_object(),
    ) {
        (before, Some(after)) => make_object_spec_changes(before, after),
        (Some(before), None) if serde_json::Value::Object(before.clone()) == *after => Vec::new(),
        _ if before == Some(after) => Vec::new(),
        _ => vec![ApplyManifestChange {
            kind: ApplyManifestChangeKind::Spec,
            path: "spec".to_string(),
            before: before.cloned(),
            after: Some(after.clone()),
        }],
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn make_object_spec_changes(
    before: Option<&serde_json::Map<String, serde_json::Value>>,
    after: &serde_json::Map<String, serde_json::Value>,
) -> Vec<ApplyManifestChange> {
    let mut keys = BTreeSet::new();

    if let Some(before) = before {
        keys.extend(before.keys().cloned());
    }
    keys.extend(after.keys().cloned());

    let mut changes = Vec::new();

    for key in keys {
        let before_value = before.and_then(|before| before.get(&key));
        let after_value = after.get(&key);

        if before_value == after_value {
            continue;
        }

        changes.push(ApplyManifestChange {
            kind: ApplyManifestChangeKind::Spec,
            path: format!("spec.{key}"),
            before: before_value.cloned(),
            after: after_value.cloned(),
        });
    }

    changes
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
