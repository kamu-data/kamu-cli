// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use internal_error::InternalError;
use kamu_resources::{
    ResourceID,
    ResourceIdentityRow,
    ResourceIdentityView,
    ResourceName,
    ResourceSnapshot,
    TypeUri,
};

use crate::ResourceLookupProblem;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn resource_identity_from_snapshot(
    snapshot: ResourceSnapshot,
    descriptors_by_schema: &HashMap<TypeUri, String>,
) -> Result<ResourceIdentityView, InternalError> {
    let canonical_kind_name =
        canonical_kind_name_for_stored_schema(&snapshot.schema, descriptors_by_schema)?;

    Ok(ResourceIdentityView {
        schema: snapshot.schema,
        canonical_kind_name,
        id: snapshot.id,
        name: snapshot.headers.name,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn resource_identity_from_row(
    row: ResourceIdentityRow,
    descriptors_by_schema: &HashMap<TypeUri, String>,
) -> Result<ResourceIdentityView, InternalError> {
    let schema = TypeUri::new_unchecked(row.schema);
    let canonical_kind_name =
        canonical_kind_name_for_stored_schema(&schema, descriptors_by_schema)?;

    Ok(ResourceIdentityView {
        schema,
        canonical_kind_name,
        id: ResourceID::new(row.id),
        name: ResourceName::new_unchecked(&row.name),
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Resolves a stored schema to its canonical kind name. A miss is a
/// data-integrity catastrophe — the resource could not have been stored without
/// a registered descriptor — so it is surfaced as an internal error, never a
/// user-facing "unsupported" outcome.
fn canonical_kind_name_for_stored_schema(
    schema: &TypeUri,
    descriptors_by_schema: &HashMap<TypeUri, String>,
) -> Result<String, InternalError> {
    descriptors_by_schema.get(schema).cloned().ok_or_else(|| {
        InternalError::new(format!(
            "Stored resource has unregistered schema '{schema}'"
        ))
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn validate_identity_row<F>(
    row: ResourceIdentityRow,
    expected_schema: &TypeUri,
    ensure_schema_matches: F,
) -> Result<ResourceIdentityRow, ResourceLookupProblem>
where
    F: FnOnce(ResourceID, &TypeUri, &str) -> Result<(), ResourceLookupProblem>,
{
    ensure_schema_matches(ResourceID::new(row.id), expected_schema, &row.schema)?;

    Ok(row)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn map_snapshots_to_identities(
    snapshots: Vec<ResourceSnapshot>,
    descriptors_by_schema: &HashMap<TypeUri, String>,
) -> Result<Vec<ResourceIdentityView>, InternalError> {
    snapshots
        .into_iter()
        .map(|snapshot| resource_identity_from_snapshot(snapshot, descriptors_by_schema))
        .collect()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
