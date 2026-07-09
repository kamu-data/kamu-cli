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
    ResourceHandle,
    ResourceHandleRow,
    ResourceID,
    ResourceName,
    ResourceSelectorName,
    ResourceSnapshot,
    TypeUri,
};

use crate::ResourceLookupProblem;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn resource_handle_from_snapshot(
    snapshot: ResourceSnapshot,
    canonical_selectors_by_schema: &HashMap<TypeUri, ResourceSelectorName>,
) -> Result<ResourceHandle, InternalError> {
    let canonical_selector =
        canonical_selector_for_stored_schema(&snapshot.schema, canonical_selectors_by_schema)?;

    Ok(ResourceHandle {
        schema: snapshot.schema,
        canonical_selector,
        id: snapshot.id,
        name: snapshot.headers.name,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn resource_handle_from_row(
    row: ResourceHandleRow,
    canonical_selectors_by_schema: &HashMap<TypeUri, ResourceSelectorName>,
) -> Result<ResourceHandle, InternalError> {
    let schema = TypeUri::new_unchecked(row.schema);
    let canonical_selector =
        canonical_selector_for_stored_schema(&schema, canonical_selectors_by_schema)?;

    Ok(ResourceHandle {
        schema,
        canonical_selector,
        id: ResourceID::new(row.id),
        name: ResourceName::new_unchecked(&row.name),
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Resolves a stored schema to its canonical selector name. A miss is a
/// data-integrity catastrophe — the resource could not have been stored without
/// a registered descriptor — so it is surfaced as an internal error, never a
/// user-facing "unsupported" outcome.
fn canonical_selector_for_stored_schema(
    schema: &TypeUri,
    canonical_selectors_by_schema: &HashMap<TypeUri, ResourceSelectorName>,
) -> Result<ResourceSelectorName, InternalError> {
    canonical_selectors_by_schema
        .get(schema)
        .cloned()
        .ok_or_else(|| {
            InternalError::new(format!(
                "Stored resource has unregistered schema '{schema}'"
            ))
        })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn validate_handle_row<F>(
    row: ResourceHandleRow,
    expected_schema: &TypeUri,
    ensure_schema_matches: F,
) -> Result<ResourceHandleRow, ResourceLookupProblem>
where
    F: FnOnce(ResourceID, &TypeUri, &str) -> Result<(), ResourceLookupProblem>,
{
    ensure_schema_matches(ResourceID::new(row.id), expected_schema, &row.schema)?;

    Ok(row)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn map_snapshots_to_handles(
    snapshots: Vec<ResourceSnapshot>,
    canonical_selectors_by_schema: &HashMap<TypeUri, ResourceSelectorName>,
) -> Result<Vec<ResourceHandle>, InternalError> {
    snapshots
        .into_iter()
        .map(|snapshot| resource_handle_from_snapshot(snapshot, canonical_selectors_by_schema))
        .collect()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
