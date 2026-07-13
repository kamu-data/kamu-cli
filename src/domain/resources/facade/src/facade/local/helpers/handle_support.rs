// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use kamu_resources::{
    ResourceHandle,
    ResourceHandleRow,
    ResourceID,
    ResourceName,
    ResourceSnapshot,
    TypeName,
    TypeUri,
    resource_type_name,
};

use crate::ResourceLookupProblem;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Builds a handle from a stored snapshot by deriving the display `type_name`
/// from the stored schema. A parse failure is internal/corrupt data, because
/// snapshots should only contain valid resource schema URIs.
pub(crate) fn resource_handle_from_snapshot(
    snapshot: ResourceSnapshot,
) -> Result<ResourceHandle, InternalError> {
    let type_name = resource_type_name(&snapshot.schema)?;

    Ok(ResourceHandle {
        schema: snapshot.schema,
        type_name,
        account: snapshot.headers.account,
        id: snapshot.id,
        name: snapshot.headers.name,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Builds a handle from a storage row by deriving the display `type_name` from
/// the row schema. See [`resource_handle_from_snapshot`].
pub(crate) fn resource_handle_from_row(
    row: ResourceHandleRow,
) -> Result<ResourceHandle, InternalError> {
    let account = row.account_handle();
    let schema = TypeUri::new_unchecked(row.schema);
    let type_name = resource_type_name(&schema)?;

    Ok(ResourceHandle {
        schema,
        type_name,
        account,
        id: ResourceID::new(row.id),
        name: ResourceName::new_unchecked(&row.name),
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Builds a handle from a row whose schema has already been validated against
/// a known request schema, allowing the caller to resolve `type_name` once for
/// the whole batch.
pub(crate) fn resource_handle_from_row_with_type_name(
    row: ResourceHandleRow,
    type_name: TypeName,
) -> ResourceHandle {
    let account = row.account_handle();

    ResourceHandle {
        schema: TypeUri::new_unchecked(row.schema),
        type_name,
        account,
        id: ResourceID::new(row.id),
        name: ResourceName::new_unchecked(&row.name),
    }
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

/// Maps stored snapshots to handles. Each handle derives its display
/// `type_name` from its own schema, so this supports both single-schema and
/// cross-schema listings.
pub(crate) fn map_snapshots_to_handles(
    snapshots: Vec<ResourceSnapshot>,
) -> Result<Vec<ResourceHandle>, InternalError> {
    snapshots
        .into_iter()
        .map(resource_handle_from_snapshot)
        .collect()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
