// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::{
    ResourceHandle,
    ResourceHandleRow,
    ResourceID,
    ResourceName,
    ResourceSnapshot,
    TypeUri,
};

use crate::ResourceLookupProblem;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Builds a handle from a stored snapshot.
pub(crate) fn resource_handle_from_snapshot(snapshot: ResourceSnapshot) -> ResourceHandle {
    ResourceHandle {
        r#type: snapshot.schema,
        // TODO: temporary until we support DID-aware resource types; once we do,
        // this must be populated instead of always `None`.
        did: None,
        account: snapshot.headers.account,
        id: snapshot.id,
        name: snapshot.headers.name,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Builds a handle from a storage row.
pub(crate) fn resource_handle_from_row(row: ResourceHandleRow) -> ResourceHandle {
    let account = row.account_handle();
    let schema = TypeUri::new_unchecked(row.schema);

    ResourceHandle {
        r#type: schema,
        // TODO: temporary until we support DID-aware resource types; once we do,
        // this must be populated instead of always `None`.
        did: None,
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
