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

use crate::{ResourceLookupProblem, ResourceNameMismatchError};

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

/// Checks a resolved row against what the ref asserted about it.
///
/// `expected_name` is the name of a ref that supplied **both** an id and a
/// name. ODF treats that pair as a consistency assertion, and the id is the
/// authoritative half used for the lookup — so the name is only meaningful if
/// it is verified here. Left unchecked, a ref pairing one resource's id with
/// another's name would read, render, or delete the resource the id names while
/// the caller believes they addressed the one they named.
///
/// `None` for a ref that named only an id, which asserts nothing to check.
pub(crate) fn validate_handle_row<F>(
    row: ResourceHandleRow,
    expected_schema: &TypeUri,
    expected_name: Option<&ResourceName>,
    ensure_schema_matches: F,
) -> Result<ResourceHandleRow, ResourceLookupProblem>
where
    F: FnOnce(ResourceID, &TypeUri, &str) -> Result<(), ResourceLookupProblem>,
{
    ensure_schema_matches(ResourceID::new(row.id), expected_schema, &row.schema)?;

    // Compared as `ResourceName`, not as raw strings: the type's equality is
    // deliberately case-insensitive, and the repository resolves names the same
    // way. Comparing `&str` here would reject a ref whose name differs from the
    // stored one only by case — a name the lookup itself treats as a match.
    if let Some(expected_name) = expected_name {
        let actual_name = ResourceName::new_unchecked(&row.name);
        if *expected_name != actual_name {
            return Err(ResourceLookupProblem::NameMismatch(
                ResourceNameMismatchError {
                    id: ResourceID::new(row.id),
                    expected_name: expected_name.clone(),
                    actual_name,
                },
            ));
        }
    }

    Ok(row)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
