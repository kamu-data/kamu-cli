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
    ResourceIdentityRow,
    ResourceIdentityView,
    ResourceSnapshot,
    ResourceUID,
    UnsupportedResourceDescriptorError,
};

use crate::ResourceLookupProblem;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn resource_identity_from_snapshot<E>(
    snapshot: ResourceSnapshot,
    descriptors_by_key: &HashMap<(String, String), String>,
) -> Result<ResourceIdentityView, E>
where
    E: From<UnsupportedResourceDescriptorError>,
{
    let key = (snapshot.kind.clone(), snapshot.api_version.clone());
    let found = descriptors_by_key.get(&key);
    let canonical_kind_name = found
        .ok_or_else(|| {
            let (kind, api_version) = key;
            UnsupportedResourceDescriptorError::NotFound { kind, api_version }
        })?
        .clone();

    Ok(ResourceIdentityView {
        kind: snapshot.kind,
        api_version: snapshot.api_version,
        canonical_kind_name,
        uid: snapshot.uid,
        name: snapshot.metadata.name,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn resource_identity_from_row<E>(
    row: ResourceIdentityRow,
    descriptors_by_key: &HashMap<(String, String), String>,
) -> Result<ResourceIdentityView, E>
where
    E: From<UnsupportedResourceDescriptorError>,
{
    let key = (row.kind.clone(), row.api_version.clone());
    let found = descriptors_by_key.get(&key);
    let canonical_kind_name = found
        .ok_or_else(|| {
            let (kind, api_version) = key;
            UnsupportedResourceDescriptorError::NotFound { kind, api_version }
        })?
        .clone();

    Ok(ResourceIdentityView {
        kind: row.kind,
        api_version: row.api_version,
        canonical_kind_name,
        uid: ResourceUID::new(row.uid),
        name: row.name,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn validate_identity_row<F1, F2>(
    row: ResourceIdentityRow,
    expected_kind: &str,
    expected_api_version: Option<&String>,
    ensure_kind_matches: F1,
    ensure_requested_api_version: F2,
) -> Result<ResourceIdentityRow, ResourceLookupProblem>
where
    F1: FnOnce(ResourceUID, &str, &str) -> Result<(), ResourceLookupProblem>,
    F2: FnOnce(Option<&String>, &str) -> Result<(), ResourceLookupProblem>,
{
    ensure_kind_matches(ResourceUID::new(row.uid), expected_kind, &row.kind)?;
    ensure_requested_api_version(expected_api_version, &row.api_version)?;

    Ok(row)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn map_snapshots_to_identities(
    snapshots: Vec<ResourceSnapshot>,
    descriptors_by_key: &HashMap<(String, String), String>,
) -> Result<Vec<ResourceIdentityView>, InternalError> {
    snapshots
        .into_iter()
        .map(|snapshot| {
            resource_identity_from_snapshot::<UnsupportedResourceDescriptorError>(
                snapshot,
                descriptors_by_key,
            )
            .map_err(|error| InternalError::new(format!("{error}")))
        })
        .collect()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
