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
    ResourceSnapshot,
    UnsupportedResourceDescriptorError,
};

use crate::ResourceLookupProblem;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn resource_identity_from_snapshot<E>(
    snapshot: ResourceSnapshot,
    descriptors_by_schema: &HashMap<String, String>,
) -> Result<ResourceIdentityView, E>
where
    E: From<UnsupportedResourceDescriptorError>,
{
    let schema = snapshot.schema.clone();
    let found = descriptors_by_schema.get(&schema);
    let canonical_kind_name = found
        .ok_or_else(|| UnsupportedResourceDescriptorError::NotFound {
            schema: schema.clone(),
        })?
        .clone();

    Ok(ResourceIdentityView {
        schema: snapshot.schema,
        canonical_kind_name,
        id: snapshot.id,
        name: snapshot.headers.name,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn resource_identity_from_row<E>(
    row: ResourceIdentityRow,
    descriptors_by_schema: &HashMap<String, String>,
) -> Result<ResourceIdentityView, E>
where
    E: From<UnsupportedResourceDescriptorError>,
{
    let schema = row.schema.clone();
    let found = descriptors_by_schema.get(&schema);
    let canonical_kind_name = found
        .ok_or_else(|| UnsupportedResourceDescriptorError::NotFound {
            schema: schema.clone(),
        })?
        .clone();

    Ok(ResourceIdentityView {
        schema: row.schema,
        canonical_kind_name,
        id: ResourceID::new(row.id),
        name: row.name,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn validate_identity_row<F>(
    row: ResourceIdentityRow,
    expected_schema: &str,
    ensure_schema_matches: F,
) -> Result<ResourceIdentityRow, ResourceLookupProblem>
where
    F: FnOnce(ResourceID, &str, &str) -> Result<(), ResourceLookupProblem>,
{
    ensure_schema_matches(ResourceID::new(row.id), expected_schema, &row.schema)?;

    Ok(row)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn map_snapshots_to_identities(
    snapshots: Vec<ResourceSnapshot>,
    descriptors_by_schema: &HashMap<String, String>,
) -> Result<Vec<ResourceIdentityView>, InternalError> {
    snapshots
        .into_iter()
        .map(|snapshot| {
            resource_identity_from_snapshot::<UnsupportedResourceDescriptorError>(
                snapshot,
                descriptors_by_schema,
            )
            .map_err(|error| InternalError::new(format!("{error}")))
        })
        .collect()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
