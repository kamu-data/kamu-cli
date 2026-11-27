// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::{ResourceID, ResourceRef, ResourceVisibility, TypeUri};

use crate::{ResourceLookupProblem, ResourceSchemaMismatchError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn ensure_schema_matches<E>(
    id: ResourceID,
    expected_schema: &TypeUri,
    actual_schema: &str,
) -> Result<(), E>
where
    E: From<ResourceLookupProblem>,
{
    if actual_schema != expected_schema.as_str() {
        return Err(
            ResourceLookupProblem::SchemaMismatch(ResourceSchemaMismatchError {
                id,
                expected_schema: expected_schema.clone(),
                actual_schema: TypeUri::new_unchecked(actual_schema),
            })
            .into(),
        );
    }

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Which resources the ref may find. Checked on what was read, since a resource
/// found by its name may have been deleted before the read.
pub(crate) fn ref_visibility(
    resource_ref: &ResourceRef,
    visibility: ResourceVisibility,
) -> ResourceVisibility {
    if resource_ref.id.is_some() {
        visibility.of_ids()
    } else {
        visibility
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
