// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::ResourceID;

use crate::{ResourceLookupProblem, ResourceSchemaMismatchError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn ensure_schema_matches<E>(
    id: ResourceID,
    expected_schema: &str,
    actual_schema: &str,
) -> Result<(), E>
where
    E: From<ResourceLookupProblem>,
{
    if actual_schema != expected_schema {
        return Err(
            ResourceLookupProblem::SchemaMismatch(ResourceSchemaMismatchError {
                id,
                expected_schema: expected_schema.to_string(),
                actual_schema: actual_schema.to_string(),
            })
            .into(),
        );
    }

    Ok(())
}
