// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::{ResourceAPIVersionMismatchError, ResourceID};

use crate::{ResourceKindMismatchError, ResourceLookupProblem};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn ensure_kind_matches<E>(
    id: ResourceID,
    expected_kind: &str,
    actual_kind: &str,
) -> Result<(), E>
where
    E: From<ResourceLookupProblem>,
{
    if actual_kind != expected_kind {
        return Err(
            ResourceLookupProblem::KindMismatch(ResourceKindMismatchError {
                id,
                expected_kind: expected_kind.to_string(),
                actual_kind: actual_kind.to_string(),
            })
            .into(),
        );
    }

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn ensure_requested_api_version<E>(
    expected_api_version: Option<&String>,
    actual_api_version: &str,
) -> Result<(), E>
where
    E: From<ResourceLookupProblem>,
{
    if let Some(expected_api_version) = expected_api_version
        && actual_api_version != expected_api_version
    {
        return Err(
            ResourceLookupProblem::ApiVersionMismatch(ResourceAPIVersionMismatchError {
                expected_api_version: expected_api_version.clone(),
                actual_api_version: actual_api_version.to_string(),
            })
            .into(),
        );
    }

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
