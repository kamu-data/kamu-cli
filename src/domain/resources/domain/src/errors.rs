// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::{LoadError, Projection};

use crate::{ResourceDescriptor, ResourceID, ResourceSnapshot};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
#[error("Resource with the specified identity failed to load. Reason: {0}")]
pub struct ResourceLoadError<S: Projection>(pub LoadError<S>);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug, Clone, Copy, PartialEq, Eq)]
#[error("Resource with id {0} was not found")]
pub struct ResourceNotFoundError(pub ResourceID);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
#[error("Resource '{resource_id}' of type '{resource_type}' is not owned by the specified account")]
pub struct ResourceNotOwnedByAccountError {
    pub resource_id: ResourceID,
    pub resource_type: &'static str,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
#[error(
    "Resource id {resource_id} refers to {actual_kind}/{actual_api_version}, expected \
     {expected_kind}/{expected_api_version}"
)]
pub struct ResourceTypeMismatchError {
    pub resource_id: ResourceID,
    pub expected_kind: String,
    pub expected_api_version: String,
    pub actual_kind: String,
    pub actual_api_version: String,
}

impl ResourceTypeMismatchError {
    pub fn new(
        resource_id: ResourceID,
        expected_kind: String,
        expected_api_version: String,
        actual_kind: String,
        actual_api_version: String,
    ) -> Self {
        Self {
            resource_id,
            expected_kind,
            expected_api_version,
            actual_kind,
            actual_api_version,
        }
    }

    pub fn from_expected_and_actual(
        resource_id: ResourceID,
        expected: &ResourceDescriptor,
        actual: &ResourceSnapshot,
    ) -> Self {
        Self::new(
            resource_id,
            expected.resource_type.to_string(),
            expected.api_version.to_string(),
            actual.kind.clone(),
            actual.api_version.clone(),
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
