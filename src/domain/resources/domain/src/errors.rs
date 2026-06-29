// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::{LoadError, Projection};

use crate::{ResourceDescriptor, ResourceID, ResourceName, ResourceSnapshot};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
#[error("Resource with the specified identity failed to load. Reason: {0}")]
pub struct ResourceLoadError<S: Projection>(pub LoadError<S>);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug, Clone, Copy, PartialEq, Eq)]
#[error("Resource with id {0} was not found")]
pub struct ResourceIDNotFoundError(pub ResourceID);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
#[error("Resource '{name}' of kind '{kind}' was not found")]
pub struct ResourceNameNotFoundError {
    pub kind: String,
    pub name: ResourceName,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
#[error("Invalid spec for resource {schema}: {message}")]
pub struct ResourceInvalidSpecError {
    pub schema: String,
    pub message: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
#[error("Resource '{id}' of type '{resource_type}' is not owned by the specified account")]
pub struct ResourceNotOwnedByAccountError {
    pub id: ResourceID,
    pub resource_type: &'static str,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
#[error("Resource id {id} refers to {actual_schema}, expected {expected_schema}")]
pub struct ResourceTypeMismatchError {
    pub id: ResourceID,
    pub expected_schema: String,
    pub actual_schema: String,
}

impl ResourceTypeMismatchError {
    pub fn new(id: ResourceID, expected_schema: String, actual_schema: String) -> Self {
        Self {
            id,
            expected_schema,
            actual_schema,
        }
    }

    pub fn from_expected_and_actual(
        id: ResourceID,
        expected: &ResourceDescriptor,
        actual: &ResourceSnapshot,
    ) -> Self {
        Self::new(id, expected.schema.to_string(), actual.schema.clone())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
