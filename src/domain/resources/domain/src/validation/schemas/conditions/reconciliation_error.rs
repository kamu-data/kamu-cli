// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{ResourceExtensionValueError, ResourceValidateSchemaValue};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const RESOURCE_CONDITION_RECONCILIATION_ERROR_SCHEMA_URI: &str =
    "https://opendatafabric.org/schemas/resources/v1alpha1/ReconciliationError";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ReconciliationErrorCondition;

impl ResourceValidateSchemaValue for ReconciliationErrorCondition {
    type ValidationError = ResourceExtensionValueError;

    fn validate(value: &serde_json::Value) -> Result<(), Self::ValidationError> {
        serde_json::from_value::<odf::metadata::serde::yaml::resources::ReconciliationError>(
            value.clone(),
        )
        .map(|_| ())
        .map_err(|err| {
            ResourceExtensionValueError::new(format!("condition value is invalid: {err}"))
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
