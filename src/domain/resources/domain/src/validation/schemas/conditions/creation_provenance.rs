// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::Deserialize;

use crate::{CreationProvenance, ResourceExtensionValueError, ResourceValidateSchemaValue};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const RESOURCE_CONDITION_CREATION_PROVENANCE_SCHEMA_URI: &str =
    "https://kamu.dev/schemas/resource/v1alpha1/conditions/CreationProvenance";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct CreationProvenanceCondition;

impl ResourceValidateSchemaValue for CreationProvenanceCondition {
    type ValidationError = ResourceExtensionValueError;

    fn validate(value: &serde_json::Value) -> Result<(), Self::ValidationError> {
        CreationProvenance::deserialize(value)
            .map(|_| ())
            .map_err(|err| {
                ResourceExtensionValueError::new(format!("condition value is invalid: {err}"))
            })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
