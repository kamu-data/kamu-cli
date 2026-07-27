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

pub const RESOURCE_CONDITION_ACCEPTED_SCHEMA_URI: &str =
    "https://kamu.dev/schemas/resource/v1alpha1/conditions/Accepted";
pub const RESOURCE_CONDITION_ACCEPTED_SCHEMA_DOC: &str =
    include_str!("../../../../../schemas/resource/v1alpha1/conditions/Accepted.json");

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AcceptedCondition;

impl ResourceValidateSchemaValue for AcceptedCondition {
    type ValidationError = ResourceExtensionValueError;

    fn validate(value: &serde_json::Value) -> Result<(), Self::ValidationError> {
        super::validate_resource_condition_value(value)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
