// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::ResourceValidateSchemaValue;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI: &str =
    "https://kamu.dev/schemas/resource/v1alpha1/labels/Environment";
pub const RESOURCE_LABEL_ENVIRONMENT_SHORT_NAME: &str = "environment";
pub const RESOURCE_LABEL_ENVIRONMENT_SCHEMA_DOC: &str =
    include_str!("../../../../../schemas/resource/v1alpha1/labels/Environment.json");

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct Environment;

#[derive(Debug, thiserror::Error)]
pub enum EnvironmentValidationError {
    #[error("environment must be a string")]
    NotAString,
}

impl ResourceValidateSchemaValue for Environment {
    type ValidationError = EnvironmentValidationError;

    fn validate(value: &serde_json::Value) -> Result<(), Self::ValidationError> {
        if value.is_string() {
            Ok(())
        } else {
            Err(EnvironmentValidationError::NotAString)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
