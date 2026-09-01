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

/// Matches the Kubernetes label-value limit, which is the DNS label length.
pub(crate) const MAX_LABEL_VALUE_LEN: usize = 63;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct Environment;

#[derive(Debug, thiserror::Error)]
pub enum EnvironmentValidationError {
    #[error("environment must be a string")]
    NotAString,

    #[error("environment must not be empty")]
    Empty,

    #[error("environment is too long: got {actual}, max is {max}")]
    TooLong { actual: usize, max: usize },
}

impl ResourceValidateSchemaValue for Environment {
    type ValidationError = EnvironmentValidationError;

    fn validate(value: &serde_json::Value) -> Result<(), Self::ValidationError> {
        let Some(value) = value.as_str() else {
            return Err(EnvironmentValidationError::NotAString);
        };

        // An empty label carries no information; omitting the key says the
        // same thing without a stored value.
        if value.is_empty() {
            return Err(EnvironmentValidationError::Empty);
        }

        let actual = value.chars().count();

        if actual > MAX_LABEL_VALUE_LEN {
            return Err(EnvironmentValidationError::TooLong {
                actual,
                max: MAX_LABEL_VALUE_LEN,
            });
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use super::*;

    #[test]
    fn test_environment_schema_document_stays_in_sync_with_code() {
        let document: serde_json::Value =
            serde_json::from_str(RESOURCE_LABEL_ENVIRONMENT_SCHEMA_DOC).unwrap();

        assert_eq!(document["$id"], RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI);
        assert_eq!(document["type"], "string");
        assert_eq!(document["minLength"], serde_json::json!(1));
        assert_eq!(
            document["maxLength"],
            serde_json::json!(MAX_LABEL_VALUE_LEN)
        );
    }

    #[test]
    fn test_rejects_an_empty_environment() {
        assert_matches!(
            Environment::validate(&serde_json::json!("")),
            Err(EnvironmentValidationError::Empty)
        );
    }

    #[test]
    fn test_rejects_an_overlong_environment() {
        let value = "e".repeat(MAX_LABEL_VALUE_LEN + 1);

        assert_matches!(
            Environment::validate(&serde_json::json!(value)),
            Err(EnvironmentValidationError::TooLong { .. })
        );
    }

    #[test]
    fn test_accepts_a_value_at_the_length_limit() {
        let value = "e".repeat(MAX_LABEL_VALUE_LEN);

        assert_matches!(Environment::validate(&serde_json::json!(value)), Ok(()));
    }

    #[test]
    fn test_rejects_a_non_string_environment() {
        assert_matches!(
            Environment::validate(&serde_json::json!(42)),
            Err(EnvironmentValidationError::NotAString)
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
