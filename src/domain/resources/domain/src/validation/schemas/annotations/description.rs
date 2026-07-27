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

pub const RESOURCE_ANNOTATION_DESCRIPTION_SCHEMA_URI: &str =
    "https://kamu.dev/schemas/resource/v1alpha1/annotations/Description";

pub const RESOURCE_ANNOTATION_DESCRIPTION_SHORT_NAME: &str = "description";

pub const RESOURCE_ANNOTATION_DESCRIPTION_SCHEMA_DOC: &str =
    include_str!("../../../../../schemas/resource/v1alpha1/annotations/Description.json");

pub(crate) const MAX_DESCRIPTION_LEN: usize = 4096;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct Description;

#[derive(Debug, thiserror::Error)]
pub enum DescriptionValidationError {
    #[error("description must be a string")]
    NotAString,

    #[error("description is too long: got {actual}, max is {max}")]
    TooLong { actual: usize, max: usize },
}

impl ResourceValidateSchemaValue for Description {
    type ValidationError = DescriptionValidationError;

    fn validate(value: &serde_json::Value) -> Result<(), Self::ValidationError> {
        let description = value
            .as_str()
            .ok_or(DescriptionValidationError::NotAString)?;
        let actual = description.chars().count();

        if actual > MAX_DESCRIPTION_LEN {
            return Err(DescriptionValidationError::TooLong {
                actual,
                max: MAX_DESCRIPTION_LEN,
            });
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_description_schema_document_stays_in_sync_with_code() {
        let document: serde_json::Value =
            serde_json::from_str(RESOURCE_ANNOTATION_DESCRIPTION_SCHEMA_DOC).unwrap();

        assert_eq!(document["$id"], RESOURCE_ANNOTATION_DESCRIPTION_SCHEMA_URI);
        assert_eq!(document["type"], "string");
        assert_eq!(
            document["maxLength"],
            serde_json::json!(MAX_DESCRIPTION_LEN)
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
