// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::ResourceValidateSchemaValue;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const RESOURCE_LABEL_LEGACY_CONFIG_TARGET_DATASET_SCHEMA_URI: &str =
    "https://kamu.dev/schemas/resource/v1alpha1/labels/LegacyConfigTargetDataset";
pub const RESOURCE_LABEL_LEGACY_CONFIG_TARGET_DATASET_SHORT_NAME: &str =
    "legacy-config-target-dataset";
pub const RESOURCE_LABEL_LEGACY_CONFIG_TARGET_DATASET_SCHEMA_DOC: &str =
    include_str!("../../../../schemas/config/v1alpha1/labels/LegacyConfigTargetDataset.json");

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Temporary association between a `VariableSet`/`SecretSet` resource and the
/// dataset whose legacy env vars it carries.
///
/// Replaces the former dataset-resource bindings table: the association now
/// rides on the resource itself and is looked up through the label index.
/// Superseded once `Source` resources gain direct references to `VariableSet`,
/// at which point this label can be dropped without a DB migration.
pub struct LegacyConfigTargetDataset;

#[derive(Debug, thiserror::Error)]
pub enum LegacyConfigTargetDatasetValidationError {
    #[error("legacy config target dataset must be a string")]
    NotAString,

    #[error("legacy config target dataset must not be empty")]
    Empty,

    #[error("legacy config target dataset must be a dataset DID: {reason}")]
    NotADatasetId { reason: String },
}

impl ResourceValidateSchemaValue for LegacyConfigTargetDataset {
    type ValidationError = LegacyConfigTargetDatasetValidationError;

    fn validate(value: &serde_json::Value) -> Result<(), Self::ValidationError> {
        let Some(value) = value.as_str() else {
            return Err(LegacyConfigTargetDatasetValidationError::NotAString);
        };

        // An empty label carries no information; omitting the key says the
        // same thing without a stored value.
        if value.is_empty() {
            return Err(LegacyConfigTargetDatasetValidationError::Empty);
        }

        // Deliberately no length cap: a full `did:odf:` DID is 77 characters,
        // well past the 63-char DNS-label limit that bounds `Environment`.
        // Parsing is the real constraint, and it is stricter than any length.
        odf::DatasetID::from_did_str(value).map_err(|err| {
            LegacyConfigTargetDatasetValidationError::NotADatasetId {
                reason: err.to_string(),
            }
        })?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use super::*;

    const SAMPLE_DATASET_DID: &str =
        "did:odf:fed0119d20360650afd3d412c6b11529778b784c697559c0107d37ee5da61465726c4";

    #[test]
    fn test_legacy_config_target_dataset_schema_document_stays_in_sync_with_code() {
        let document: serde_json::Value =
            serde_json::from_str(RESOURCE_LABEL_LEGACY_CONFIG_TARGET_DATASET_SCHEMA_DOC).unwrap();

        assert_eq!(
            document["$id"],
            RESOURCE_LABEL_LEGACY_CONFIG_TARGET_DATASET_SCHEMA_URI
        );
        assert_eq!(document["type"], "string");
        assert_eq!(document["minLength"], serde_json::json!(1));

        // A dataset DID is longer than any DNS-style label limit, so the
        // document must not constrain length at all.
        assert_eq!(document.get("maxLength"), None);
    }

    #[test]
    fn test_accepts_a_dataset_did() {
        assert_matches!(
            LegacyConfigTargetDataset::validate(&serde_json::json!(SAMPLE_DATASET_DID)),
            Ok(())
        );
    }

    #[test]
    fn test_accepts_a_dataset_did_longer_than_the_environment_label_limit() {
        // Guards against copying `Environment`'s 63-char cap into this schema.
        assert!(SAMPLE_DATASET_DID.chars().count() > 63);

        assert_matches!(
            LegacyConfigTargetDataset::validate(&serde_json::json!(SAMPLE_DATASET_DID)),
            Ok(())
        );
    }

    #[test]
    fn test_rejects_an_empty_value() {
        assert_matches!(
            LegacyConfigTargetDataset::validate(&serde_json::json!("")),
            Err(LegacyConfigTargetDatasetValidationError::Empty)
        );
    }

    #[test]
    fn test_rejects_a_non_string_value() {
        assert_matches!(
            LegacyConfigTargetDataset::validate(&serde_json::json!(42)),
            Err(LegacyConfigTargetDatasetValidationError::NotAString)
        );
    }

    #[test]
    fn test_rejects_a_value_that_is_not_a_dataset_did() {
        assert_matches!(
            LegacyConfigTargetDataset::validate(&serde_json::json!("not-a-did")),
            Err(LegacyConfigTargetDatasetValidationError::NotADatasetId { .. })
        );
    }

    #[test]
    fn test_rejects_an_account_did() {
        // Accounts and datasets share the `did:odf:` prefix but not the key
        // type, so a copy-pasted account DID must not silently pass.
        assert_matches!(
            LegacyConfigTargetDataset::validate(&serde_json::json!("did:odf:fed0")),
            Err(LegacyConfigTargetDatasetValidationError::NotADatasetId { .. })
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
