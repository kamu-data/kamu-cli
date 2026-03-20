// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{ResourceName, ResourceValidateMetadata};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceMetadataInput {
    pub name: ResourceName,
    pub description: Option<String>,
    pub labels: BTreeMap<String, String>,
    pub annotations: BTreeMap<String, String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceMetadataInput {
    pub const MAX_NAME_LEN: usize = 128;
    pub const MAX_DESCRIPTION_LEN: usize = 4096;
    pub const MAX_LABELS: usize = 64;
    pub const MAX_LABEL_VALUE_LEN: usize = 256;
    pub const MAX_ANNOTATIONS: usize = 128;
    pub const MAX_ANNOTATION_VALUE_LEN: usize = 16 * 1024;

    fn is_valid_name(name: &str) -> bool {
        if name.is_empty() {
            return false;
        }

        name.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
    }

    fn is_valid_metadata_key(key: &str) -> bool {
        if key.is_empty() {
            return false;
        }

        key.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' || c == '/')
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceValidateMetadata for ResourceMetadataInput {
    type ValidationError = ResourceMetadataValidationError;

    fn validate(&self) -> Result<(), Self::ValidationError> {
        if self.name.is_empty() {
            return Err(ResourceMetadataValidationError::EmptyName);
        }

        if self.name.len() > Self::MAX_NAME_LEN {
            return Err(ResourceMetadataValidationError::NameTooLong {
                actual: self.name.len(),
                max: Self::MAX_NAME_LEN,
            });
        }

        if !Self::is_valid_name(&self.name) {
            return Err(ResourceMetadataValidationError::InvalidName {
                name: self.name.clone(),
            });
        }

        if let Some(description) = &self.description
            && description.len() > Self::MAX_DESCRIPTION_LEN
        {
            return Err(ResourceMetadataValidationError::DescriptionTooLong {
                actual: description.len(),
                max: Self::MAX_DESCRIPTION_LEN,
            });
        }

        if self.labels.len() > Self::MAX_LABELS {
            return Err(ResourceMetadataValidationError::TooManyLabels {
                actual: self.labels.len(),
                max: Self::MAX_LABELS,
            });
        }

        for (key, value) in &self.labels {
            if !Self::is_valid_metadata_key(key) {
                return Err(ResourceMetadataValidationError::InvalidLabelKey { key: key.clone() });
            }

            if value.len() > Self::MAX_LABEL_VALUE_LEN {
                return Err(ResourceMetadataValidationError::LabelValueTooLong {
                    key: key.clone(),
                    actual: value.len(),
                    max: Self::MAX_LABEL_VALUE_LEN,
                });
            }
        }

        if self.annotations.len() > Self::MAX_ANNOTATIONS {
            return Err(ResourceMetadataValidationError::TooManyAnnotations {
                actual: self.annotations.len(),
                max: Self::MAX_ANNOTATIONS,
            });
        }

        for (key, value) in &self.annotations {
            if !Self::is_valid_metadata_key(key) {
                return Err(ResourceMetadataValidationError::InvalidAnnotationKey {
                    key: key.clone(),
                });
            }

            if value.len() > Self::MAX_ANNOTATION_VALUE_LEN {
                return Err(ResourceMetadataValidationError::AnnotationValueTooLong {
                    key: key.clone(),
                    actual: value.len(),
                    max: Self::MAX_ANNOTATION_VALUE_LEN,
                });
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub enum ResourceMetadataValidationError {
    #[error("resource name must not be empty")]
    EmptyName,

    #[error("resource name is too long: got {actual}, max is {max}")]
    NameTooLong { actual: usize, max: usize },

    #[error("invalid resource name '{name}'")]
    InvalidName { name: String },

    #[error("description is too long: got {actual}, max is {max}")]
    DescriptionTooLong { actual: usize, max: usize },

    #[error("too many labels: got {actual}, max is {max}")]
    TooManyLabels { actual: usize, max: usize },

    #[error("invalid label key '{key}'")]
    InvalidLabelKey { key: String },

    #[error("label '{key}' value is too long: got {actual}, max is {max}")]
    LabelValueTooLong {
        key: String,
        actual: usize,
        max: usize,
    },

    #[error("too many annotations: got {actual}, max is {max}")]
    TooManyAnnotations { actual: usize, max: usize },

    #[error("invalid annotation key '{key}'")]
    InvalidAnnotationKey { key: String },

    #[error("annotation '{key}' value is too long: got {actual}, max is {max}")]
    AnnotationValueTooLong {
        key: String,
        actual: usize,
        max: usize,
    },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
