// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{
    ResourceAnnotations,
    ResourceLabels,
    ResourceName,
    ResourceValidateHeaders,
    TypeRef,
    TypeUri,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type ResourceHeadersInput = odf::metadata::resources::ResourceHeadersInput;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait ResourceHeadersInputExt {
    /// Removes a label, if present. Spares read-modify-write callers an
    /// `Option` dance, since `into_input` always populates `labels`.
    fn remove_label(&mut self, key: &TypeRef);
}

impl ResourceHeadersInputExt for ResourceHeadersInput {
    fn remove_label(&mut self, key: &TypeRef) {
        if let Some(labels) = self.labels.as_mut() {
            labels.entries.remove(key);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const MAX_NAME_LEN: usize = 128;
pub const MAX_LABELS: usize = 64;
pub const MAX_ANNOTATIONS: usize = 128;

impl ResourceValidateHeaders for ResourceHeadersInput {
    type ValidationError = ResourceHeadersValidationError;

    fn validate(&self) -> Result<(), Self::ValidationError> {
        validate_name(&self.name)?;
        validate_entry_counts(self.labels.as_ref(), self.annotations.as_ref())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn validate_name(name: &ResourceName) -> Result<(), ResourceHeadersValidationError> {
    if name.is_empty() {
        return Err(ResourceHeadersValidationError::EmptyName);
    }

    if name.len() > MAX_NAME_LEN {
        return Err(ResourceHeadersValidationError::NameTooLong {
            actual: name.len(),
            max: MAX_NAME_LEN,
        });
    }

    Ok(())
}

pub(crate) fn validate_entry_counts(
    labels: Option<&ResourceLabels>,
    annotations: Option<&ResourceAnnotations>,
) -> Result<(), ResourceHeadersValidationError> {
    if let Some(labels) = labels
        && labels.entries.len() > MAX_LABELS
    {
        return Err(ResourceHeadersValidationError::TooManyLabels {
            actual: labels.entries.len(),
            max: MAX_LABELS,
        });
    }

    if let Some(annotations) = annotations
        && annotations.entries.len() > MAX_ANNOTATIONS
    {
        return Err(ResourceHeadersValidationError::TooManyAnnotations {
            actual: annotations.entries.len(),
            max: MAX_ANNOTATIONS,
        });
    }

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub enum ResourceHeadersValidationError {
    #[error("resource name is required: resources of type '{schema}' do not get generated names")]
    MissingName { schema: TypeUri },

    #[error("resource name must not be empty")]
    EmptyName,

    #[error("resource name is too long: got {actual}, max is {max}")]
    NameTooLong { actual: usize, max: usize },

    #[error("invalid resource name '{name}'")]
    InvalidName { name: String },

    #[error("too many labels: got {actual}, max is {max}")]
    TooManyLabels { actual: usize, max: usize },

    #[error("duplicate label key '{key}'")]
    DuplicateLabelKey { key: String },

    #[error("too many annotations: got {actual}, max is {max}")]
    TooManyAnnotations { actual: usize, max: usize },

    #[error("duplicate annotation key '{key}'")]
    DuplicateAnnotationKey { key: String },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
