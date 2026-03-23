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

use crate::ResourceValidateSpec;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VariableSetSpec {
    pub variables: BTreeMap<String, VariableSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VariableSpec {
    pub value: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl VariableSetSpec {
    pub const MAX_VARIABLES: usize = 256;
    pub const MAX_VARIABLE_VALUE_LEN: usize = 16 * 1024;

    fn is_valid_variable_name(name: &str) -> bool {
        let mut chars = name.chars();

        match chars.next() {
            Some(c) if c == '_' || c.is_ascii_uppercase() => {}
            _ => return false,
        }

        chars.all(|c| c == '_' || c.is_ascii_uppercase() || c.is_ascii_digit())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceValidateSpec for VariableSetSpec {
    type ValidationError = VariableSetSpecValidationError;

    fn validate(&self) -> Result<(), Self::ValidationError> {
        if self.variables.is_empty() {
            return Err(VariableSetSpecValidationError::EmptyVariables);
        }

        if self.variables.len() > Self::MAX_VARIABLES {
            return Err(VariableSetSpecValidationError::TooManyVariables {
                actual: self.variables.len(),
                max: Self::MAX_VARIABLES,
            });
        }

        for (name, variable) in &self.variables {
            if !Self::is_valid_variable_name(name) {
                return Err(VariableSetSpecValidationError::InvalidVariableName {
                    name: name.clone(),
                });
            }

            if variable.value.is_empty() {
                return Err(VariableSetSpecValidationError::EmptyVariableValue {
                    name: name.clone(),
                });
            }

            if variable.value.len() > Self::MAX_VARIABLE_VALUE_LEN {
                return Err(VariableSetSpecValidationError::VariableValueTooLong {
                    name: name.clone(),
                    actual: variable.value.len(),
                    max: Self::MAX_VARIABLE_VALUE_LEN,
                });
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum VariableSetSpecValidationError {
    #[error("variable set must contain at least one variable")]
    EmptyVariables,

    #[error("too many variables: got {actual}, max is {max}")]
    TooManyVariables { actual: usize, max: usize },

    #[error("invalid variable name '{name}': expected regex ^[A-Z_][A-Z0-9_]*$")]
    InvalidVariableName { name: String },

    #[error("variable '{name}' has empty value")]
    EmptyVariableValue { name: String },

    #[error("variable '{name}' value is too long: got {actual}, max is {max}")]
    VariableValueTooLong {
        name: String,
        actual: usize,
        max: usize,
    },

    #[error("description is too long: got {actual}, max is {max}")]
    DescriptionTooLong { actual: usize, max: usize },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
