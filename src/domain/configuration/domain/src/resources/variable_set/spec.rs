// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::{ResourceLinterSpec, ResourceValidateSpec, ResourceWarning};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// RFC-derived variable shape (`{ value: String }`); accepts scalar-or-`{
/// value }` shorthand on input via ODF's `StructOrString`, but always
/// round-trips as the structured form once parsed — there is no retained
/// flag for "was this written as shorthand."
pub type Variable = odf::metadata::config::Variable;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_resources::declare_rfc_spec_newtype!(
    VariableSetSpec,
    dto = odf::metadata::config::VariableSetSpec,
    proxy = odf::metadata::serde::yaml::config::VariableSetSpec,
    proxy_path = "odf::metadata::serde::yaml::config::VariableSetSpec"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Field-identical to `VariableSetSpec`, but kept as its own type (not a
// `type` alias) so validation/linting attach to the write-path type the
// framework actually asks for — see `ResourceSpecFromInput`.
kamu_resources::declare_rfc_spec_newtype!(
    VariableSetSpecInput,
    dto = odf::metadata::config::VariableSetSpecInput,
    proxy = odf::metadata::serde::yaml::config::VariableSetSpecInput,
    proxy_path = "odf::metadata::serde::yaml::config::VariableSetSpecInput"
);

impl kamu_resources::ResourceSpecFromInput<VariableSetSpecInput> for VariableSetSpec {
    fn from_input(input: VariableSetSpecInput) -> Self {
        Self(odf::metadata::config::VariableSetSpec {
            variables: input.0.variables,
        })
    }

    fn into_input(self) -> VariableSetSpecInput {
        VariableSetSpecInput(odf::metadata::config::VariableSetSpecInput {
            variables: self.0.variables,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl VariableSetSpecInput {
    pub const MAX_VARIABLES: usize = 256;
    pub const MAX_VARIABLE_VALUE_LEN: usize = 16 * 1024;
    pub const WARNING_VARIABLE_VALUE_LEN: usize = 1024;
    pub const RESERVED_VARIABLE_PREFIX: &str = "KAMU_";

    pub const WARNING_CODE_RESERVED_VARIABLE_PREFIX: &str = "reserved_variable_prefix";
    pub const WARNING_CODE_LONG_VARIABLE_VALUE: &str = "long_variable_value";
    pub const WARNING_CODE_LOWERCASE_VARIABLE_NAME: &str = "lowercase_variable_name";

    fn is_valid_variable_name(name: &str) -> bool {
        let mut chars = name.chars();

        match chars.next() {
            Some(c) if c == '_' || c.is_ascii_alphabetic() => {}
            _ => return false,
        }

        chars.all(|c| c == '_' || c.is_ascii_alphabetic() || c.is_ascii_digit())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceValidateSpec for VariableSetSpecInput {
    type ValidationError = VariableSetSpecValidationError;

    fn validate(&self) -> Result<(), Self::ValidationError> {
        let entries = &self.variables.entries;

        if entries.is_empty() {
            return Err(VariableSetSpecValidationError::EmptyVariables);
        }

        if entries.len() > Self::MAX_VARIABLES {
            return Err(VariableSetSpecValidationError::TooManyVariables {
                actual: entries.len(),
                max: Self::MAX_VARIABLES,
            });
        }

        for (name, variable) in entries {
            let value = variable.value.as_str();

            if !Self::is_valid_variable_name(name) {
                return Err(VariableSetSpecValidationError::InvalidVariableName {
                    name: name.clone(),
                });
            }

            if value.is_empty() {
                return Err(VariableSetSpecValidationError::EmptyVariableValue {
                    name: name.clone(),
                });
            }

            if value.len() > Self::MAX_VARIABLE_VALUE_LEN {
                return Err(VariableSetSpecValidationError::VariableValueTooLong {
                    name: name.clone(),
                    actual: value.len(),
                    max: Self::MAX_VARIABLE_VALUE_LEN,
                });
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceLinterSpec for VariableSetSpecInput {
    fn lint_warnings(&self) -> Vec<ResourceWarning> {
        let mut warnings = Vec::new();

        for (name, variable) in &self.variables.entries {
            let value = variable.value.as_str();

            if name.starts_with(Self::RESERVED_VARIABLE_PREFIX) {
                warnings.push(ResourceWarning {
                    code: Self::WARNING_CODE_RESERVED_VARIABLE_PREFIX.to_string(),
                    path: Some(format!("spec.variables.{name}")),
                    message: format!(
                        "Variable '{name}' uses reserved '{prefix}' prefix",
                        prefix = Self::RESERVED_VARIABLE_PREFIX
                    ),
                });
            }

            if name.chars().any(|c| c.is_ascii_lowercase()) {
                warnings.push(ResourceWarning {
                    code: Self::WARNING_CODE_LOWERCASE_VARIABLE_NAME.to_string(),
                    path: Some(format!("spec.variables.{name}")),
                    message: format!(
                        "Variable '{name}' uses lowercase letters; prefer uppercase names like \
                         '{}'",
                        name.to_uppercase()
                    ),
                });
            }

            if value.len() > Self::WARNING_VARIABLE_VALUE_LEN {
                warnings.push(ResourceWarning {
                    code: Self::WARNING_CODE_LONG_VARIABLE_VALUE.to_string(),
                    path: Some(format!("spec.variables.{name}.value")),
                    message: format!(
                        "Variable '{name}' value is unusually long: got {actual}, warning \
                         threshold is {threshold}",
                        actual = value.len(),
                        threshold = Self::WARNING_VARIABLE_VALUE_LEN
                    ),
                });
            }
        }

        warnings
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum VariableSetSpecValidationError {
    #[error("variable set must contain at least one variable")]
    EmptyVariables,

    #[error("too many variables: got {actual}, max is {max}")]
    TooManyVariables { actual: usize, max: usize },

    #[error("invalid variable name '{name}': expected regex ^[A-Za-z_][A-Za-z0-9_]*$")]
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

#[cfg(test)]
mod tests {
    use super::{Variable, VariableSetSpec, VariableSetSpecInput};

    fn make_spec(
        entries: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> VariableSetSpec {
        VariableSetSpec(odf::metadata::config::VariableSetSpec {
            variables: make_variables(entries),
        })
    }

    fn make_spec_input(
        entries: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> VariableSetSpecInput {
        VariableSetSpecInput(odf::metadata::config::VariableSetSpecInput {
            variables: make_variables(entries),
        })
    }

    fn make_variables(
        entries: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> odf::metadata::config::Variables {
        odf::metadata::config::Variables {
            entries: entries
                .into_iter()
                .map(|(name, value)| {
                    (
                        name.into(),
                        Variable {
                            value: value.into(),
                        },
                    )
                })
                .collect(),
        }
    }

    #[test]
    fn deserializes_scalar_variable_syntax() {
        let spec: VariableSetSpec = serde_json::from_value(serde_json::json!({
            "variables": {
                "INPUT_TOPIC": "analytics.events",
            }
        }))
        .unwrap();

        assert_eq!(spec, make_spec([("INPUT_TOPIC", "analytics.events")]));
    }

    #[test]
    fn deserializes_structured_variable_syntax() {
        let spec: VariableSetSpec = serde_json::from_value(serde_json::json!({
            "variables": {
                "INPUT_TOPIC": {
                    "value": "analytics.events",
                },
            }
        }))
        .unwrap();

        assert_eq!(spec, make_spec([("INPUT_TOPIC", "analytics.events")]));
    }

    #[test]
    fn serializes_variable_as_structured_syntax() {
        let value = serde_json::to_value(make_spec([("INPUT_TOPIC", "analytics.events")])).unwrap();

        assert_eq!(
            value,
            serde_json::json!({
                "variables": {
                    "INPUT_TOPIC": {
                        "value": "analytics.events",
                    },
                }
            })
        );
    }

    #[test]
    fn lints_reserved_prefix_warning() {
        use kamu_resources::ResourceLinterSpec;

        let spec = make_spec_input([("KAMU_INTERNAL", "value")]);

        let warnings = spec.lint_warnings();
        assert_eq!(warnings.len(), 1);
        assert_eq!(
            warnings[0].code,
            VariableSetSpecInput::WARNING_CODE_RESERVED_VARIABLE_PREFIX
        );
        assert_eq!(
            warnings[0].path,
            Some("spec.variables.KAMU_INTERNAL".to_string())
        );
    }

    #[test]
    fn lints_lowercase_name_warning() {
        use kamu_resources::ResourceLinterSpec;

        let spec = make_spec_input([("my_variable", "value")]);

        let warnings = spec.lint_warnings();
        assert_eq!(warnings.len(), 1);
        assert_eq!(
            warnings[0].code,
            VariableSetSpecInput::WARNING_CODE_LOWERCASE_VARIABLE_NAME
        );
        assert_eq!(
            warnings[0].path,
            Some("spec.variables.my_variable".to_string())
        );
        assert!(warnings[0].message.contains("MY_VARIABLE"));
    }

    #[test]
    fn lints_long_value_warning() {
        use kamu_resources::ResourceLinterSpec;

        let long_value = "x".repeat(VariableSetSpecInput::WARNING_VARIABLE_VALUE_LEN + 1);
        let spec = make_spec_input([("CONFIG_VALUE", long_value)]);

        let warnings = spec.lint_warnings();
        assert_eq!(warnings.len(), 1);
        assert_eq!(
            warnings[0].code,
            VariableSetSpecInput::WARNING_CODE_LONG_VARIABLE_VALUE
        );
        assert_eq!(
            warnings[0].path,
            Some("spec.variables.CONFIG_VALUE.value".to_string())
        );
    }

    #[test]
    fn lints_multiple_warnings() {
        use kamu_resources::ResourceLinterSpec;

        let long_value = "x".repeat(VariableSetSpecInput::WARNING_VARIABLE_VALUE_LEN + 1);
        let spec = make_spec_input([
            ("KAMU_CONFIG".to_string(), "short".to_string()),
            ("my_var".to_string(), long_value),
        ]);

        let warnings = spec.lint_warnings();
        assert_eq!(warnings.len(), 3);
        assert_eq!(
            warnings
                .iter()
                .filter(|w| w.code == VariableSetSpecInput::WARNING_CODE_RESERVED_VARIABLE_PREFIX)
                .count(),
            1
        );
        assert_eq!(
            warnings
                .iter()
                .filter(|w| w.code == VariableSetSpecInput::WARNING_CODE_LOWERCASE_VARIABLE_NAME)
                .count(),
            1
        );
        assert_eq!(
            warnings
                .iter()
                .filter(|w| w.code == VariableSetSpecInput::WARNING_CODE_LONG_VARIABLE_VALUE)
                .count(),
            1
        );
    }

    #[test]
    fn lints_no_warnings_for_valid_variable() {
        use kamu_resources::ResourceLinterSpec;

        let spec = make_spec_input([("INPUT_TOPIC", "analytics.events")]);

        let warnings = spec.lint_warnings();
        assert_eq!(warnings.len(), 0);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
