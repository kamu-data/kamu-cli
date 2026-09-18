// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::{ResourceLinterSpec, ResourceValidateSpec, ResourceWarning};

use crate::resources::spec_lints;

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
    pub const WARNING_CODE_SECRET_MATERIAL_IN_VARIABLE: &str = "secret_material_in_variable";
    pub const WARNING_CODE_CASE_COLLIDING_NAMES: &str = "case_colliding_names";
    pub const WARNING_CODE_SUSPICIOUS_VALUE_WHITESPACE: &str = "suspicious_value_whitespace";
    pub const WARNING_CODE_UNEXPANDED_INTERPOLATION: &str = "unexpanded_interpolation";

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

        // A collision spans two entries, so it is resolved once up front
        // rather than per entry.
        let collisions = spec_lints::CaseCollisions::scan(&self.variables.entries);

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

            // Unlike `SecretSet`, variable values are never encrypted, so a
            // credential filed here applies cleanly and then leaks.
            if spec_lints::CredentialShape::new(name, value).is_suspicious() {
                warnings.push(ResourceWarning {
                    code: Self::WARNING_CODE_SECRET_MATERIAL_IN_VARIABLE.to_string(),
                    path: Some(format!("spec.variables.{name}")),
                    message: format!(
                        "Variable '{name}' looks like a credential; variable values are stored \
                         unencrypted and are shown by 'get' — consider a SecretSet instead"
                    ),
                });
            }

            if let Some(other) = collisions.other(name) {
                warnings.push(ResourceWarning {
                    code: Self::WARNING_CODE_CASE_COLLIDING_NAMES.to_string(),
                    path: Some(format!("spec.variables.{name}")),
                    message: format!(
                        "Variable '{name}' differs only by case from '{other}'; they are stored \
                         as distinct variables and may collide wherever names are compared \
                         case-insensitively"
                    ),
                });
            }

            let value_shape = spec_lints::ValueShape::new(value);

            if value_shape.has_suspicious_whitespace() {
                warnings.push(ResourceWarning {
                    code: Self::WARNING_CODE_SUSPICIOUS_VALUE_WHITESPACE.to_string(),
                    path: Some(format!("spec.variables.{name}.value")),
                    message: format!(
                        "Variable '{name}' value has leading, trailing, or embedded whitespace; \
                         it is stored verbatim"
                    ),
                });
            }

            if value_shape.has_unexpanded_interpolation() {
                warnings.push(ResourceWarning {
                    code: Self::WARNING_CODE_UNEXPANDED_INTERPOLATION.to_string(),
                    path: Some(format!("spec.variables.{name}.value")),
                    message: format!(
                        "Variable '{name}' value contains interpolation syntax; specs are not \
                         templated and the value is stored literally"
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
    fn lints_secret_material_by_name() {
        use kamu_resources::ResourceLinterSpec;

        let spec = make_spec_input([("DB_PASSWORD", "hunter2")]);

        let warnings = spec.lint_warnings();
        assert_eq!(warnings.len(), 1);
        assert_eq!(
            warnings[0].code,
            VariableSetSpecInput::WARNING_CODE_SECRET_MATERIAL_IN_VARIABLE
        );
        assert_eq!(
            warnings[0].path,
            Some("spec.variables.DB_PASSWORD".to_string())
        );
        assert!(warnings[0].message.contains("SecretSet"));
    }

    #[test]
    fn lints_secret_material_by_value() {
        use kamu_resources::ResourceLinterSpec;

        // An innocuous name, but the value is unmistakably an AWS key ID.
        let spec = make_spec_input([("PIPELINE_INPUT", "AKIAIOSFODNN7EXAMPLE")]);

        let warnings = spec.lint_warnings();
        assert_eq!(warnings.len(), 1);
        assert_eq!(
            warnings[0].code,
            VariableSetSpecInput::WARNING_CODE_SECRET_MATERIAL_IN_VARIABLE
        );
        assert_eq!(
            warnings[0].path,
            Some("spec.variables.PIPELINE_INPUT".to_string())
        );
    }

    #[test]
    fn lints_case_colliding_names() {
        use kamu_resources::ResourceLinterSpec;

        let spec = make_spec_input([("DB_HOST", "a"), ("db_host", "b")]);

        let warnings = spec.lint_warnings();
        assert_eq!(warnings.len(), 1);
        assert_eq!(
            warnings[0].code,
            VariableSetSpecInput::WARNING_CODE_CASE_COLLIDING_NAMES
        );
        assert_eq!(warnings[0].path, Some("spec.variables.db_host".to_string()));
        assert!(warnings[0].message.contains("DB_HOST"));
    }

    #[test]
    fn lints_suspicious_value_whitespace() {
        use kamu_resources::ResourceLinterSpec;

        for value in [" leading", "trailing ", "embedded\nnewline"] {
            let spec = make_spec_input([("INPUT_TOPIC", value)]);

            let warnings = spec.lint_warnings();
            assert_eq!(warnings.len(), 1, "value: {value:?}");
            assert_eq!(
                warnings[0].code,
                VariableSetSpecInput::WARNING_CODE_SUSPICIOUS_VALUE_WHITESPACE
            );
            assert_eq!(
                warnings[0].path,
                Some("spec.variables.INPUT_TOPIC.value".to_string())
            );
        }
    }

    #[test]
    fn lints_unexpanded_interpolation() {
        use kamu_resources::ResourceLinterSpec;

        let spec = make_spec_input([("HOME_DIR", "${HOME}/data")]);

        let warnings = spec.lint_warnings();
        assert_eq!(warnings.len(), 1);
        assert_eq!(
            warnings[0].code,
            VariableSetSpecInput::WARNING_CODE_UNEXPANDED_INTERPOLATION
        );
        assert_eq!(
            warnings[0].path,
            Some("spec.variables.HOME_DIR.value".to_string())
        );
    }

    #[test]
    fn lints_no_warnings_for_mixed_case_name() {
        use kamu_resources::ResourceLinterSpec;

        // Mixed case is legal per `is_valid_variable_name` and carries no
        // consequence on its own, so it must not warn — this is the guard
        // against reintroducing a style-only casing lint.
        let spec = make_spec_input([("Http_Port", "8080")]);

        let warnings = spec.lint_warnings();
        assert_eq!(warnings.len(), 0);
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
        assert_eq!(warnings.len(), 2);
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
