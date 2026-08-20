// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Canonical schemas
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const VARIABLE_SET_SCHEMA: &str = odf::metadata::config::VariableSet::schema_str();
pub const SECRET_SET_SCHEMA: &str = odf::metadata::config::SecretSet::schema_str();

/// Short CRD-style type name used in `list`/`summary` output.
pub const VARIABLE_SET_SHORT_NAME: &str = "VariableSet";
pub const SECRET_SET_SHORT_NAME: &str = "SecretSet";
pub const DESCRIPTION_ANNOTATION_SCHEMA: &str =
    "https://kamu.dev/schemas/resource/v1alpha1/annotations/Description";
/// Canonical schema URI for the built-in `environment` label.
pub const ENVIRONMENT_LABEL_SCHEMA: &str =
    "https://kamu.dev/schemas/resource/v1alpha1/labels/Environment";

/// Kamu config required by scenarios that apply `SecretSet` manifests.
pub const SECRETS_ENCRYPTION_KAMU_CONFIG: &str = indoc::indoc!(
    r#"
    kind: CLIConfig
    version: 1
    content:
      secretsEncryption:
        enabled: true
        encryptionKey: QfnEDcnUtGSW2pwVXaFPvZOwxyFm2BOC
    "#
);

/// Default `description` annotation for warning-free fixtures.
pub const DEFAULT_DESCRIPTION: &str = "e2e test fixture resource";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Builders
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A well-formed `VariableSet` manifest in YAML.
pub fn variable_set_manifest_yaml(name: &str, value: &str) -> String {
    indoc::formatdoc!(
        r#"
        $schema: {VARIABLE_SET_SCHEMA}
        headers:
          name: {name}
          annotations:
            description: {DEFAULT_DESCRIPTION}
        spec:
          variables:
            MESSAGE: {value}
        "#
    )
}

/// Same as [`variable_set_manifest_yaml`] with labels and annotations.
pub fn variable_set_manifest_yaml_with_labels(name: &str, value: &str) -> String {
    indoc::formatdoc!(
        r#"
        $schema: {VARIABLE_SET_SCHEMA}
        headers:
          name: {name}
          labels:
            env: prod
            team:
              name: data-platform
          annotations:
            description: {DEFAULT_DESCRIPTION}
            owner: https://github.com/open-data-fabric
        spec:
          variables:
            MESSAGE: {value}
        "#
    )
}

/// A `VariableSet` manifest with several labels and variables, for exercising
/// apply-diff rendering across multiple independent regions.
///
/// Every field is caller-controlled so a test can change exactly one of them
/// and assert that the rendered diff stays proportional to that change.
pub fn variable_set_manifest_yaml_rich(
    name: &str,
    team: &str,
    tier: &str,
    variables: &[(&str, &str)],
) -> String {
    // Quoted so numeric-looking values stay strings in YAML.
    let variables = variables.iter().fold(String::new(), |mut acc, (k, v)| {
        use std::fmt::Write;
        writeln!(acc, "    {k}: \"{v}\"").unwrap();
        acc
    });

    indoc::formatdoc!(
        r#"
        $schema: {VARIABLE_SET_SCHEMA}
        headers:
          name: {name}
          labels:
            env: prod
            team: {team}
            tier: {tier}
          annotations:
            description: {DEFAULT_DESCRIPTION}
        spec:
          variables:
        {variables}"#
    )
}

/// The same `VariableSet` manifest as [`variable_set_manifest_yaml`] but in
/// JSON.
pub fn variable_set_manifest_json(name: &str, value: &str) -> String {
    serde_json::json!({
        "$schema": VARIABLE_SET_SCHEMA,
        "headers": { "name": name, "annotations": { "description": DEFAULT_DESCRIPTION } },
        "spec": { "variables": { "MESSAGE": value } },
    })
    .to_string()
}

/// A `VariableSet` manifest targeting a specific account by name.
pub fn variable_set_manifest_yaml_for_account(
    name: &str,
    value: &str,
    account_name: &str,
) -> String {
    indoc::formatdoc!(
        r#"
        $schema: {VARIABLE_SET_SCHEMA}
        headers:
          name: {name}
          annotations:
            description: {DEFAULT_DESCRIPTION}
          account:
            name: {account_name}
        spec:
          variables:
            MESSAGE: {value}
        "#
    )
}

/// A `VariableSet` manifest without a `description` annotation.
pub fn variable_set_manifest_no_description(name: &str, value: &str) -> String {
    indoc::formatdoc!(
        r#"
        $schema: {VARIABLE_SET_SCHEMA}
        headers:
          name: {name}
        spec:
          variables:
            MESSAGE: {value}
        "#
    )
}

/// A `VariableSet` manifest that is otherwise warning-free but files a
/// credential-shaped variable, tripping the `secret_material_in_variable`
/// lint. Applies successfully — the warning is advisory.
pub fn variable_set_manifest_secret_material(name: &str) -> String {
    indoc::formatdoc!(
        r#"
        $schema: {VARIABLE_SET_SCHEMA}
        headers:
          name: {name}
          annotations:
            description: {DEFAULT_DESCRIPTION}
        spec:
          variables:
            DB_PASSWORD: hunter2
        "#
    )
}

/// A well-formed `SecretSet` manifest in YAML.
pub fn secret_set_manifest_yaml(name: &str, token: &str, password: &str) -> String {
    indoc::formatdoc!(
        r#"
        $schema: {SECRET_SET_SCHEMA}
        headers:
          name: {name}
          annotations:
            description: {DEFAULT_DESCRIPTION}
        spec:
          secrets:
            API_TOKEN:
              value: {token}
            DB_PASSWORD:
              value: {password}
        "#
    )
}

/// Plaintext that [`PRE_ENCRYPTED_API_TOKEN_JWE`] decrypts back to.
pub const PRE_ENCRYPTED_API_TOKEN_PLAINTEXT: &str = "super-secret-token";

/// A compact JWE token for [`PRE_ENCRYPTED_API_TOKEN_PLAINTEXT`].
/// Must be applied with [`SECRETS_ENCRYPTION_KAMU_CONFIG`].
pub const PRE_ENCRYPTED_API_TOKEN_JWE: &str = "eyJhbGciOiJkaXIiLCJlbmMiOiJBMjU2R0NNIn0..\
                                               xqDKWewaviCEWvPB.-hVKByad54NGfdcYuA0lyGPm.\
                                               vy4bkibgH2ZgDXVsFxDUmw";

/// A `SecretSet` manifest whose single secret is supplied pre-encrypted.
pub fn secret_set_manifest_pre_encrypted_yaml(name: &str) -> String {
    indoc::formatdoc!(
        r#"
        $schema: {SECRET_SET_SCHEMA}
        headers:
          name: {name}
          annotations:
            description: {DEFAULT_DESCRIPTION}
        spec:
          secrets:
            API_TOKEN:
              value: {token}
              contentEncoding: jwe
        "#,
        token = PRE_ENCRYPTED_API_TOKEN_JWE,
    )
}

/// A `SecretSet` manifest carrying the built-in `environment` label.
pub fn secret_set_manifest_with_environment_label(
    name: &str,
    value: &str,
    label_key: &str,
) -> String {
    indoc::formatdoc!(
        r#"
        $schema: {SECRET_SET_SCHEMA}
        headers:
          name: {name}
          labels:
            {label_key}: {value}
          annotations:
            description: {DEFAULT_DESCRIPTION}
        spec:
          secrets:
            API_TOKEN:
              value: token-{name}
        "#
    )
}

/// The same `SecretSet` manifest as [`secret_set_manifest_yaml`] but in JSON.
pub fn secret_set_manifest_json(name: &str, token: &str, password: &str) -> String {
    serde_json::json!({
        "$schema": SECRET_SET_SCHEMA,
        "headers": { "name": name, "annotations": { "description": DEFAULT_DESCRIPTION } },
        "spec": { "secrets": {
            "API_TOKEN": { "value": token },
            "DB_PASSWORD": { "value": password },
        } },
    })
    .to_string()
}

/// A syntactically valid `VariableSet` manifest that fails business validation.
pub fn variable_set_manifest_business_invalid(name: &str) -> String {
    indoc::formatdoc!(
        r#"
        $schema: {VARIABLE_SET_SCHEMA}
        headers:
          name: {name}
        spec:
          variables: {{}}
        "#
    )
}

/// A `VariableSet` manifest carrying the built-in `environment` label.
pub fn variable_set_manifest_with_environment_label(
    name: &str,
    value: &str,
    label_key: &str,
) -> String {
    indoc::formatdoc!(
        r#"
        $schema: {VARIABLE_SET_SCHEMA}
        headers:
          name: {name}
          labels:
            {label_key}: {value}
          annotations:
            description: {DEFAULT_DESCRIPTION}
        spec:
          variables:
            MESSAGE: value
        "#
    )
}

/// A `VariableSet` manifest carrying `environment` and free-form `team` labels.
pub fn variable_set_manifest_with_environment_and_team_labels(
    name: &str,
    environment: &str,
    team: &str,
) -> String {
    indoc::formatdoc!(
        r#"
        $schema: {VARIABLE_SET_SCHEMA}
        headers:
          name: {name}
          labels:
            environment: {environment}
            team: {team}
          annotations:
            description: {DEFAULT_DESCRIPTION}
        spec:
          variables:
            MESSAGE: value
        "#
    )
}

/// A `VariableSet` manifest carrying an unregistered URI label key.
pub fn variable_set_manifest_with_unknown_label_uri(name: &str) -> String {
    indoc::formatdoc!(
        r#"
        $schema: {VARIABLE_SET_SCHEMA}
        headers:
          name: {name}
          labels:
            https://kamu.dev/schemas/resource/v1alpha1/labels/NotRegistered: value
        spec:
          variables:
            MESSAGE: value
        "#
    )
}

/// A `VariableSet` manifest with an overlong `description` annotation.
pub fn variable_set_manifest_with_overlong_description(name: &str) -> String {
    let description = "x".repeat(4097);
    indoc::formatdoc!(
        r#"
        $schema: {VARIABLE_SET_SCHEMA}
        headers:
          name: {name}
          annotations:
            description: {description}
        spec:
          variables:
            MESSAGE: value
        "#
    )
}

/// A `VariableSet` manifest carrying an unregistered short-name label.
pub fn variable_set_manifest_with_freeform_label(name: &str) -> String {
    indoc::formatdoc!(
        r#"
        $schema: {VARIABLE_SET_SCHEMA}
        headers:
          name: {name}
          labels:
            team: data-platform
          annotations:
            description: {DEFAULT_DESCRIPTION}
        spec:
          variables:
            MESSAGE: value
        "#
    )
}

/// A `VariableSet` manifest carrying a structured label value.
pub fn variable_set_manifest_with_structured_label(name: &str) -> String {
    indoc::formatdoc!(
        r#"
        $schema: {VARIABLE_SET_SCHEMA}
        headers:
          name: {name}
          labels:
            coordinates:
              lat: 1
              lon: 2
          annotations:
            description: {DEFAULT_DESCRIPTION}
        spec:
          variables:
            MESSAGE: value
        "#
    )
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
