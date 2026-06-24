// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Manifest fixtures for resource CLI e2e tests.
//
// The manifest envelope (`apiVersion`/`kind`/`metadata.name`/`spec`) mirrors
// `kamu_resources::ResourceManifest`. The specs mirror
// `kamu_configuration::{VariableSetSpec, SecretSetSpec}`:
//   - VariableSet spec: `{ variables: { NAME: "value" } }` (scalar) or `{
//     variables: { NAME: { value: "value" } } }` (structured).
//   - SecretSet spec:   `{ secrets:   { NAME: { value: "value" } } }`.
//
// Variable/secret names must match `^[A-Za-z_][A-Za-z0-9_]*$`; uppercase is
// preferred (lowercase only triggers a lint warning). An empty `variables`
// map deserializes fine but fails business validation — used for the invalid
// fixture below.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Canonical kinds / api version (kept in sync with the domain constants)
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const RESOURCE_API_VERSION: &str = "kamu.dev/v1alpha1";
pub const VARIABLE_SET_KIND: &str = "VariableSet";
pub const SECRET_SET_KIND: &str = "SecretSet";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Builders
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A `VariableSet` manifest in YAML with a single `MESSAGE` variable.
pub fn variable_set_manifest_yaml(name: &str, value: &str) -> String {
    indoc::formatdoc!(
        r#"
        apiVersion: {RESOURCE_API_VERSION}
        kind: {VARIABLE_SET_KIND}
        metadata:
          name: {name}
        spec:
          variables:
            MESSAGE: {value}
        "#
    )
}

/// The same `VariableSet` manifest as [`variable_set_manifest_yaml`] but in
/// JSON.
pub fn variable_set_manifest_json(name: &str, value: &str) -> String {
    serde_json::json!({
        "apiVersion": RESOURCE_API_VERSION,
        "kind": VARIABLE_SET_KIND,
        "metadata": { "name": name },
        "spec": { "variables": { "MESSAGE": value } },
    })
    .to_string()
}

/// A `SecretSet` manifest in YAML with two secret entries.
pub fn secret_set_manifest_yaml(name: &str, token: &str, password: &str) -> String {
    indoc::formatdoc!(
        r#"
        apiVersion: {RESOURCE_API_VERSION}
        kind: {SECRET_SET_KIND}
        metadata:
          name: {name}
        spec:
          secrets:
            API_TOKEN:
              value: {token}
            DB_PASSWORD:
              value: {password}
        "#
    )
}

/// The same `SecretSet` manifest as [`secret_set_manifest_yaml`] but in JSON.
pub fn secret_set_manifest_json(name: &str, token: &str, password: &str) -> String {
    serde_json::json!({
        "apiVersion": RESOURCE_API_VERSION,
        "kind": SECRET_SET_KIND,
        "metadata": { "name": name },
        "spec": { "secrets": {
            "API_TOKEN": { "value": token },
            "DB_PASSWORD": { "value": password },
        } },
    })
    .to_string()
}

/// A syntactically valid `VariableSet` manifest that fails business validation
/// (empty `variables` map). Useful for batch / error scenarios.
pub fn variable_set_manifest_business_invalid(name: &str) -> String {
    indoc::formatdoc!(
        r#"
        apiVersion: {RESOURCE_API_VERSION}
        kind: {VARIABLE_SET_KIND}
        metadata:
          name: {name}
        spec:
          variables: {{}}
        "#
    )
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
