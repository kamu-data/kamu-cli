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
// The manifest envelope (`$schema`/`headers.name`/`spec`) mirrors
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
// Canonical schemas (sourced from the ODF codegen — the single source of truth)
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const VARIABLE_SET_SCHEMA: &str = odf::metadata::config::VariableSet::schema_str();
pub const SECRET_SET_SCHEMA: &str = odf::metadata::config::SecretSet::schema_str();

/// Short CRD-style type name (the `Type` column in `list`/`summary` output),
/// as opposed to [`VARIABLE_SET_SCHEMA`]/[`SECRET_SET_SCHEMA`] which are the
/// full canonical `$schema` URIs.
pub const VARIABLE_SET_SHORT_NAME: &str = "VariableSet";
pub const SECRET_SET_SHORT_NAME: &str = "SecretSet";

/// Kamu config enabling secrets encryption. `SecretSet` apply encrypts values
/// via the configured key, so any scenario that applies a `SecretSet` must run
/// with this config (passed to the harness via `Options::with_kamu_config`).
/// Without it, the CLI panics in the secret-set sanitizer. The key is the
/// 32-char sample from `kamu_datasets::SAMPLE_SECRETS_ENCRYPTION_KEY`.
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

/// Default `description` annotation baked into the well-formed builders. A
/// manifest without a description applies successfully but emits a
/// `missing_description` lint warning; the canonical fixtures include one so
/// the common lifecycle scenarios stay warning-free. The warning path itself is
/// covered explicitly by [`variable_set_manifest_no_description`].
pub const DEFAULT_DESCRIPTION: &str = "e2e test fixture resource";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Builders
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A well-formed `VariableSet` manifest in YAML with a single `MESSAGE`
/// variable and a `description` annotation (so it applies without lint
/// warnings).
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

/// Same as [`variable_set_manifest_yaml`] but carries a populated
/// `headers.labels`/`headers.annotations` block — a short `TypeName` label
/// key, a full `TypeUri` label key with a nested-object value, and two
/// annotations (the well-known `description` plus an `owner`) — to pin the
/// ODF `TypeRef`-keyed canonical shape end-to-end.
pub fn variable_set_manifest_yaml_with_labels(name: &str, value: &str) -> String {
    indoc::formatdoc!(
        r#"
        $schema: {VARIABLE_SET_SCHEMA}
        headers:
          name: {name}
          labels:
            env: prod
            https://opendatafabric.org/schemas/labels/v1/Team:
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

/// A well-formed `VariableSet` manifest in YAML that targets a specific account
/// by name via the `headers.account.name` field — the GitOps-style path where
/// a manifest names the account it belongs to (mirrors `ResourceAccountRef
/// { name, id }`, here using the by-name form). Used by the multi-tenant
/// scenarios to prove the CLI honors the manifest's account selector, including
/// the mismatch/unknown rejection paths.
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

/// A `VariableSet` manifest **without** a `description` annotation. Applies
/// successfully but emits the `missing_description` lint warning — used to
/// cover the warning surface explicitly.
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

/// A well-formed `SecretSet` manifest in YAML with two secret entries and a
/// `description` (so it applies without lint warnings).
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

/// Plaintext that [`PRE_ENCRYPTED_API_TOKEN_JWE`] decrypts back to. A scenario
/// that applies the pre-encrypted manifest can reveal it and assert this value.
pub const PRE_ENCRYPTED_API_TOKEN_PLAINTEXT: &str = "super-secret-token";

/// A compact JWE token (`contentEncoding: "jwe"`) for
/// [`PRE_ENCRYPTED_API_TOKEN_PLAINTEXT`], as a GitOps-committed manifest would
/// carry an already-encrypted secret.
///
/// The token is tied to the encryption key in
/// [`SECRETS_ENCRYPTION_KAMU_CONFIG`] (`QfnEDcnUtGSW2pwVXaFPvZOwxyFm2BOC`, i.e.
/// `kamu_datasets::SAMPLE_SECRETS_ENCRYPTION_KEY`). It MUST be applied with
/// that same config, otherwise `--revealed` would fail to decrypt.
///
/// How the token was produced (reproduce if the sample key ever changes): build
/// a `crypto_utils::SecretCryptor` from the sample key and call
/// `encrypt_to_jwe(b"super-secret-token")`. The JWE IV is random per call, so
/// re-running yields a different token that still decrypts to the same
/// plaintext — any such token is valid.
pub const PRE_ENCRYPTED_API_TOKEN_JWE: &str = "eyJhbGciOiJkaXIiLCJlbmMiOiJBMjU2R0NNIn0..\
                                               xqDKWewaviCEWvPB.-hVKByad54NGfdcYuA0lyGPm.\
                                               vy4bkibgH2ZgDXVsFxDUmw";

/// A `SecretSet` manifest whose single secret is supplied **pre-encrypted** (as
/// a JWE token, see [`PRE_ENCRYPTED_API_TOKEN_JWE`]). Applying it exercises the
/// already-encrypted apply path — the sanitizer must accept the encrypted value
/// as-is rather than re-encrypting it. Must be applied with
/// [`SECRETS_ENCRYPTION_KAMU_CONFIG`].
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

/// A syntactically valid `VariableSet` manifest that fails business validation
/// (empty `variables` map). Useful for batch / error scenarios.
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
