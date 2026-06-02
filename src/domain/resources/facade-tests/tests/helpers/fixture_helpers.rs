// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_configuration::{SecretSetResource, VariableSetResource};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const VARIABLE_SET_KIND: &str = VariableSetResource::RESOURCE_TYPE;
pub const VARIABLE_SET_API_VERSION: &str = VariableSetResource::API_VERSION;

pub const SECRET_SET_KIND: &str = SecretSetResource::RESOURCE_TYPE;
pub const SECRET_SET_API_VERSION: &str = SecretSetResource::API_VERSION;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Builds a `VariableSet` manifest JSON string.
///
/// Each variable is emitted as `{"value": "<v>"}` — the untagged `Value`
/// variant of `VariableSpec`.
pub fn variable_set_manifest_json(
    name: &str,
    account: Option<&str>,
    vars: &[(&str, &str)],
) -> String {
    let account_field = match account {
        Some(a) => indoc::formatdoc!(
            r#"
            "account": {{"name": "{a}"}},
            "#
        ),
        None => String::new(),
    };
    let variables: String = vars
        .iter()
        .map(|(k, v)| {
            indoc::formatdoc!(
                r#"
                    "{k}": {{"value": "{v}"}}
                "#
            )
        })
        .collect::<Vec<_>>()
        .join(",\n");
    let api = VARIABLE_SET_API_VERSION;
    let kind = VARIABLE_SET_KIND;
    indoc::formatdoc!(
        r#"
        {{
            "apiVersion": "{api}",
            "kind": "{kind}",
            "metadata": {{"name": "{name}"}},
            {account_field}"spec": {{
                "variables": {{
                    {variables}
                }}
            }}
        }}"#
    )
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Builds a `VariableSet` manifest YAML string.
#[expect(dead_code)]
pub fn variable_set_manifest_yaml(
    name: &str,
    account: Option<&str>,
    vars: &[(&str, &str)],
) -> String {
    let account_section = match account {
        Some(a) => indoc::formatdoc!(
            "
            account:
              name: {a}
            "
        ),
        None => String::new(),
    };
    let variables_section: String = vars
        .iter()
        .map(|(k, v)| {
            indoc::formatdoc!(
                "
                    {k}:
                      value: {v}"
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    let api = VARIABLE_SET_API_VERSION;
    let kind = VARIABLE_SET_KIND;
    indoc::formatdoc!(
        "
        apiVersion: {api}
        kind: {kind}
        metadata:
          name: {name}
        {account_section}spec:
          variables:
        {variables_section}
        "
    )
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Builds a `SecretSet` manifest JSON string.
///
/// Each secret is emitted as `{"value": "<v>"}` — the untagged `Value`
/// variant of `SecretSpec`.
#[expect(dead_code)]
pub fn secret_set_manifest_json(
    name: &str,
    account: Option<&str>,
    secrets: &[(&str, &str)],
) -> String {
    let account_field = match account {
        Some(a) => indoc::formatdoc!(
            r#"
            "account": {{"name": "{a}"}},
            "#
        ),
        None => String::new(),
    };
    let entries: String = secrets
        .iter()
        .map(|(k, v)| {
            indoc::formatdoc!(
                r#"
                    "{k}": {{"value": "{v}"}}
                "#
            )
        })
        .collect::<Vec<_>>()
        .join(",\n");
    let api = SECRET_SET_API_VERSION;
    let kind = SECRET_SET_KIND;
    indoc::formatdoc!(
        r#"
        {{
            "apiVersion": "{api}",
            "kind": "{kind}",
            "metadata": {{"name": "{name}"}},
            {account_field}"spec": {{
                "secrets": {{
                    {entries}
                }}
            }}
        }}"#
    )
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
