// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_configuration::{SecretSetResource, VariableSetResource};
use kamu_resources::{ApplyResourceOutcome, ResourceID};
use kamu_resources_facade::{ApplyManifestRequest, ResourceManifestFormat};

use crate::harness::{FacadeContractHarness, TestAccount};
use crate::helpers::assert_applied_outcome;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const VARIABLE_SET_KIND: &str = VariableSetResource::RESOURCE_NAME;
pub const VARIABLE_SET_SCHEMA: &str = VariableSetResource::SCHEMA;

pub const SECRET_SET_KIND: &str = SecretSetResource::RESOURCE_NAME;
pub const SECRET_SET_SCHEMA: &str = SecretSetResource::SCHEMA;

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
    indoc::formatdoc!(
        r#"
        {{
            "$schema": "{VARIABLE_SET_SCHEMA}",
            "headers": {{"name": "{name}"}},
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
    // Each variable entry must appear at 4-space indent under `variables:`.
    // The outer formatdoc! strips the 8-space common indent from the template
    // body, but {variables_section} is a string interpolation — its content is
    // NOT re-indented by formatdoc!.  We therefore pre-indent each line by the
    // exact 4 spaces we want in the final YAML output.
    let variables_section: String = vars
        .iter()
        .map(|(k, v)| format!("    {k}:\n      value: {v}"))
        .collect::<Vec<_>>()
        .join("\n");
    indoc::formatdoc!(
        "
        $schema: {VARIABLE_SET_SCHEMA}
        headers:
          name: {name}
        {account_section}spec:
          variables:
        {variables_section}
        "
    )
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn sorted_identity_names(mut items: Vec<kamu_resources::ResourceIdentityView>) -> Vec<String> {
    crate::helpers::normalize_identity_views(&mut items);
    items
        .into_iter()
        .map(|item| item.name.to_string())
        .collect()
}

pub fn total_kind_count(summary: kamu_resources::ResourcesSummary, kind: &str) -> u64 {
    summary
        .resource_counts
        .into_iter()
        .find(|count| count.schema == kind)
        .map_or(0, |count| count.total_count)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn apply_manifest_and_get_id(
    h: &impl FacadeContractHarness,
    account: TestAccount,
    manifest: String,
) -> ResourceID {
    let decision = h
        .facade_for(account)
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest,
        })
        .await
        .unwrap();
    let view = assert_applied_outcome(&decision, ApplyResourceOutcome::Created);
    view.headers.id
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Builds a `SecretSet` manifest JSON string.
///
/// Each secret is emitted as `{"value": "<v>"}` — the untagged `Value`
/// variant of `SecretSpec`.
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
    indoc::formatdoc!(
        r#"
        {{
            "$schema": "{SECRET_SET_SCHEMA}",
            "headers": {{"name": "{name}"}},
            {account_field}"spec": {{
                "secrets": {{
                    {entries}
                }}
            }}
        }}"#
    )
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
