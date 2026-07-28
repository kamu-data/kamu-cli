// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_configuration::{SecretSetResource, VariableSetResource};
use kamu_resources::{ApplyResourceOutcome, ResourceID, ResourceLabelFilterInput, TypeUri};
use kamu_resources_facade::{ApplyManifestRequest, ResourceManifestFormat};

use crate::harness::{FacadeContractHarness, TestAccount};
use crate::helpers::assert_applied_outcome;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const VARIABLE_SET_CANONICAL_SELECTOR: &str = VariableSetResource::CANONICAL_SELECTOR_NAME_STR;
pub const VARIABLE_SET_SCHEMA_STR: &str = VariableSetResource::SCHEMA_STR;

pub const SECRET_SET_CANONICAL_SELECTOR: &str = SecretSetResource::CANONICAL_SELECTOR_NAME_STR;
pub const SECRET_SET_SCHEMA_STR: &str = SecretSetResource::SCHEMA_STR;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Builds a `VariableSet` manifest JSON string.
///
/// Each variable is emitted as `{"value": "<v>"}` — the structured form of
/// the RFC-derived `Variable` shape.
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
            "$schema": "{VARIABLE_SET_SCHEMA_STR}",
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
        $schema: {VARIABLE_SET_SCHEMA_STR}
        headers:
          name: {name}
        {account_section}spec:
          variables:
        {variables_section}
        "
    )
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn sorted_handle_names(mut items: Vec<kamu_resources::ResourceHandle>) -> Vec<String> {
    crate::helpers::normalize_handles(&mut items);
    items
        .into_iter()
        .map(|item| item.name.to_string())
        .collect()
}

pub fn total_schema_count(summary: kamu_resources::ResourcesSummary, schema: &TypeUri) -> u64 {
    summary
        .resource_counts
        .into_iter()
        .find(|count| count.schema == *schema)
        .map_or(0, |count| count.total_count)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Applies a `VariableSet` manifest authoring the given labels and returns its
/// id. `label_values` are embedded as raw JSON, so structured (non-string)
/// values can be exercised too.
pub async fn create_variable_set_with_labels(
    h: &impl FacadeContractHarness,
    account: TestAccount,
    name: &str,
    labels: &[(&str, serde_json::Value)],
) -> ResourceID {
    let labels_json: serde_json::Map<_, _> = labels
        .iter()
        .map(|(k, v)| ((*k).to_string(), v.clone()))
        .collect();
    let manifest = serde_json::json!({
        "$schema": VARIABLE_SET_SCHEMA_STR,
        "headers": {
            "name": name,
            "labels": labels_json,
        },
        "spec": {"variables": {"K": {"value": "v"}}}
    })
    .to_string();

    apply_manifest_and_get_id(h, account, manifest).await
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Builds a `ResourceLabelFilterInput` from string-valued entries.
pub fn label_filter(entries: &[(&str, &str)]) -> ResourceLabelFilterInput {
    ResourceLabelFilterInput {
        entries: entries
            .iter()
            .map(|(k, v)| {
                (
                    (*k).to_string(),
                    serde_json::Value::String((*v).to_string()),
                )
            })
            .collect(),
    }
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
/// Each secret is emitted as `{"value": "<v>"}` — the structured,
/// non-encrypted form of `odf::metadata::config::Secret`.
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
            "$schema": "{SECRET_SET_SCHEMA_STR}",
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
