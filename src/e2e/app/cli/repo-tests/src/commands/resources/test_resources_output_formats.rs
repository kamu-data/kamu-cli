// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::resources::{self, ResourceCtx, fixtures};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Scenario: resource output format sanity (QA scenario 9)
//
// This is intentionally a lightweight smoke test for parseability and stable
// structural fields. The full `get -o json` resource document is pinned by
// `test_resources_golden_view`; this scenario only proves each public format
// renders usable data.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_resources_output_formats(ctx: ResourceCtx) {
    let resource_name = "format-vars";
    let resource_value = "format-value";

    ctx.apply_variable_set(resource_name, resource_value).await;

    // `get -o json`: use the shared ResourceView layer, not ad-hoc document
    // parsing, because this is the normal `get` JSON resource-view shape.
    let json_view = ctx.get_one(["get", "vs", resource_name]).await;
    assert_eq!(
        json_view.ident(),
        (fixtures::VARIABLE_SET_KIND, resource_name)
    );
    assert_eq!(json_view.variable("MESSAGE"), Some(resource_value));

    // `get -o yaml`: parseable YAML with the same identity and one field under
    // test.
    let get_yaml = ctx
        .stdout_yaml_as_json(["get", "vs", resource_name, "-o", "yaml"])
        .await;
    assert_eq!(
        get_yaml
            .pointer("/$schema")
            .and_then(serde_json::Value::as_str),
        Some(fixtures::VARIABLE_SET_KIND)
    );
    assert_eq!(
        get_yaml
            .pointer("/headers/name")
            .and_then(serde_json::Value::as_str),
        Some(resource_name)
    );
    assert_eq!(
        get_yaml
            .pointer("/spec/variables/MESSAGE")
            .and_then(serde_json::Value::as_str),
        Some(resource_value)
    );

    // `get -o name`: one canonical resource reference per line.
    let get_name = ctx.stdout(["get", "vs", resource_name, "-o", "name"]).await;
    assert_eq!(
        get_name.trim(),
        format!("variablesets/{resource_name}"),
        "`get vs <name> -o name` should emit the canonical resource ref, got:\n{get_name}"
    );

    // `list all -o json`: parseable records array with a row for our resource.
    let list_json = ctx.stdout_json(["list", "all", "-o", "json"]).await;
    resources::assert_record_row(
        &list_json,
        "list all -o json",
        resource_name,
        fixtures::VARIABLE_SET_KIND,
    );

    // `list all -o csv`: header + row are present. We do not parse full CSV
    // because values asserted here are simple and unquoted.
    let list_csv = ctx.stdout(["list", "all", "-o", "csv"]).await;
    assert_csv_contains_resource(
        &list_csv,
        "list all -o csv",
        resource_name,
        fixtures::VARIABLE_SET_KIND,
    );

    // `summary -o json` / `summary -o yaml`: parseable and count the created
    // VariableSet.
    let summary_json = ctx.stdout_json(["summary", "-o", "json"]).await;
    assert_eq!(
        resources::summary_count(
            &summary_json,
            "summary -o json",
            fixtures::VARIABLE_SET_KIND
        ),
        1
    );

    let summary_yaml = ctx.stdout_yaml_as_json(["summary", "-o", "yaml"]).await;
    assert_eq!(
        resources::summary_count(
            &summary_yaml,
            "summary -o yaml",
            fixtures::VARIABLE_SET_KIND
        ),
        1
    );

    // `context api-resources -o json`: parseable records array listing the
    // supported resource kinds. The `"Kind"` column carries the PascalCase kind
    // name; `"Name"` carries the canonical lowercase plural (e.g. "variablesets").
    // Assert by Kind only — the plural name is an implementation detail.
    let api_resources_json = ctx
        .stdout_json(["context", "api-resources", "-o", "json"])
        .await;
    assert_api_resources_has_kind(
        &api_resources_json,
        "context api-resources -o json",
        fixtures::VARIABLE_SET_KIND,
    );
    assert_api_resources_has_kind(
        &api_resources_json,
        "context api-resources -o json",
        fixtures::SECRET_SET_KIND,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn assert_csv_contains_resource(raw: &str, label: &str, name: &str, kind: &str) {
    let mut lines = raw.lines();
    let header = lines
        .next()
        .unwrap_or_else(|| panic!("`{label}` should include a CSV header, got empty output"));

    assert!(
        header.contains("Name") && header.contains("Kind"),
        "`{label}` header should contain Name and Kind columns, got:\n{raw}"
    );
    assert!(
        lines.any(|line| line.contains(name) && line.contains(kind)),
        "`{label}` should contain a row for {kind}/{name}, got:\n{raw}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn assert_api_resources_has_kind(doc: &serde_json::Value, label: &str, kind: &str) {
    let rows = doc
        .as_array()
        .unwrap_or_else(|| panic!("`{label}` should be a JSON array of records:\n{doc}"));

    assert!(
        rows.iter()
            .any(|row| row.get("Schema").and_then(serde_json::Value::as_str) == Some(kind)),
        "`{label}` should list supported kind {kind}, got:\n{doc}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
