// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Shared building blocks for resource CLI e2e scenarios: the [`ResourceCtx`]
//! local/remote abstraction, manifest [`fixtures`], and small assert helpers.
//!
//! Scenario bodies live in `crate::commands::test_resources_*` and are written
//! once against [`ResourceCtx`], then wired per-database as local and remote
//! permutations.

mod context;
pub mod fixtures;
mod get_view;

pub use context::*;
pub use get_view::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Assert helpers for raw command output
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Assert every expected substring appears in a command output.
///
/// Use this for batch/resource-set commands where the CLI may emit stable
/// status lines in backend-dependent order.
pub fn assert_output_contains_all(output: &str, expected_lines: &[&str], command_name: &str) {
    for expected_line in expected_lines {
        assert!(
            output.contains(expected_line),
            "`{command_name}` should contain '{expected_line}', got:\n{output}"
        );
    }
}

/// Assert that a JSON records array (e.g. `list -o json`,
/// `context api-resources -o json`) contains at least one row whose `"Name"`
/// and `"Kind"` columns both match. Column names follow the Arrow/RecordsWriter
/// convention used by the CLI: title-cased strings, not camelCase.
pub fn assert_record_row(doc: &serde_json::Value, label: &str, name: &str, kind: &str) {
    let rows = doc
        .as_array()
        .unwrap_or_else(|| panic!("`{label}` should be a JSON array of records:\n{doc}"));

    assert!(
        rows.iter().any(|row| {
            row.get("Name").and_then(serde_json::Value::as_str) == Some(name)
                && row.get("Kind").and_then(serde_json::Value::as_str) == Some(kind)
        }),
        "`{label}` should contain a row with Name={name} Kind={kind}, got:\n{doc}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Return the `totalCount` for a resource kind from `summary -o json/yaml`
/// output converted to JSON.
pub fn summary_count(doc: &serde_json::Value, label: &str, kind: &str) -> u64 {
    summary_row(doc, label, kind)
        .get("totalCount")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_else(|| panic!("`{label}` row for {kind} has no numeric totalCount:\n{doc}"))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn summary_row<'a>(doc: &'a serde_json::Value, label: &str, kind: &str) -> &'a serde_json::Value {
    let rows = doc
        .get("resourceCounts")
        .and_then(serde_json::Value::as_array)
        .unwrap_or_else(|| panic!("`{label}` should contain resourceCounts array:\n{doc}"));

    rows.iter()
        .find(|row| row.get("kind").and_then(serde_json::Value::as_str) == Some(kind))
        .unwrap_or_else(|| panic!("`{label}` should contain a summary row for {kind}:\n{doc}"))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
