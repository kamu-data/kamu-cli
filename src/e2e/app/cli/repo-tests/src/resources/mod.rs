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

pub use context::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Assert helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Assert a resource is not present: `get <kind> <name> --ignore-not-found`
/// succeeds (does not error) and its name does not appear in `list <kind>`.
pub async fn assert_resource_absent(ctx: &ResourceCtx, kind: &str, name: &str) {
    // `get … --ignore-not-found` must succeed and must not echo the name.
    let get_stdout = ctx
        .stdout([
            "get".to_string(),
            kind.to_string(),
            name.to_string(),
            "--ignore-not-found".to_string(),
        ])
        .await;
    assert!(
        !get_stdout.contains(name),
        "expected resource '{name}' to be absent, but `get` output mentioned it:\n{get_stdout}"
    );

    let list_stdout = ctx.stdout(["list".to_string(), kind.to_string()]).await;
    assert!(
        !list_stdout.contains(name),
        "expected resource '{name}' absent from `list {kind}`, but it appeared:\n{list_stdout}"
    );
}

/// Fetch a resource as JSON (`get <kind> <name> -o json`) and extract its
/// stable UID. Looks for a `uid` field anywhere in the parsed document.
pub async fn get_resource_uid(ctx: &ResourceCtx, kind: &str, name: &str) -> String {
    let stdout = ctx
        .stdout([
            "get".to_string(),
            kind.to_string(),
            name.to_string(),
            "-o".to_string(),
            "json".to_string(),
        ])
        .await;

    let doc: serde_json::Value = serde_json::from_str(&stdout).unwrap_or_else(|e| {
        panic!("`get {kind} {name} -o json` did not return JSON: {e}\n{stdout}")
    });

    find_uid(&doc).unwrap_or_else(|| panic!("no `uid` field found in resource JSON:\n{stdout}"))
}

/// Recursively search a JSON value for a `uid` string field.
fn find_uid(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::Object(map) => {
            if let Some(serde_json::Value::String(uid)) = map.get("uid") {
                return Some(uid.clone());
            }
            map.values().find_map(find_uid)
        }
        serde_json::Value::Array(items) => items.iter().find_map(find_uid),
        _ => None,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
