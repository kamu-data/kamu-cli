// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A small, *queryable* view over `get -o json` output.
//!
//! ## Why this exists
//!
//! Resource e2e tests almost always want to answer one of two questions:
//!
//! 1. **Identity** — "which resources did this selector return?" i.e. the set
//!    of `(kind, name)` pairs. This is the whole point of the selector-grammar
//!    tests (QA 7): did `s%/db-creds` resolve to the `SecretSet` named
//!    `db-creds`, and *only* that?
//! 2. **A specific field** — "does the resolved `VariableSet` carry
//!    `MESSAGE=hello`?" i.e. the one spec field the case is actually about.
//!
//! Neither question is served by comparing the *whole* `get -o json` document.
//! A full-document comparison forces every test to (re-)assert volatile fields
//! (`uid`, `account`, timestamps) and incidental server state (the reconciler
//! status block, `stats`, condition timestamps) that the test does not care
//! about — so an unrelated status change breaks selector tests with a diff that
//! points at the wrong feature.
//!
//! [`ResourceView`] is therefore an **allowlist**: it reads only the fields
//! tests assert on. New volatile/server fields are ignored by construction, so
//! they never make a test flaky or break it for the wrong reason. The one place
//! that *does* want the full shape — catching drift in the `get` render struct
//! — is a deliberate, single "golden" test per kind (see
//! `test_resources_golden_view`), which owns its own volatile-field handling.
//!
//! ## Cardinality
//!
//! `get` emits a single bare object for one result and a `{ "items": [...] }`
//! envelope for many (see `output_rendered_items` in
//! `get_resource_command.rs`). [`parse_get_views`] hides that: callers always
//! get a `Vec<ResourceView>` and never branch on cardinality.
//! [`ResourceCtx::get_one`] is the sugar for the "exactly one" case.

use serde_json::Value;

use super::ResourceCtx;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// One resource as returned by `get -o json`, reduced to the fields tests
/// assert on. Wraps the raw JSON; accessors read specific paths on demand, so
/// fields this type does not expose are simply never read (and thus never make
/// an assertion brittle).
#[derive(Debug, Clone)]
pub struct ResourceView(Value);

impl ResourceView {
    /// The resource kind, e.g. `VariableSet` / `SecretSet` (`/kind`).
    pub fn kind(&self) -> &str {
        self.str_at("/kind", "kind")
    }

    /// The resource name (`/metadata/name`).
    pub fn name(&self) -> &str {
        self.str_at("/metadata/name", "metadata.name")
    }

    /// `(kind, name)` — the resource's identity. The primary assertion target
    /// for selector tests: collect these across a result and compare the set.
    pub fn ident(&self) -> (&str, &str) {
        (self.kind(), self.name())
    }

    /// `metadata.description`, if present.
    pub fn description(&self) -> Option<&str> {
        self.0
            .pointer("/metadata/description")
            .and_then(Value::as_str)
    }

    /// Stable resource UID. The exact nesting is intentionally not part of the
    /// test contract, so this searches the rendered document recursively.
    pub fn uid(&self) -> String {
        find_uid(&self.0)
            .unwrap_or_else(|| panic!("resource view has no string `uid`:\n{}", self.0))
    }

    /// The value of a `VariableSet` variable (`spec.variables.<key>`), if
    /// present. Variables render as scalar strings.
    pub fn variable(&self, key: &str) -> Option<&str> {
        self.0
            .pointer(&format!("/spec/variables/{key}"))
            .and_then(Value::as_str)
    }

    /// Whether a `SecretSet` carries a secret under `<key>`
    /// (`spec.secrets.<key>`). Secret *values* are encrypted (and the
    /// ciphertext is non-deterministic), so tests assert presence of the
    /// key, not its value; plaintext leakage is asserted separately on raw
    /// output.
    pub fn has_secret(&self, key: &str) -> bool {
        self.0.pointer(&format!("/spec/secrets/{key}")).is_some()
    }

    /// The underlying JSON document — escape hatch for the golden test and any
    /// assertion not yet covered by a typed accessor.
    pub fn into_json(self) -> Value {
        self.0
    }

    /// The underlying JSON document by reference.
    pub fn as_json(&self) -> &Value {
        &self.0
    }

    fn str_at(&self, pointer: &str, label: &str) -> &str {
        self.0
            .pointer(pointer)
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("resource view has no string `{label}`:\n{}", self.0))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn find_uid(value: &Value) -> Option<String> {
    match value {
        Value::Object(map) => {
            if let Some(Value::String(uid)) = map.get("uid") {
                return Some(uid.clone());
            }
            map.values().find_map(find_uid)
        }
        Value::Array(items) => items.iter().find_map(find_uid),
        _ => None,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Parse `get -o json` output into views, transparently handling both the
/// single-object and the `{ "items": [...] }` multi-result shapes. Always
/// returns a `Vec` so callers don't branch on cardinality. `label` is the
/// command (for panic messages on malformed JSON).
pub fn parse_get_views(raw: &str, label: &str) -> Vec<ResourceView> {
    let doc: Value = serde_json::from_str(raw)
        .unwrap_or_else(|e| panic!("`{label}` did not return valid JSON: {e}\n{raw}"));

    match doc {
        Value::Object(ref map) if map.contains_key("items") => map["items"]
            .as_array()
            .unwrap_or_else(|| panic!("`{label}` `items` was not an array:\n{raw}"))
            .iter()
            .cloned()
            .map(ResourceView)
            .collect(),
        other => vec![ResourceView(other)],
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceCtx {
    /// Run a `get … -o json` command and parse the result into views.
    ///
    /// `-o json` is appended automatically, so callers pass only the selector
    /// (e.g. `["get", "vs", "app-vars"]`).
    pub async fn get_views<I, S>(&self, args: I) -> Vec<ResourceView>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut full: Vec<String> = args.into_iter().map(Into::into).collect();
        let label = full.join(" ");
        full.push("-o".to_string());
        full.push("json".to_string());

        let raw = self.stdout(full).await;
        parse_get_views(&raw, &label)
    }

    /// Run a `get` command expected to resolve to exactly one resource, and
    /// return it. Panics (with the command) if zero or more than one match — so
    /// single-form selector tests double as a cardinality assertion.
    pub async fn get_one<I, S>(&self, args: I) -> ResourceView
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let collected: Vec<String> = args.into_iter().map(Into::into).collect();
        let label = collected.join(" ");
        let mut views = self.get_views(collected).await;

        assert_eq!(
            views.len(),
            1,
            "`{label}` should resolve to exactly one resource, got {}",
            views.len()
        );
        views.pop().unwrap()
    }

    /// Run a `get` command and return the **sorted set of `(kind, name)`
    /// identities** it resolved to. The canonical assertion for selector
    /// grammar: compare against an expected sorted list to prove the selector
    /// matched the right resources and only those.
    pub async fn get_idents<I, S>(&self, args: I) -> Vec<(String, String)>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut idents: Vec<(String, String)> = self
            .get_views(args)
            .await
            .iter()
            .map(|v| (v.kind().to_string(), v.name().to_string()))
            .collect();
        idents.sort();
        idents
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
