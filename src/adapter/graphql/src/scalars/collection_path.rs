// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Validate correctness (not empty, valid URL-encodeding)
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize, serde::Serialize)]
pub struct CollectionPath(String);

scalar!(
    CollectionPath,
    "CollectionPath",
    "Collection entry paths are similar to HTTP path components. They are rooted (start with \
     `/`), separated by forward slashes, with elements URL-encoded (e.g. `/foo%20bar/baz`)"
);

impl From<String> for CollectionPath {
    fn from(v: String) -> Self {
        Self(v)
    }
}

impl From<CollectionPath> for String {
    fn from(v: CollectionPath) -> Self {
        v.0
    }
}

impl From<CollectionPath> for serde_json::Value {
    fn from(v: CollectionPath) -> Self {
        serde_json::Value::String(v.0)
    }
}

impl std::fmt::Display for CollectionPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::borrow::Borrow<String> for CollectionPath {
    fn borrow(&self) -> &String {
        &self.0
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
