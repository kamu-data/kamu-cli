// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// This type is kept for backward data compatibility.
/// For new code, please use [`CollectionPathV2`](crate::CollectionPathV2)
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct CollectionPath(String);

impl CollectionPath {
    pub fn new(path: String) -> Self {
        Self(path)
    }

    pub fn into_inner(self) -> String {
        self.0
    }

    pub fn depth(&self) -> usize {
        self.0.chars().filter(|&c| c == '/').count()
    }
}

impl TryFrom<&str> for CollectionPath {
    type Error = InternalError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(Self::new(value.to_string()))
    }
}

impl std::fmt::Display for CollectionPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::ops::Deref for CollectionPath {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
