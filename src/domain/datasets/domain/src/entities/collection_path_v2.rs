// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::CollectionPath;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Collection path that was verified at creation time.
#[derive(
    Clone,
    Debug,
    serde::Serialize,
    serde::Deserialize,
    derive_more::Deref,
    derive_more::Display,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
)]
pub struct CollectionPathV2(String); // Stored trimmed decoded value

impl CollectionPathV2 {
    pub fn try_new(path: impl Into<String>) -> Result<Self, CollectionPathValidationError> {
        let path = path.into();
        let path = if path.starts_with('/') {
            path
        } else {
            format!("/{path}")
        };

        for (i, segment) in path.split('/').enumerate() {
            if segment.is_empty() {
                if i == 0 {
                    continue;
                }
                return Err(CollectionPathValidationError { path });
            }

            let Ok(decoded) = urlencoding::decode(segment) else {
                return Err(CollectionPathValidationError { path });
            };

            if urlencoding::encode(&decoded) != segment {
                return Err(CollectionPathValidationError { path });
            }
        }

        Ok(Self(path))
    }

    pub fn into_v1(self) -> CollectionPath {
        CollectionPath::new(self.0)
    }

    pub fn from_v1_unchecked(path: CollectionPath) -> Self {
        Self(path.into_inner())
    }
}

#[derive(Debug, thiserror::Error)]
#[error(
    "Invalid path `{path}` - path must be in the form \
     `/<url-encoded-segment>/<url-encoded-segment>/...`"
)]
pub struct CollectionPathValidationError {
    path: String,
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use super::*;

    #[test]
    fn test_collection_path_v2() {
        // Err: empty
        assert_matches!(CollectionPathV2::try_new(""), Err(_));

        // Err: unencoded spaces
        assert_matches!(CollectionPathV2::try_new("     "), Err(_));

        // Err: empty segments
        assert_matches!(CollectionPathV2::try_new("/"), Err(_));
        assert_matches!(CollectionPathV2::try_new("/a/"), Err(_));
        assert_matches!(CollectionPathV2::try_new("/a//b"), Err(_));

        // Ok: prepends leading slash
        assert_eq!(
            CollectionPathV2::try_new("path/to/file").unwrap().as_str(),
            "/path/to/file",
        );

        // Ok
        CollectionPathV2::try_new("a").unwrap();
        CollectionPathV2::try_new("/a").unwrap();
        CollectionPathV2::try_new("/a.txt").unwrap();

        // Ok: can have encoded slashes
        CollectionPathV2::try_new("/a%2Fb%2Fc").unwrap();

        // Ok: 'slava 🇺🇦'
        CollectionPathV2::try_new("/slava%20%F0%9F%87%BA%F0%9F%87%A6").unwrap();

        // Ok: yes even ' ' path is ok - we don't care
        CollectionPathV2::try_new("/%20").unwrap();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
