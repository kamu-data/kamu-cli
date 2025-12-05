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

const DELIM: &str = "%2F";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Collection path that was verified at creation time.
#[derive(Clone, Debug, derive_more::Deref, derive_more::Display, Eq, PartialEq)]
pub struct CollectionPathV2(String); // Stored trimmed decoded value

impl CollectionPathV2 {
    pub fn try_new(path: impl Into<String>) -> Result<Self, CollectionPathValidationError> {
        let path = path.into();
        let trimmed_path = path.trim();

        if trimmed_path.is_empty() {
            return Err(CollectionPathValidationError::IsEmpty);
        }

        // "/" is allowed in the path
        let trimmed_path = trimmed_path.replace('/', DELIM);

        let Ok(decoded) = urlencoding::decode(&trimmed_path) else {
            return Err(CollectionPathValidationError::InvalidUrlEncoding { path });
        };

        if urlencoding::encode(&decoded) != trimmed_path {
            return Err(CollectionPathValidationError::InvalidUrlEncoding { path });
        }

        let trimmed_decoded = decoded.trim();

        if trimmed_decoded.is_empty() {
            return Err(CollectionPathValidationError::IsEmpty);
        }

        let result = if !trimmed_decoded.starts_with('/') {
            // Add leading / if it was missing
            format!("/{trimmed_decoded}")
        } else {
            trimmed_decoded.to_owned()
        };

        Ok(Self(result))
    }

    pub fn into_v1(self) -> CollectionPath {
        CollectionPath::new(self.0)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CollectionPathValidationError {
    #[error("Path is empty")]
    IsEmpty,

    #[error("Invalid URL encoding for path: `{path}`")]
    InvalidUrlEncoding { path: String },
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use super::*;

    #[test]
    fn test_collection_path_v2() {
        const SPACE: &str = "%20";

        // --- Invalid paths ---
        assert_matches!(
            CollectionPathV2::try_new(""),
            Err(CollectionPathValidationError::IsEmpty)
        );
        assert_matches!(
            CollectionPathV2::try_new("     "),
            Err(CollectionPathValidationError::IsEmpty)
        );
        assert_matches!(
            CollectionPathV2::try_new(format!("{DELIM}path{DELIM}to{DELIM}file with spaces")),
            Err(CollectionPathValidationError::InvalidUrlEncoding { .. })
        );

        // --- Valid paths ---
        assert_eq!(
            "/path/to/file",
            *CollectionPathV2::try_new("/path/to/file").unwrap()
        );
        assert_eq!("file.exe", *CollectionPathV2::try_new("file.exe").unwrap());
        // Leading / is optional
        assert_eq!(
            "/path/to/file",
            *CollectionPathV2::try_new(format!("path{DELIM}to{DELIM}file")).unwrap()
        );
        assert_eq!(
            "/path/to/file with spaces",
            *CollectionPathV2::try_new(format!(
                "{DELIM}path{DELIM}to{DELIM}file{SPACE}with{SPACE}spaces"
            ))
            .unwrap()
        );
        // Trim decoded spaces
        assert_eq!(
            "path to file",
            *CollectionPathV2::try_new(format!(
                "{SPACE}{SPACE}path{SPACE}to{SPACE}file{SPACE}{SPACE}"
            ))
            .unwrap()
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
