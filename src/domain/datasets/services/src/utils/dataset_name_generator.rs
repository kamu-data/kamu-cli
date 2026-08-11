// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_datasets::CollectionPathV2;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetNameGenerator;

impl DatasetNameGenerator {
    pub fn based_on_collection_path(path: &CollectionPathV2) -> odf::DatasetName {
        let uuid = uuid::Uuid::new_v4();
        Self::based_on_collection_path_with_uuid(path, uuid)
    }

    // Public for testing purposes
    pub fn based_on_collection_path_with_uuid(
        path: &CollectionPathV2,
        uuid: uuid::Uuid,
    ) -> odf::DatasetName {
        use std::fmt::Write;

        // Dataset name PEG grammar: [a-zA-Z0-9]+ ("-" [a-zA-Z0-9]+)*
        // Based on: <https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#dataset-identity>
        let mut s = String::with_capacity(odf::DatasetName::MAX_LEN);

        write!(&mut s, "{uuid}-").unwrap();

        // Handle V1 path names without `/` prefix just in case
        let basename_encoded = if path.starts_with('/') {
            path.as_str().rsplit_once('/').unwrap().1
        } else {
            path.as_str()
        };

        // SAFETY: Path is already validated
        let basename_decoded = urlencoding::decode(basename_encoded).unwrap();

        for c in basename_decoded.chars() {
            if c.is_ascii_alphanumeric() {
                s.push(c);
            } else if !s.ends_with('-') {
                s.push('-');
            }
        }

        while s.ends_with('-') {
            s.pop();
        }

        s.truncate(odf::DatasetName::MAX_LEN);

        odf::DatasetName::new_unchecked(&s)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
