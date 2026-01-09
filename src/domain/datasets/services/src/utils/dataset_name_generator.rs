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
        // Dataset name PEG grammar: [a-zA-Z0-9]+ ("-" [a-zA-Z0-9]+)*
        // Based on: <https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#dataset-identity>
        let mut s = String::with_capacity(path.as_str().len());

        for segment in path.as_str().split('/') {
            // SAFETY: Path is already validated
            let segment_decoded = urlencoding::decode(segment).unwrap();

            for c in segment_decoded.chars() {
                if c.is_ascii_alphanumeric() {
                    s.push(c);
                } else if !s.ends_with('-') {
                    s.push('-');
                }
            }

            if !s.ends_with('-') {
                s.push('-');
            }
        }

        let sanitized_path = s.trim_matches('-');

        let raw = format!("{uuid}-{sanitized_path}");
        odf::DatasetName::try_from(raw).unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
