// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_datasets::CollectionPath;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetNameGenerator;

impl DatasetNameGenerator {
    pub fn based_on_collection_path(path: &CollectionPath) -> odf::DatasetName {
        let uuid = uuid::Uuid::new_v4();
        Self::based_on_collection_path_with_uuid(path, uuid)
    }

    // Public for testing purposes
    pub fn based_on_collection_path_with_uuid(
        path: &CollectionPath,
        uuid: uuid::Uuid,
    ) -> odf::DatasetName {
        // Dataset name PEG grammar: [a-zA-Z0-9]+ ("-" [a-zA-Z0-9]+)*
        // Based on: <https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#dataset-identity>
        let sanitized_path = {
            let mut s = String::with_capacity(path.as_str().len());
            let mut prev_was_dash = true; // ignore the first dash in the path

            for c in path.as_str().chars() {
                match c {
                    // valid chars
                    c if c.is_ascii_alphanumeric() => {
                        s.push(c);
                        prev_was_dash = false;
                    }
                    _ if prev_was_dash => {
                        // merge "--" into "-"
                    }
                    _ => {
                        s.push('-');
                        prev_was_dash = true
                    }
                };
            }

            if s.ends_with('-') {
                s.pop();
            }

            s
        };

        let raw = format!("{uuid}-{sanitized_path}");
        odf::DatasetName::try_from(raw).unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
