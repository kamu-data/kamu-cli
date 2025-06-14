// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait ResponseExt {
    fn into_json_data(self) -> serde_json::Value;

    fn error_messages(&self) -> Vec<String>;
}

impl ResponseExt for async_graphql::Response {
    fn into_json_data(self) -> serde_json::Value {
        match self.data.into_json() {
            Ok(json) => json,
            Err(e) => panic!(
                "Failed to JSON: {e}; server errors: {:?}",
                self.errors
                    .into_iter()
                    .map(|e| e.message)
                    .collect::<Vec<_>>()
                    .join(",")
            ),
        }
    }

    fn error_messages(&self) -> Vec<String> {
        self.errors
            .iter()
            .map(|e| e.message.clone())
            .collect::<Vec<_>>()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
