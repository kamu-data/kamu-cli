// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
pub struct ExtraDataFields(serde_json::Map<String, serde_json::Value>);

impl ExtraDataFields {
    pub fn new(value: serde_json::Map<String, serde_json::Value>) -> Self {
        Self(value)
    }

    pub fn into_inner(self) -> serde_json::Map<String, serde_json::Value> {
        self.0
    }

    pub fn as_map(&self) -> &serde_json::Map<String, serde_json::Value> {
        &self.0
    }

    pub fn as_mut_map(&mut self) -> &mut serde_json::Map<String, serde_json::Value> {
        &mut self.0
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
