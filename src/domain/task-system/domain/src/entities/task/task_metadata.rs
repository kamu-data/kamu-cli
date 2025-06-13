// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct TaskMetadata {
    properties: HashMap<String, String>,
}

impl TaskMetadata {
    pub fn from<TKey: Into<String>, TValue: Into<String>>(
        key_value_pairs: Vec<(TKey, TValue)>,
    ) -> Self {
        let mut properties = HashMap::new();
        for (k, v) in key_value_pairs {
            properties.insert(k.into(), v.into());
        }
        Self { properties }
    }

    pub fn set_property<TKey: Into<String>, TValue: Into<String>>(
        &mut self,
        key: TKey,
        value: TValue,
    ) {
        self.properties.insert(key.into(), value.into());
    }

    pub fn try_get_property(&self, key: &str) -> Option<String> {
        self.properties.get(key).cloned()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
