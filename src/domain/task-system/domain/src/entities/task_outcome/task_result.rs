// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::de::{MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskResult {
    pub result_type: String,
    pub payload: serde_json::Value,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl TaskResult {
    pub const TASK_RESULT_EMPTY: &str = "Empty";

    pub fn empty() -> Self {
        TaskResult {
            result_type: Self::TASK_RESULT_EMPTY.to_string(),
            payload: serde_json::Value::Null,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.result_type == Self::TASK_RESULT_EMPTY
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Serialize for TaskResult {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if self.is_empty() {
            serializer.serialize_str(TaskResult::TASK_RESULT_EMPTY)
        } else {
            let mut map = serializer.serialize_map(Some(1))?;
            map.serialize_entry(self.result_type.as_str(), &self.payload)?;
            map.end()
        }
    }
}

impl<'de> Deserialize<'de> for TaskResult {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct TaskResultVisitor;

        impl<'de> Visitor<'de> for TaskResultVisitor {
            type Value = TaskResult;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("either a string \"Empty\" or a map with one key")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                if v == TaskResult::TASK_RESULT_EMPTY {
                    Ok(TaskResult::empty())
                } else {
                    Err(E::custom(format!("Unknown string variant: {v}")))
                }
            }

            fn visit_map<M>(self, mut map: M) -> Result<TaskResult, M::Error>
            where
                M: MapAccess<'de>,
            {
                let (type_id, payload): (String, serde_json::Value) = map
                    .next_entry()?
                    .ok_or_else(|| serde::de::Error::custom("Expected a single-key map"))?;
                Ok(TaskResult {
                    result_type: type_id,
                    payload,
                })
            }
        }

        deserializer.deserialize_map(TaskResultVisitor)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
