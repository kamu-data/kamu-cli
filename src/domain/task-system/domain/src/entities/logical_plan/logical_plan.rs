// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::de::{self, MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalPlan {
    pub plan_type: String,
    pub payload: serde_json::Value,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl LogicalPlan {
    pub fn dataset_id(&self) -> Option<odf::DatasetID> {
        self.payload
            .get("dataset_id")
            .and_then(|v| v.as_str())
            .and_then(|s| odf::DatasetID::from_did_str(s).ok())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Serialize for LogicalPlan {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry(self.plan_type.as_str(), &self.payload)?;
        map.end()
    }
}

impl<'de> Deserialize<'de> for LogicalPlan {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct LogicalPlanVisitor;

        impl<'de> Visitor<'de> for LogicalPlanVisitor {
            type Value = LogicalPlan;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a map with one key representing the task type")
            }

            fn visit_map<M>(self, mut map: M) -> Result<LogicalPlan, M::Error>
            where
                M: MapAccess<'de>,
            {
                let (type_id, payload): (String, serde_json::Value) = map
                    .next_entry()?
                    .ok_or_else(|| de::Error::custom("Expected a single-key map"))?;
                Ok(LogicalPlan {
                    plan_type: type_id,
                    payload,
                })
            }
        }

        deserializer.deserialize_map(LogicalPlanVisitor)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
