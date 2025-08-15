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
pub struct FlowConfigurationRule {
    pub rule_type: String,
    pub payload: serde_json::Value,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Serialize for FlowConfigurationRule {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry(self.rule_type.as_str(), &self.payload)?;
        map.end()
    }
}

impl<'de> Deserialize<'de> for FlowConfigurationRule {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct FlowConfigurationRuleVisitor;

        impl<'de> Visitor<'de> for FlowConfigurationRuleVisitor {
            type Value = FlowConfigurationRule;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a map with one key representing the rule type")
            }

            fn visit_map<M>(self, mut map: M) -> Result<FlowConfigurationRule, M::Error>
            where
                M: MapAccess<'de>,
            {
                let (type_id, payload): (String, serde_json::Value) = map
                    .next_entry()?
                    .ok_or_else(|| serde::de::Error::custom("Expected a single-key map"))?;
                Ok(FlowConfigurationRule {
                    rule_type: type_id,
                    payload,
                })
            }
        }

        deserializer.deserialize_map(FlowConfigurationRuleVisitor)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
