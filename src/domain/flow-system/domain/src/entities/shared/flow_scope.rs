// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct FlowScope(serde_json::Value);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const FLOW_SCOPE_ATTRIBUTE_TYPE: &str = "type";

pub const FLOW_SCOPE_TYPE_SYSTEM: &str = "System";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl FlowScope {
    #[inline]
    pub fn new(payload: serde_json::Value) -> Self {
        FlowScope(payload)
    }

    pub fn make_system_scope() -> Self {
        let payload = serde_json::json!({
            FLOW_SCOPE_ATTRIBUTE_TYPE: FLOW_SCOPE_TYPE_SYSTEM,
        });
        FlowScope::new(payload)
    }

    #[inline]
    pub fn is_system_scope(&self) -> bool {
        self.scope_type() == FLOW_SCOPE_TYPE_SYSTEM
    }

    pub fn scope_type(&self) -> &str {
        self.0
            .get(FLOW_SCOPE_ATTRIBUTE_TYPE)
            .and_then(serde_json::Value::as_str)
            .unwrap_or_else(|| panic!("FlowScope must have a '{FLOW_SCOPE_ATTRIBUTE_TYPE}' field"))
    }

    pub fn get_attribute(&self, key: &str) -> Option<&serde_json::Value> {
        self.0.get(key)
    }

    pub fn add_attribute(mut self, key: &'static str, value: impl serde::Serialize) -> Self {
        let value = serde_json::to_value(value).unwrap();
        self.0
            .as_object_mut()
            .unwrap()
            .insert(key.to_string(), value);
        self
    }

    pub fn matches_query(&self, query: &FlowScopeQuery) -> bool {
        if let Some((_, type_values)) = query
            .attributes
            .iter()
            .find(|(key, _)| *key == FLOW_SCOPE_ATTRIBUTE_TYPE)
            && !type_values.iter().any(|v| v == self.scope_type())
        {
            return false;
        }

        query
            .attributes
            .iter()
            .filter(|(key, _)| *key != FLOW_SCOPE_ATTRIBUTE_TYPE)
            .all(|(key, values)| {
                self.get_attribute(key)
                    .and_then(|value| value.as_str())
                    .map(|value_str| values.iter().any(|v| v == value_str))
                    .unwrap_or(false)
            })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct FlowScopeQuery {
    pub attributes: Vec<(&'static str, Vec<String>)>,
}

impl FlowScopeQuery {
    pub fn all() -> Self {
        Self { attributes: vec![] }
    }

    pub fn build_for_system_scope() -> Self {
        Self {
            attributes: vec![(
                FLOW_SCOPE_ATTRIBUTE_TYPE,
                vec![FLOW_SCOPE_TYPE_SYSTEM.to_string()],
            )],
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scope_pack_and_query() {
        let scope = FlowScope::make_system_scope();
        assert_eq!(scope.scope_type(), FLOW_SCOPE_TYPE_SYSTEM);

        assert!(scope.is_system_scope());
    }

    #[test]
    fn test_matches_scope_query() {
        let scope = FlowScope::make_system_scope();
        let query = FlowScopeQuery::build_for_system_scope();
        assert!(scope.matches_query(&query));

        let non_matching_query = FlowScopeQuery {
            attributes: vec![(FLOW_SCOPE_ATTRIBUTE_TYPE, vec!["NonSystem".to_string()])],
        };
        assert!(!scope.matches_query(&non_matching_query));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
