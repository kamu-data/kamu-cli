// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ExtraAttributes
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ExtraAttributes {
    pub fn new() -> Self {
        Self {
            attributes: serde_json::Map::new(),
        }
    }

    pub fn new_from_json(value: serde_json::Value) -> Result<Self, ExtraAttributesInvalidInput> {
        let serde_json::Value::Object(attributes) = value else {
            return Err(ExtraAttributesInvalidInput { value });
        };

        Ok(Self { attributes })
    }

    pub fn into_json(self) -> serde_json::Value {
        serde_json::Value::Object(self.attributes)
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.attributes.contains_key(key)
    }

    pub fn deserialize_as<T>(&self) -> Result<T, serde_json::Error>
    where
        T: ::serde::de::DeserializeOwned,
    {
        // TODO: PERF: Avoid cloning
        let json = serde_json::Value::Object(self.attributes.clone());
        serde_json::from_value(json)
    }

    pub fn deserialize_key_as<T>(&self, key: &str) -> Result<Option<T>, serde_json::Error>
    where
        T: ::serde::de::DeserializeOwned,
    {
        let Some(value) = self.attributes.get(key) else {
            return Ok(None);
        };

        // TODO: PERF: Avoid cloning
        serde_json::from_value(value.clone()).map(Some)
    }

    pub fn merge_serialized<T: ::serde::Serialize>(&mut self, value: &T) {
        let serde_json::Value::Object(mut other) = serde_json::to_value(value).unwrap() else {
            panic!(
                "{} must serialize to object to use merge",
                std::any::type_name::<T>()
            );
        };

        self.attributes.append(&mut other);
    }

    pub fn merge(&mut self, value: serde_json::Value) {
        let serde_json::Value::Object(mut other) = value else {
            panic!("Value must be an object");
        };

        self.attributes.append(&mut other);
    }

    pub fn insert_serialized<T: ::serde::Serialize>(
        &mut self,
        key: impl Into<String>,
        value: &T,
    ) -> Result<(), serde_json::Error> {
        self.attributes
            .insert(key.into(), serde_json::to_value(value)?);
        Ok(())
    }

    pub fn get_str(&self, key: &str) -> Option<&str> {
        match self.attributes.get(key) {
            Some(serde_json::Value::String(v)) => Some(v.as_str()),
            _ => None,
        }
    }

    pub fn get_and_parse<T>(&self, key: &str) -> Result<Option<T>, <T as std::str::FromStr>::Err>
    where
        T: std::str::FromStr,
    {
        match self.get_str(key) {
            None => Ok(None),
            Some(v) => Some(v.parse()).transpose(),
        }
    }

    pub fn retain<F>(mut self, fun: F) -> Self
    where
        F: FnMut(&String, &mut serde_json::Value) -> bool,
    {
        self.attributes.retain(fun);
        self
    }

    pub fn map_empty(self) -> Option<Self> {
        if self.attributes.is_empty() {
            None
        } else {
            Some(self)
        }
    }
}

#[expect(clippy::derivable_impls)]
impl Default for ExtraAttributes {
    fn default() -> Self {
        Self {
            attributes: Default::default(),
        }
    }
}

impl TryFrom<serde_json::Value> for ExtraAttributes {
    type Error = ExtraAttributesInvalidInput;

    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
        Self::new_from_json(value)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Invalid input for ExtraAttributes: {value}")]
pub struct ExtraAttributesInvalidInput {
    value: serde_json::Value,
}
