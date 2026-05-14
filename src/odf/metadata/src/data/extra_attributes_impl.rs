// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::data::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ExtraAttributes
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait ExtraAttribute: ::serde::Serialize + ::serde::de::DeserializeOwned {
    const KEYS: &[&str];
}

pub trait IntoExtraAttribute {
    type Output: ExtraAttribute;
    fn into_attribute(self) -> Self::Output;
}

impl<T> IntoExtraAttribute for T
where
    T: ExtraAttribute,
{
    type Output = Self;

    fn into_attribute(self) -> Self::Output {
        self
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ExtraAttributes {
    pub fn new() -> Self {
        Self {
            entries: std::collections::BTreeMap::new(),
        }
    }

    pub fn new_from_json(value: serde_json::Value) -> Result<Self, ExtraAttributesInvalidInput> {
        let serde_json::Value::Object(json_map) = value else {
            return Err(ExtraAttributesInvalidInput { value });
        };

        Ok(Self {
            // TODO: Avoid reallocation
            entries: std::collections::BTreeMap::from_iter(json_map),
        })
    }

    pub fn into_json(self) -> serde_json::Value {
        // TODO: Avoid reallocation
        serde_json::Value::Object(serde_json::Map::from_iter(self.entries))
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.entries.contains_key(key)
    }

    pub fn get<T>(&self) -> Result<Option<T>, serde_json::Error>
    where
        T: ExtraAttribute,
    {
        if !T::KEYS.iter().any(|k| self.entries.contains_key(*k)) {
            return Ok(None);
        }

        // TODO: PERF: Avoid cloning
        let json = serde_json::Value::Object(serde_json::Map::from_iter(self.entries.clone()));
        let val = serde_json::from_value(json)?;
        Ok(Some(val))
    }

    pub fn insert<T: IntoExtraAttribute>(&mut self, value: T) {
        let attr = value.into_attribute();
        let serde_json::Value::Object(other) = serde_json::to_value(attr).unwrap() else {
            panic!(
                "{} must serialize to object to use merge",
                std::any::type_name::<T>()
            );
        };

        self.entries.extend(other);
    }

    pub fn with<T: IntoExtraAttribute>(mut self, value: T) -> Self {
        self.insert(value);
        self
    }

    pub fn insert_json(&mut self, value: serde_json::Value) {
        let serde_json::Value::Object(other) = value else {
            panic!("Value must be an object");
        };

        self.entries.extend(other);
    }

    pub fn remove<T: ExtraAttribute>(&mut self) {
        self.entries.retain(|k, _| !T::KEYS.contains(&k.as_str()));
    }

    pub fn retain<F>(mut self, fun: F) -> Self
    where
        F: FnMut(&String, &mut serde_json::Value) -> bool,
    {
        self.entries.retain(fun);
        self
    }

    pub fn map_empty(self) -> Option<Self> {
        if self.entries.is_empty() {
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
            entries: Default::default(),
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
