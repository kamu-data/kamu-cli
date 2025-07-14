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
pub struct FlowRunArguments {
    pub arguments_type: String,
    pub payload: serde_json::Value,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Serialize for FlowRunArguments {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry(self.arguments_type.as_str(), &self.payload)?;
        map.end()
    }
}

impl<'de> Deserialize<'de> for FlowRunArguments {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct FlowRunArgumentsVisitor;

        impl<'de> Visitor<'de> for FlowRunArgumentsVisitor {
            type Value = FlowRunArguments;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a map with one key representing the arguments type")
            }

            fn visit_map<M>(self, mut map: M) -> Result<FlowRunArguments, M::Error>
            where
                M: MapAccess<'de>,
            {
                let (type_id, payload): (String, serde_json::Value) = map
                    .next_entry()?
                    .ok_or_else(|| serde::de::Error::custom("Expected a single-key map"))?;
                Ok(FlowRunArguments {
                    arguments_type: type_id,
                    payload,
                })
            }
        }

        deserializer.deserialize_map(FlowRunArgumentsVisitor)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Macro to generate flow run argument structs with serialization helpers
#[macro_export]
macro_rules! flow_run_arguments_struct {
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident {
            $($field_vis:vis $field:ident : $ty:ty),* $(,)?
        }
        => $type_id:expr
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
        $vis struct $name {
            $($field_vis $field : $ty),*
        }

        impl $name {
            pub const TYPE_ID: &'static str = $type_id;

            pub fn into_flow_run_arguments(self) -> $crate::FlowRunArguments {
                $crate::FlowRunArguments {
                    arguments_type: Self::TYPE_ID.to_string(),
                    payload: serde_json::to_value(self)
                        .expect(concat!("Failed to serialize ", stringify!($name), " into JSON")),
                }
            }

            pub fn from_flow_run_arguments(
                flow_run_arguments: &$crate::FlowRunArguments,
            ) -> Result<Self, internal_error::InternalError> {
                use internal_error::ResultIntoInternal;
                serde_json::from_value(flow_run_arguments.payload.clone()).int_err()
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
