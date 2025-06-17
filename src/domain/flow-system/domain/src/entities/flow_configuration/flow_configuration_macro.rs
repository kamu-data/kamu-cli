// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Macro to generate flow config structs with serialization helpers
#[macro_export]
macro_rules! flow_config_struct {
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

            pub fn into_flow_config(self) -> $crate::FlowConfigurationRule {
                $crate::FlowConfigurationRule {
                    rule_type: Self::TYPE_ID.to_string(),
                    payload: serde_json::to_value(self)
                        .expect(concat!("Failed to serialize ", stringify!($name), " into JSON")),
                }
            }

            pub fn from_flow_config(
                flow_config: &$crate::FlowConfigurationRule,
            ) -> Result<Self, internal_error::InternalError> {
                use internal_error::ResultIntoInternal;
                serde_json::from_value(flow_config.payload.clone()).int_err()
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Macro to generate flow config enums with serialization helpers
#[macro_export]
macro_rules! flow_config_enum {
    (
        $(#[$meta:meta])*
        $vis:vis enum $name:ident {
            $($variant:tt)*
        }
        => $type_id:expr
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
        $vis enum $name {
            $($variant)*
        }

        impl $name {
            pub const TYPE_ID: &'static str = $type_id;

            pub fn into_flow_config(self) -> $crate::FlowConfigurationRule {
                $crate::FlowConfigurationRule {
                    rule_type: Self::TYPE_ID.to_string(),
                    payload: serde_json::to_value(self)
                        .expect(concat!("Failed to serialize ", stringify!($name), " into JSON")),
                }
            }

            pub fn from_flow_config(
                flow_config: &$crate::FlowConfigurationRule,
            ) -> Result<Self, internal_error::InternalError> {
                use internal_error::ResultIntoInternal;
                serde_json::from_value(flow_config.payload.clone()).int_err()
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
