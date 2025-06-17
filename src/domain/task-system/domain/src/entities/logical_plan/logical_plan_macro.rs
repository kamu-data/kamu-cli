// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Macro to generate logical plan structs with serialization helpers
#[macro_export]
macro_rules! logical_plan_struct {
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

            pub fn into_logical_plan(self) -> $crate::LogicalPlan {
                $crate::LogicalPlan {
                    plan_type: Self::TYPE_ID.to_string(),
                    payload: serde_json::to_value(self)
                        .expect(concat!("Failed to serialize ", stringify!($name), " into JSON")),
                }
            }

            pub fn from_logical_plan(
                logical_plan: &$crate::LogicalPlan,
            ) -> Result<Self, internal_error::InternalError> {
                use internal_error::ResultIntoInternal;
                serde_json::from_value(logical_plan.payload.clone()).int_err()
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
