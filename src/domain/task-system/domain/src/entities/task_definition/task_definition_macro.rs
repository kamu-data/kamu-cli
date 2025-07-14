// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Macro to generate task definition structs and their trait impls
#[macro_export]
macro_rules! task_definition_struct {
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident {
            $($field_vis:vis $field:ident : $ty:ty),* $(,)?
        }
        => $task_type:expr
    ) => {
        $(#[$meta])*
        #[derive(Debug)]
        $vis struct $name {
            $($field_vis $field : $ty),*
        }

        impl $name {
            pub const TASK_TYPE: &'static str = $task_type;
        }

        #[async_trait::async_trait]
        impl $crate::TaskDefinitionInner for $name {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn into_any(self: Box<Self>) -> Box<dyn std::any::Any> {
                self
            }

            fn task_type(&self) -> &'static str {
                Self::TASK_TYPE
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
