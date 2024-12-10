// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod access_token;
mod account;
mod data_batch;
mod data_query;
mod data_schema;
mod dataset_endpoints;
mod dataset_env_var;
mod dataset_id_name;
mod dataset_metadata;
mod dataset_visibility;
mod dateset_state;
mod engine_desc;
mod event_id;
mod flow_configuration;
mod flow_scalars;
mod flow_trigger;
mod metadata;
mod multihash;
mod odf_generated;
mod os_path;
mod pagination;
mod task_id;
mod task_status_outcome;

pub(crate) use access_token::*;
pub(crate) use account::*;
pub(crate) use data_batch::*;
pub(crate) use data_query::*;
pub(crate) use data_schema::*;
pub(crate) use dataset_endpoints::*;
pub(crate) use dataset_env_var::*;
pub(crate) use dataset_id_name::*;
pub(crate) use dataset_metadata::*;
pub(crate) use dataset_visibility::*;
pub(crate) use dateset_state::*;
pub(crate) use engine_desc::*;
pub(crate) use event_id::*;
pub(crate) use flow_configuration::*;
pub(crate) use flow_scalars::*;
pub(crate) use flow_trigger::*;
pub(crate) use metadata::*;
pub(crate) use multihash::*;
pub(crate) use odf_generated::*;
pub(crate) use os_path::*;
pub(crate) use pagination::*;
pub(crate) use task_id::*;
pub(crate) use task_status_outcome::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

macro_rules! simple_scalar {
    ($name: ident, $source_type: ty) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub struct $name($source_type);

        impl From<$source_type> for $name {
            fn from(value: $source_type) -> Self {
                $name(value)
            }
        }

        impl From<$name> for $source_type {
            fn from(val: $name) -> Self {
                val.0
            }
        }

        impl Deref for $name {
            type Target = $source_type;
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        #[Scalar]
        impl ScalarType for $name {
            fn parse(value: Value) -> InputValueResult<Self> {
                if let Value::String(s) = &value {
                    match s.parse() {
                        Ok(i) => Ok(Self(<$source_type>::new(i))),
                        Err(_) => Err(InputValueError::expected_type(value)),
                    }
                } else {
                    Err(InputValueError::expected_type(value))
                }
            }

            fn to_value(&self) -> Value {
                Value::String(self.0.to_string())
            }
        }
    };
}

pub(crate) use simple_scalar;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
