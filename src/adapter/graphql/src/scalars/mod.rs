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
mod dataset_collaboration;
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
pub(crate) use dataset_collaboration::*;
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

        impl std::ops::Deref for $name {
            type Target = $source_type;
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
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

macro_rules! simple_string_scalar {
    ($name: ident, $source_type: ty) => {
        crate::scalars::__simple_string_scalar_general!($name, $source_type);
        crate::scalars::__simple_string_scalar_scalar_type!($name, $source_type, try_from);
    };
    ($name: ident, $source_type: ty, $source_parse_method: ident) => {
        crate::scalars::__simple_string_scalar_general!($name, $source_type);
        crate::scalars::__simple_string_scalar_scalar_type!(
            $name,
            $source_type,
            $source_parse_method
        );
    };
}

macro_rules! __simple_string_scalar_general {
    ($name: ident, $source_type: ty) => {
        #[derive(Clone, Debug, PartialEq, Eq)]
        pub struct $name<'a>(std::borrow::Cow<'a, $source_type>);

        impl From<$source_type> for $name<'_> {
            fn from(value: $source_type) -> Self {
                Self(std::borrow::Cow::Owned(value))
            }
        }

        impl<'a> From<&'a $source_type> for $name<'a> {
            fn from(value: &'a $source_type) -> Self {
                Self(std::borrow::Cow::Borrowed(value))
            }
        }

        impl From<$name<'_>> for $source_type {
            fn from(val: $name) -> Self {
                val.0.into_owned()
            }
        }

        impl From<$name<'_>> for String {
            fn from(val: $name<'_>) -> Self {
                val.0.to_string()
            }
        }

        impl std::ops::Deref for $name<'_> {
            type Target = $source_type;
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl std::fmt::Display for $name<'_> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

macro_rules! __simple_string_scalar_scalar_type {
    ($name: ident, $source_type: ty, $source_parse_method: ident) => {
        #[async_graphql::Scalar]
        impl async_graphql::ScalarType for $name<'_> {
            fn parse(value: async_graphql::Value) -> async_graphql::InputValueResult<Self> {
                if let async_graphql::Value::String(value) = &value {
                    let val = <$source_type>::$source_parse_method(value.as_str())?;
                    Ok(val.into())
                } else {
                    Err(async_graphql::InputValueError::expected_type(value))
                }
            }

            fn to_value(&self) -> async_graphql::Value {
                async_graphql::Value::String(self.0.to_string())
            }
        }
    };
}

pub(crate) use simple_string_scalar;
use {__simple_string_scalar_general, __simple_string_scalar_scalar_type};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
