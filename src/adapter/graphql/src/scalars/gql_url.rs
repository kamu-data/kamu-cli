// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::{InputValueError, InputValueResult, Scalar, ScalarType, Value};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A wrapper type for `url::Url` to be used as a GraphQL scalar.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GqlUrl(pub url::Url);

#[Scalar(name = "URL")]
impl ScalarType for GqlUrl {
    fn parse(value: Value) -> InputValueResult<Self> {
        if let Value::String(s) = &value {
            url::Url::parse(s)
                .map(GqlUrl)
                .map_err(|err| InputValueError::custom(format!("Invalid URL: {err}")))
        } else {
            Err(InputValueError::expected_type(value))
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.as_str().to_string())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
