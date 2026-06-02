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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UInt64(pub u64);

impl From<u64> for UInt64 {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<UInt64> for u64 {
    fn from(value: UInt64) -> Self {
        value.0
    }
}

#[Scalar]
impl ScalarType for UInt64 {
    fn parse(value: Value) -> InputValueResult<Self> {
        match value {
            Value::String(s) => {
                let val = s
                    .parse::<u64>()
                    .map_err(|e| InputValueError::custom(format!("Invalid UInt64: {e}")))?;
                Ok(Self(val))
            }
            Value::Number(n) => {
                let n = n.to_string();

                Err(InputValueError::custom(format!(
                    "Invalid UInt64: the value is expected to be a string (\"{n}\") instead of a \
                     number ({n})"
                )))
            }
            v @ (Value::Null
            | Value::Boolean(_)
            | Value::Binary(_)
            | Value::Enum(_)
            | Value::List(_)
            | Value::Object(_)) => Err(InputValueError::expected_type(v)),
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.to_string())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
