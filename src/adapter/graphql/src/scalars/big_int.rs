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

#[nutype::nutype(derive(AsRef, Clone, Debug, Into))]
pub struct BigInt(num_bigint::BigInt);

#[Scalar]
/// A big integer scalar type.
impl ScalarType for BigInt {
    fn parse(value: Value) -> InputValueResult<Self> {
        match value {
            Value::String(s) => {
                let big_int = s
                    .parse()
                    .map_err(|e| InputValueError::custom(format!("Invalid BigInt: {e}")))?;
                Ok(BigInt::new(big_int))
            }
            Value::Number(n) => {
                let n = n.to_string();

                Err(InputValueError::custom(format!(
                    "Invalid BigInt: the value is expected to be a string (\"{n}\") instead of a \
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
        Value::String(self.as_ref().to_string())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
