// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::LazyLock;

use async_graphql::{InputValueError, InputValueResult, Scalar, ScalarType, Value};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

static MAX_U256: LazyLock<num_bigint::BigInt> = LazyLock::new(|| {
    const ONES_256_BYTES: [u8; 256] = [b'1'; 256];
    const BINARY_RADIX: u32 = 2;

    let max_u256_as_str = std::str::from_utf8(&ONES_256_BYTES).unwrap();

    // 115792089237316195423570985008687907853269984665640564039457584007913129639935
    num_bigint::BigInt::parse_bytes(max_u256_as_str.as_bytes(), BINARY_RADIX).unwrap()
});

#[nutype::nutype(derive(AsRef, Clone, Debug, Into))]
pub struct U256(num_bigint::BigInt);

#[Scalar]
/// 256-bit unsigned number. Can be constructed from string.
impl ScalarType for U256 {
    fn parse(value: Value) -> InputValueResult<Self> {
        match value {
            Value::String(s) => {
                let big_int = s
                    .parse::<num_bigint::BigInt>()
                    .map_err(|e| InputValueError::custom(format!("Invalid U256: {e}")))?;

                if matches!(big_int.sign(), num_bigint::Sign::Minus) {
                    return Err(InputValueError::custom(format!(
                        "Invalid U256: negative values are not allowed for U256: {big_int}"
                    )));
                }

                if big_int > *MAX_U256 {
                    return Err(InputValueError::custom(format!(
                        "Invalid U256: value exceeds maximum for U256: {big_int}"
                    )));
                }

                Ok(U256::new(big_int))
            }
            Value::Number(n) => {
                let n = n.to_string();

                Err(InputValueError::custom(format!(
                    "Invalid U256: the value is expected to be a string (\"{n}\") instead of a \
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
