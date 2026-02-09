// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::*;
use kamu_adapter_graphql::scalars::{BigInt, ExtraData, U256};
use kamu_adapter_graphql::traits::ResponseExt;
use pretty_assertions::assert_eq;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_extra_data() {
    fn request(variables_as_value: Value) -> Request {
        Request::new(indoc::indoc!(
            r#"
            query ($extraData: ExtraData!) {
              extraData(value: $extraData)
            }
            "#
        ))
        .variables(Variables::from_value(variables_as_value))
    }

    let schema = schema();

    {
        let res = schema.execute(request(value!({ "extraData": {} }))).await;
        assert_eq!(value!({ "extraData": {} }), res.data, "{res:?}");
    }
    {
        let res = schema
            .execute(request(value!({ "extraData": {"foo": "bar"} })))
            .await;
        assert_eq!(value!({ "extraData": {"foo": "bar"} }), res.data, "{res:?}");
    }
    {
        let res = schema.execute(request(value!({ "extraData": 1 }))).await;
        assert_eq!(
            [
                "Failed to parse \"ExtraData\": Invalid input value: '1'. A flat object is \
                 expected, such as: '{}', '{\"foo\": \"bar\"}'"
            ],
            *res.error_messages(),
            "{res:?}"
        );
    }
    {
        let res = schema
            .execute(request(value!({ "extraData": "foo" })))
            .await;
        assert_eq!(
            [
                "Failed to parse \"ExtraData\": Invalid input value: '\"foo\"'. A flat object is \
                 expected, such as: '{}', '{\"foo\": \"bar\"}'"
            ],
            *res.error_messages(),
            "{res:?}"
        );
    }
    {
        let res = schema.execute(request(value!({ "extraData": [] }))).await;
        assert_eq!(
            [
                "Failed to parse \"ExtraData\": Invalid input value: '[]'. A flat object is \
                 expected, such as: '{}', '{\"foo\": \"bar\"}'"
            ],
            *res.error_messages(),
            "{res:?}"
        );
    }
    {
        let res = schema
            .execute(request(value!({ "extraData": {"foo": {"bar": "baz"}} })))
            .await;
        assert_eq!(
            [
                "Failed to parse \"ExtraData\": Invalid input value: '{foo: {bar: \"baz\"}}'. A \
                 flat object is expected, such as: '{}', '{\"foo\": \"bar\"}'"
            ],
            *res.error_messages(),
            "{res:?}"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_big_int() {
    fn request(variables_as_value: Value) -> Request {
        Request::new(indoc::indoc!(
            r#"
            query ($bigInt: BigInt!) {
              bigInt(value: $bigInt)
            }
            "#
        ))
        .variables(Variables::from_value(variables_as_value))
    }

    let schema = schema();

    {
        let res = schema
            .execute(request(
                value!({
                    "bigInt": "108494037067113761580099112583860151730516105403483528465874625006707409835912"
                }),
            ))
            .await;
        assert_eq!(
            value!({
                "bigInt": "108494037067113761580099112583860151730516105403483528465874625006707409835912"
            }),
            res.data,
            "{res:?}"
        );
    }
    {
        let res = schema
            .execute(request(value!({
                "bigInt": ""
            })))
            .await;
        assert_eq!(
            [
                "Failed to parse \"BigInt\": Invalid BigInt: cannot parse integer from empty \
                 string"
            ],
            *res.error_messages(),
            "{res:?}"
        );
    }
    {
        let res = schema
            .execute(request(value!({
                "bigInt": 9
            })))
            .await;
        assert_eq!(
            [
                "Failed to parse \"BigInt\": Invalid BigInt: the value is expected to be a string \
                 (\"9\") instead of a number (9)"
            ],
            *res.error_messages(),
            "{res:?}"
        );
    }
    {
        let res = schema
            .execute(request(value!({
                "bigInt": ""
            })))
            .await;
        assert_eq!(
            [
                "Failed to parse \"BigInt\": Invalid BigInt: cannot parse integer from empty \
                 string"
            ],
            *res.error_messages(),
            "{res:?}"
        );
    }
    {
        let res = schema
            .execute(request(value!({
                "bigInt": "0xFFFF"
            })))
            .await;
        assert_eq!(
            ["Failed to parse \"BigInt\": Invalid BigInt: invalid digit found in string"],
            *res.error_messages(),
            "{res:?}"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_u256() {
    fn request(variables_as_value: Value) -> Request {
        Request::new(indoc::indoc!(
            r#"
            query ($u256: U256!) {
              u256(value: $u256)
            }
            "#
        ))
        .variables(Variables::from_value(variables_as_value))
    }

    let schema = schema();

    {
        let res = schema
            .execute(request(
                value!({
                    "u256": "108494037067113761580099112583860151730516105403483528465874625006707409835912"
                }),
            ))
            .await;
        assert_eq!(
            value!({
                "u256": "108494037067113761580099112583860151730516105403483528465874625006707409835912"
            }),
            res.data,
            "{res:?}"
        );
    }
    {
        let res = schema
            .execute(request(value!({
                "u256": "0"
            })))
            .await;
        assert_eq!(
            value!({
                "u256": "0"
            }),
            res.data,
            "{res:?}"
        );
    }
    {
        let res = schema
            .execute(request(
                value!({
                    // MAX
                    "u256": "115792089237316195423570985008687907853269984665640564039457584007913129639935"
                }),
            ))
            .await;
        assert_eq!(
            value!({
                "u256": "115792089237316195423570985008687907853269984665640564039457584007913129639935"
            }),
            res.data,
            "{res:?}"
        );
    }
    {
        let res = schema
            .execute(request(
                value!({
                    // MAX + 1
                    "u256": "115792089237316195423570985008687907853269984665640564039457584007913129639936"
                }),
            ))
            .await;
        assert_eq!(
            [
                "Failed to parse \"U256\": Invalid U256: value exceeds maximum for U256: \
                 115792089237316195423570985008687907853269984665640564039457584007913129639936"
            ],
            *res.error_messages(),
            "{res:?}"
        );
    }
    {
        let res = schema
            .execute(request(value!({
                "u256": "-1"
            })))
            .await;
        assert_eq!(
            [
                "Failed to parse \"U256\": Invalid U256: negative values are not allowed for \
                 U256: -1"
            ],
            *res.error_messages(),
            "{res:?}"
        );
    }
    {
        let res = schema
            .execute(request(value!({
                "u256": -1
            })))
            .await;
        assert_eq!(
            [
                "Failed to parse \"U256\": Invalid U256: the value is expected to be a string \
                 (\"-1\") instead of a number (-1)"
            ],
            *res.error_messages(),
            "{res:?}"
        );
    }
    {
        let res = schema
            .execute(request(value!({
                "u256": 42
            })))
            .await;
        assert_eq!(
            [
                "Failed to parse \"U256\": Invalid U256: the value is expected to be a string \
                 (\"42\") instead of a number (42)"
            ],
            *res.error_messages(),
            "{res:?}"
        );
    }
    {
        let res = schema
            .execute(request(value!({
                "u256": ""
            })))
            .await;
        assert_eq!(
            ["Failed to parse \"U256\": Invalid U256: cannot parse integer from empty string"],
            *res.error_messages(),
            "{res:?}"
        );
    }
    {
        let res = schema
            .execute(request(value!({
                "u256": "0xFFFF"
            })))
            .await;
        assert_eq!(
            ["Failed to parse \"U256\": Invalid U256: invalid digit found in string"],
            *res.error_messages(),
            "{res:?}"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct TestScalarQuery;

#[Object]
impl TestScalarQuery {
    async fn extra_data(&self, value: ExtraData) -> ExtraData {
        value
    }

    async fn big_int(&self, value: BigInt) -> BigInt {
        value
    }

    async fn u256(&self, value: U256) -> U256 {
        value
    }
}

type TestScalarSchema = Schema<TestScalarQuery, EmptyMutation, EmptySubscription>;

fn schema() -> TestScalarSchema {
    TestScalarSchema::build(TestScalarQuery, EmptyMutation, EmptySubscription).finish()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
