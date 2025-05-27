// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::*;
use kamu_adapter_graphql::scalars::{BigInt, ExtraData};
use kamu_adapter_graphql::traits::ResponseExt;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn extra_data() {
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
        pretty_assertions::assert_eq!(value!({ "extraData": {} }), res.data, "{res:?}");
    }
    {
        let res = schema
            .execute(request(value!({ "extraData": {"foo": "bar"} })))
            .await;
        pretty_assertions::assert_eq!(value!({ "extraData": {"foo": "bar"} }), res.data, "{res:?}");
    }
    {
        let res = schema.execute(request(value!({ "extraData": 1 }))).await;
        pretty_assertions::assert_eq!(
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
        pretty_assertions::assert_eq!(
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
        pretty_assertions::assert_eq!(
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
        pretty_assertions::assert_eq!(
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
async fn extra_big_int() {
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
        pretty_assertions::assert_eq!(
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
        pretty_assertions::assert_eq!(
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
        pretty_assertions::assert_eq!(
            ["Failed to parse \"BigInt\": Invalid BigInt: invalid digit found in string",],
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
}

type TestScalarSchema = Schema<TestScalarQuery, EmptyMutation, EmptySubscription>;

fn schema() -> TestScalarSchema {
    TestScalarSchema::build(TestScalarQuery, EmptyMutation, EmptySubscription).finish()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
