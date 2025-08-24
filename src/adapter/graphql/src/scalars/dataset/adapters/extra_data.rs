// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::LazyLock;

use kamu_datasets::ExtraDataFields;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

static EXTRA_DATA_JSONSCHEMA_VALIDATOR: LazyLock<jsonschema::Validator> = LazyLock::new(|| {
    let schema = serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "additionalProperties": {
            "type": [
                "string",
                "number",
                "boolean"
            ]
        },
        "minProperties": 0
    });
    jsonschema::validator_for(&schema).unwrap()
});

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[nutype::nutype(derive(AsRef, Clone, Debug, Into, PartialEq, Eq))]
pub struct ExtraData(serde_json::Map<String, serde_json::Value>);

impl Default for ExtraData {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

#[async_graphql::Scalar]
impl async_graphql::ScalarType for ExtraData {
    fn parse(gql_value: async_graphql::Value) -> async_graphql::InputValueResult<Self> {
        let async_graphql::Value::Object(gql_map) = &gql_value else {
            return Err(invalid_input_error(&gql_value));
        };

        let json_value = serde_json::to_value(gql_map)?;

        if !EXTRA_DATA_JSONSCHEMA_VALIDATOR.is_valid(&json_value) {
            return Err(invalid_input_error(&gql_value));
        }

        let serde_json::Value::Object(json_map) = json_value else {
            return Err(invalid_input_error(&gql_value));
        };

        Ok(Self::new(json_map))
    }

    fn to_value(&self) -> async_graphql::Value {
        match async_graphql::Value::from_json(serde_json::Value::Object(self.as_ref().clone())) {
            Ok(v) => v,
            Err(e) => unreachable!("{e}"),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn invalid_input_error(
    input_value: &async_graphql::Value,
) -> async_graphql::InputValueError<ExtraData> {
    let message = format!(
        "Invalid input value: '{input_value}'. A flat object is expected, such as: '{{}}', \
         '{{\"foo\": \"bar\"}}'"
    );

    async_graphql::InputValueError::custom(message)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<ExtraData> for ExtraDataFields {
    fn from(value: ExtraData) -> Self {
        Self::new(value.into_inner())
    }
}
