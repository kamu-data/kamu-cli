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

#[nutype::nutype(derive(AsRef, Clone, Debug, Into))]
pub struct ExtraData(serde_json::Value);

impl Default for ExtraData {
    fn default() -> Self {
        Self::new(serde_json::Value::Object(Default::default()))
    }
}

#[async_graphql::Scalar]
impl async_graphql::ScalarType for ExtraData {
    fn parse(value: async_graphql::Value) -> async_graphql::InputValueResult<Self> {
        let async_graphql::Value::Object(gql_extra_data) = &value else {
            return Err(invalid_input_error(&value));
        };

        let extra_data = serde_json::to_value(gql_extra_data)?;

        if !EXTRA_DATA_JSONSCHEMA_VALIDATOR.is_valid(&extra_data) {
            return Err(invalid_input_error(&value));
        }

        Ok(Self::new(extra_data))
    }

    fn to_value(&self) -> async_graphql::Value {
        match async_graphql::Value::from_json(self.as_ref().clone()) {
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
