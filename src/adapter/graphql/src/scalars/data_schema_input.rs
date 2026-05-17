// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[nutype::nutype(derive(AsRef, Clone, Debug, Into, PartialEq, Eq))]
pub struct DataSchemaInput(odf::schema::DataSchema);

#[async_graphql::Scalar]
impl async_graphql::ScalarType for DataSchemaInput {
    fn parse(gql_value: async_graphql::Value) -> async_graphql::InputValueResult<Self> {
        let Ok(json_value) = gql_value.clone().into_json() else {
            return Err(invalid_input_error(&gql_value));
        };

        let Ok(schema) = serde_json::from_value::<odf::serde::yaml::DataSchema>(json_value) else {
            return Err(invalid_input_error(&gql_value));
        };

        Ok(Self::new(schema.into()))
    }

    fn to_value(&self) -> async_graphql::Value {
        // TODO: PERF: Avoid cloning
        let value = serde_json::to_value(odf::serde::yaml::DataSchema::from(self.as_ref().clone()))
            .unwrap();

        async_graphql::Value::from_json(value).unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn invalid_input_error(
    input_value: &async_graphql::Value,
) -> async_graphql::InputValueError<DataSchemaInput> {
    let message = format!("Not a valid ODF schema: '{input_value}'");
    async_graphql::InputValueError::custom(message)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
