// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct GraphQLQueryRequest {
    request_code: String,
    variables: async_graphql::Variables,
    expect_success: bool,
}

impl GraphQLQueryRequest {
    pub(crate) fn new(request_code: &str, variables: async_graphql::Variables) -> Self {
        Self {
            request_code: request_code.to_string(),
            variables,
            expect_success: true,
        }
    }

    pub(crate) fn expect_error(mut self) -> Self {
        self.expect_success = false;
        self
    }

    pub(crate) async fn execute(
        self,
        schema: &kamu_adapter_graphql::Schema,
        catalog: &dill::Catalog,
    ) -> async_graphql::Response {
        let response = schema
            .execute(
                async_graphql::Request::new(self.request_code)
                    .variables(self.variables)
                    .data(catalog.clone()),
            )
            .await;

        if self.expect_success {
            assert!(response.is_ok(), "{:?}", response.errors);
        } else {
            assert!(response.is_err(), "{response:#?}");
        }

        response
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn get_gql_value_property<'a>(
    value: &'a async_graphql::Value,
    key: &str,
) -> Option<&'a async_graphql::Value> {
    if let async_graphql::Value::Object(obj) = value {
        obj.get(key)
    } else {
        None
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn get_gql_value_string_property(
    value: &async_graphql::Value,
    key: &str,
) -> Option<String> {
    get_gql_value_property(value, key)
        .and_then(|v| match v {
            async_graphql::Value::String(s) => Some(s.as_str()),
            async_graphql::Value::Enum(e) => Some(e.as_str()),
            _ => None,
        })
        .map(ToString::to_string)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn get_gql_value_i64_property(value: &async_graphql::Value, key: &str) -> Option<i64> {
    get_gql_value_property(value, key).and_then(|v| {
        if let async_graphql::Value::Number(n) = v {
            n.as_i64()
        } else {
            None
        }
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
