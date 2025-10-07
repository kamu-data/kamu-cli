// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MutationRequest {
    mutation_code: String,
    variables: async_graphql::Variables,
    expect_success: bool,
}

impl MutationRequest {
    pub fn new(mutation_code: &str, variables: async_graphql::Variables) -> Self {
        Self {
            mutation_code: mutation_code.to_string(),
            variables,
            expect_success: true,
        }
    }

    pub fn expect_error(mut self) -> Self {
        self.expect_success = false;
        self
    }

    pub async fn execute(
        self,
        schema: &kamu_adapter_graphql::Schema,
        catalog: &dill::Catalog,
    ) -> async_graphql::Response {
        let response = schema
            .execute(
                async_graphql::Request::new(self.mutation_code)
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
