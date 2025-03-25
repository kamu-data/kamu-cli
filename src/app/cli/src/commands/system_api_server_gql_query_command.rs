// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::Catalog;

use super::{CLIError, Command};

pub struct APIServerGqlQueryCommand {
    base_catalog: Catalog,
    query: String,
    full: bool,
}

impl APIServerGqlQueryCommand {
    pub fn new<S: Into<String>>(base_catalog: Catalog, query: S, full: bool) -> Self {
        Self {
            base_catalog,
            query: query.into(),
            full,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for APIServerGqlQueryCommand {
    async fn run(&self) -> Result<(), CLIError> {
        // TODO: Access token? Not every GraphQL can run unauthorized
        let gql_schema = kamu_adapter_graphql::schema();
        let response = gql_schema
            .execute(async_graphql::Request::new(&self.query).data(self.base_catalog.clone()))
            .await;

        let data = if self.full {
            serde_json::to_string_pretty(&response).unwrap()
        } else if response.is_ok() {
            serde_json::to_string_pretty(&response.data).unwrap()
        } else {
            for err in &response.errors {
                eprintln!("{err}");
            }
            // TODO: Error should be propagated as bad exit code
            String::new()
        };

        println!("{data}");

        if response.is_ok() {
            Ok(())
        } else {
            Err(CLIError::PartialFailure)
        }
    }
}
