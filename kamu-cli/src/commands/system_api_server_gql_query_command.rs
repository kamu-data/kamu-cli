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
    catalog: Catalog,
    query: String,
    full: bool,
}

impl APIServerGqlQueryCommand {
    pub fn new<S: Into<String>>(catalog: Catalog, query: S, full: bool) -> Self {
        Self {
            catalog,
            query: query.into(),
            full,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for APIServerGqlQueryCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        // TODO: Cloning catalog is too expensive currently
        let gql_schema = kamu_adapter_graphql::schema(self.catalog.clone());
        let response = gql_schema.execute(&self.query).await;

        let data = if self.full {
            serde_json::to_string_pretty(&response).unwrap()
        } else {
            if response.is_ok() {
                serde_json::to_string_pretty(&response.data).unwrap()
            } else {
                for err in &response.errors {
                    eprintln!("{}", err)
                }
                // TODO: Error should be propagated as bad exit code
                "".to_owned()
            }
        };

        println!("{}", data);

        if response.is_ok() {
            Ok(())
        } else {
            Err(CLIError::PartialFailure)
        }
    }
}
