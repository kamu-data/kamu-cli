// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::Catalog;
use kamu_adapter_graphql::data_loader::{account_entity_data_loader, dataset_handle_data_loader};

use super::{CLIError, Command};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Command)]
pub struct APIServerGqlQueryCommand {
    #[dill::component(explicit)]
    cli_catalog: Catalog,

    #[dill::component(explicit)]
    query: String,

    #[dill::component(explicit)]
    full: bool,
}

#[async_trait::async_trait(?Send)]
impl Command for APIServerGqlQueryCommand {
    async fn run(&self) -> Result<(), CLIError> {
        let gql_schema = kamu_adapter_graphql::schema();
        // NOTE: Authorization based on the current subject from the CLI catalog
        let request = async_graphql::Request::new(&self.query)
            // Resolvers reach for these via `data_unchecked` and panic when they
            // are absent, so they must be registered here exactly as the HTTP
            // handler does it -- see explore/graphql_handler.rs.
            .data(account_entity_data_loader(&self.cli_catalog))
            .data(dataset_handle_data_loader(&self.cli_catalog))
            .data(self.cli_catalog.clone());

        let response = gql_schema.execute(request).await;

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
