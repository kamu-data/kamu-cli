// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{CLIError, Command};

pub struct APIServerGqlSchemaCommand {}

impl APIServerGqlSchemaCommand {
    pub fn get_schema(&self) -> String {
        let gql_schema = kamu_adapter_graphql::schema();
        gql_schema.sdl()
    }
}

#[async_trait::async_trait(?Send)]
impl Command for APIServerGqlSchemaCommand {
    fn needs_workspace(&self) -> bool {
        false
    }

    async fn run(&mut self) -> Result<(), CLIError> {
        println!("{}", self.get_schema());
        Ok(())
    }
}
