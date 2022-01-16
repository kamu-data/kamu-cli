// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::Catalog;
use std::path::PathBuf;

#[test_log::test(tokio::test)]
async fn test_update_graphql_schema() {
    let mut schema_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    schema_path.push("../resources/schema.gql");

    let cmd = kamu_cli::commands::APIServerGqlSchemaCommand::new(Catalog::new());
    let schema = cmd.get_schema();

    std::fs::write(&schema_path, schema).unwrap();
}
