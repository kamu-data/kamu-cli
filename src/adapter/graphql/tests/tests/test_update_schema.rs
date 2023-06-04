// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;

#[test_log::test(tokio::test)]
async fn update_graphql_schema() {
    let mut schema_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    schema_path.push("../../../resources/schema.gql");

    let cat = dill::CatalogBuilder::new().build();

    let schema = kamu_adapter_graphql::schema(cat);
    std::fs::write(&schema_path, schema.sdl()).unwrap();
}
