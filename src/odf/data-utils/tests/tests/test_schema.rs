// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use datafusion::error::DataFusionError;
use datafusion::sql::sqlparser::parser::ParserError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_parse_ddl() {
    let ctx = datafusion::prelude::SessionContext::new();
    let schema = opendatafabric_data_utils::schema::parse::parse_ddl_to_datafusion_schema(
        &ctx,
        "ts timestamp not null",
        false,
    )
    .await
    .unwrap();
    opendatafabric_data_utils::testing::assert_schema_eq(
        &schema,
        r#"
message arrow_schema {
  REQUIRED INT64 ts (TIMESTAMP(NANOS,false));
}
        "#,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_parse_ddl_with_force_utc() {
    let ctx = datafusion::prelude::SessionContext::new();
    let schema = opendatafabric_data_utils::schema::parse::parse_ddl_to_datafusion_schema(
        &ctx,
        "ts timestamp not null",
        true,
    )
    .await
    .unwrap();
    opendatafabric_data_utils::testing::assert_schema_eq(
        &schema,
        r#"
message arrow_schema {
  REQUIRED INT64 ts (TIMESTAMP(NANOS,true));
}
        "#,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_parse_ddl_with_reserved_keyword() {
    let ctx = datafusion::prelude::SessionContext::new();
    let result = opendatafabric_data_utils::schema::parse::parse_ddl_to_datafusion_schema(
        &ctx,
        "foo TEXT, key TEXT, bar TEXT",
        false,
    )
    .await;

    let Err(DataFusionError::SQL(err, _)) = result else {
        panic!("Expected SQL error, got: {result:?}");
    };

    assert_matches!(*err, ParserError::ParserError(msg) if msg ==  *"Argument 'key TEXT' is invalid or a reserved keyword");
}
