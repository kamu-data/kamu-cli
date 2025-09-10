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
use indoc::indoc;
use odf_metadata::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_parse_ddl_to_odf_schema() {
    let schema = opendatafabric_data_utils::schema::parse::parse_ddl_to_odf_schema(indoc!(
        r#"
        b bool not null,
        b_opt bool,
        i32 int not null,
        u32 int unsigned not null,
        i64 bigint not null,
        u64 bigint unsigned not null,
        f32 float not null,
        f64 double not null,
        d date not null,
        ts timestamp not null,
        s1 string not null,
        s2 varchar not null
        "#
    ))
    .unwrap();

    opendatafabric_data_utils::testing::assert_odf_schema_eq(
        &schema,
        &DataSchema::new(vec![
            DataField::bool("b"),
            DataField::bool("b_opt").optional(),
            DataField::i32("i32"),
            DataField::u32("u32"),
            DataField::i64("i64"),
            DataField::u64("u64"),
            DataField::f32("f32"),
            DataField::f64("f64"),
            DataField::date("d"),
            DataField::timestamp_millis_utc("ts"),
            DataField::string("s1"),
            DataField::string("s2"),
        ]),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_parse_ddl_datafusion() {
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
        indoc!(
            r#"
            message arrow_schema {
              REQUIRED INT64 ts (TIMESTAMP(NANOS,false));
            }
            "#
        ),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_parse_ddl_datafusion_with_force_utc() {
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
        indoc!(
            r#"
            message arrow_schema {
              REQUIRED INT64 ts (TIMESTAMP(NANOS,true));
            }
            "#
        ),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_parse_ddl_datafusion_with_reserved_keyword() {
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
